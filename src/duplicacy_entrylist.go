// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"encoding/hex"
	"encoding/binary"
	"fmt"
	"os"
	"io"
	"path"
	"crypto/sha256"
	"crypto/rand"	
	"sync"

    "github.com/vmihailenco/msgpack"
)

// This struct stores information about a file entry that has been modified
type ModifiedEntry struct {
	Path string
	Size int64
	Hash string
}

// EntryList is basically a list of entries, which can be kept in the memory, or serialized to a disk file,
// depending on if maximumInMemoryEntries is reached.
//
// The idea behind the on-disk entry list is that entries are written to a disk file as they are coming in.
// Entries that have been modified and thus need to be uploaded will have their Incomplete bit set (i.e., 
// with a size of -1).  When the limit is reached, entries are moved to a disk file but ModifiedEntries and
// UploadedChunks are still kept in memory.  When later entries are read from the entry list, incomplete 
// entries are back-annotated with info from ModifiedEntries and UploadedChunk* before sending them out.

type EntryList struct {
	onDiskFile *os.File                     // the file to store entries
	encoder *msgpack.Encoder                // msgpack encoder for entry serialization
	entries []*Entry                        // in-memory entry list
	
	SnapshotID string                       // the snapshot id
	Token string                            // this unique random token makes sure we read/write 
	                                        // the same entry list
	ModifiedEntries []ModifiedEntry         // entries that will be uploaded

	UploadedChunkHashes []string            // chunks from entries that have been uploaded
	UploadedChunkLengths []int              // chunk lengths from entries that have been uploaded
	uploadedChunkLock sync.Mutex            // lock for UploadedChunkHashes and UploadedChunkLengths

	PreservedChunkHashes []string           // chunks from entries not changed
	PreservedChunkLengths []int             // chunk lengths from entries not changed

	Checksum string                         // checksum of all entries to detect disk corruption

	maximumInMemoryEntries int              // max in-memory entries
	NumberOfEntries int64                   // number of entries (not including directories and links)
	cachePath string                        // the directory for the on-disk file

	// These 3 variables are used in entry infomation back-annotation
	modifiedEntryIndex int                  // points to the current modified entry
	uploadedChunkIndex int                  // counter for upload chunks
	uploadedChunkOffset int                 // the start offset for the current modified entry

}

// Create a new entry list
func CreateEntryList(snapshotID string, cachePath string, maximumInMemoryEntries int) (*EntryList, error) {

	token := make([]byte, 16)
	_, err := rand.Read(token)
	if err != nil {
		return nil, fmt.Errorf("Failed to create a random token: %v", err)
	}

	entryList := &EntryList {
		SnapshotID: snapshotID,
		maximumInMemoryEntries: maximumInMemoryEntries,
		cachePath: cachePath,
		Token: string(token),
	}

	return entryList, nil

}

// Create the on-disk entry list file
func (entryList *EntryList)createOnDiskFile() error {
	file, err := os.OpenFile(path.Join(entryList.cachePath, "incomplete_files"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("Failed to create on disk entry list: %v", err)
	}

	entryList.onDiskFile = file
	entryList.encoder = msgpack.NewEncoder(file)

	err = entryList.encoder.EncodeString(entryList.Token)
	if err != nil {
		return fmt.Errorf("Failed to create on disk entry list: %v", err)
	}

	for _, entry := range entryList.entries {
		err = entry.EncodeWithHash(entryList.encoder)
		if err != nil {
			return err
		}
	}
	return nil
}

// Add an entry to the entry list
func (entryList *EntryList)AddEntry(entry *Entry) error {

	if !entry.IsDir() && !entry.IsLink() {
		entryList.NumberOfEntries++
	}

	if !entry.IsComplete() {
		if entry.IsDir() || entry.IsLink() {
			entry.Size = 0
		} else {
			modifiedEntry := ModifiedEntry {
				Path: entry.Path,
				Size: -1,
			}
		
			entryList.ModifiedEntries = append(entryList.ModifiedEntries, modifiedEntry)
		}
	}

	if entryList.onDiskFile != nil {
		return entry.EncodeWithHash(entryList.encoder)
	} else {
		entryList.entries = append(entryList.entries, entry)
		if entryList.maximumInMemoryEntries >= 0 && len(entryList.entries) > entryList.maximumInMemoryEntries {
			err := entryList.createOnDiskFile()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Add a preserved chunk that belongs to files that have not been modified
func (entryList *EntryList)AddPreservedChunk(chunkHash string, chunkSize int) {
	entryList.PreservedChunkHashes = append(entryList.PreservedChunkHashes, chunkHash)
	entryList.PreservedChunkLengths = append(entryList.PreservedChunkLengths, chunkSize)
}

// Add a chunk just uploaded (that belongs to files that have been modified)
func (entryList *EntryList)AddUploadedChunk(chunkIndex int, chunkHash string, chunkSize int) {
	entryList.uploadedChunkLock.Lock()

	for len(entryList.UploadedChunkHashes) <= chunkIndex {
		entryList.UploadedChunkHashes = append(entryList.UploadedChunkHashes, "")
	}

	for len(entryList.UploadedChunkLengths) <= chunkIndex {
		entryList.UploadedChunkLengths = append(entryList.UploadedChunkLengths, 0)
	}

	entryList.UploadedChunkHashes[chunkIndex] = chunkHash
	entryList.UploadedChunkLengths[chunkIndex] = chunkSize
	entryList.uploadedChunkLock.Unlock()
}

// Close the on-disk file
func (entryList *EntryList) CloseOnDiskFile() error {

	if entryList.onDiskFile == nil {
		return nil
	}

	err := entryList.onDiskFile.Sync()
	if err != nil {
		return err
	}

	err = entryList.onDiskFile.Close()
	if err != nil {
		return err
	}

	entryList.onDiskFile = nil
	return nil
}

// Return the length of the `index`th chunk
func (entryList *EntryList) getChunkLength(index int) int {
	if index < len(entryList.PreservedChunkLengths) {
		return entryList.PreservedChunkLengths[index]
	} else {
		return entryList.UploadedChunkLengths[index - len(entryList.PreservedChunkLengths)]
	}
}

// Sanity check for each entry
func (entryList *EntryList) checkEntry(entry *Entry) error {

	if entry.Size < 0 {
		return fmt.Errorf("the file %s hash an invalid size (%d)", entry.Path, entry.Size)
	}

	if !entry.IsFile() || entry.Size == 0 {
		return nil
	}

	numberOfChunks := len(entryList.PreservedChunkLengths) + len(entryList.UploadedChunkLengths) 

	if entry.StartChunk < 0 {
		return fmt.Errorf("the file %s starts at chunk %d", entry.Path, entry.StartChunk)
	}

	if entry.EndChunk >= numberOfChunks {
		return fmt.Errorf("the file %s ends at chunk %d while the number of chunks is %d",
			entry.Path, entry.EndChunk, numberOfChunks)
	}

	if entry.EndChunk < entry.StartChunk {
		return fmt.Errorf("the file %s starts at chunk %d and ends at chunk %d",
			entry.Path, entry.StartChunk, entry.EndChunk)
	}

	if entry.StartOffset >= entryList.getChunkLength(entry.StartChunk) {
		return fmt.Errorf("the file %s starts at offset %d of chunk %d with a length of %d",
			entry.Path, entry.StartOffset, entry.StartChunk, entryList.getChunkLength(entry.StartChunk))
	}

	if entry.EndOffset > entryList.getChunkLength(entry.EndChunk) {
		return fmt.Errorf("the file %s ends at offset %d of chunk %d with a length of %d",
			entry.Path, entry.EndOffset, entry.EndChunk, entryList.getChunkLength(entry.EndChunk))
	}

	fileSize := int64(0)

	for i := entry.StartChunk; i <= entry.EndChunk; i++ {

		start := 0
		if i == entry.StartChunk {
			start = entry.StartOffset
		}
		end := entryList.getChunkLength(i)
		if i == entry.EndChunk {
			end = entry.EndOffset
		}

		fileSize += int64(end - start)
	}

	if entry.Size != fileSize {
		return fmt.Errorf("the file %s has a size of %d but the total size of chunks is %d",
			entry.Path, entry.Size, fileSize)
	}

	return nil
}

// An incomplete entry (with a size of -1) does not have 'startChunk', 'startOffset', 'endChunk', and 'endOffset'.  This function
// is to fill in these information before sending the entry out.
func (entryList *EntryList) fillAndSendEntry(entry *Entry, entryOut func(*Entry)error) (skipped bool, err error) {

	if entry.IsComplete() {
		err := entryList.checkEntry(entry)
		if err != nil {
			return false, err
		}
		return false, entryOut(entry)
	}

	if entryList.modifiedEntryIndex >= len(entryList.ModifiedEntries) {
		return false, fmt.Errorf("Unexpected file index %d (%d modified files)", entryList.modifiedEntryIndex, len(entryList.ModifiedEntries))
	}

	modifiedEntry := &entryList.ModifiedEntries[entryList.modifiedEntryIndex]
	entryList.modifiedEntryIndex++

	if modifiedEntry.Path != entry.Path {
		return false, fmt.Errorf("Unexpected file path %s when expecting %s", modifiedEntry.Path, entry.Path)
	}

	if modifiedEntry.Size <= 0 {
		return true, nil
	}

	entry.Size = modifiedEntry.Size
	entry.Hash = modifiedEntry.Hash

	entry.StartChunk = entryList.uploadedChunkIndex + len(entryList.PreservedChunkHashes)
	entry.StartOffset = entryList.uploadedChunkOffset
	entry.EndChunk = entry.StartChunk
	endOffset := int64(entry.StartOffset) + entry.Size

	for entryList.uploadedChunkIndex < len(entryList.UploadedChunkLengths) && endOffset > int64(entryList.UploadedChunkLengths[entryList.uploadedChunkIndex]) {
		endOffset -= int64(entryList.UploadedChunkLengths[entryList.uploadedChunkIndex])
		entry.EndChunk++
		entryList.uploadedChunkIndex++
	}

	if entryList.uploadedChunkIndex >= len(entryList.UploadedChunkLengths) {
		return false, fmt.Errorf("File %s has not been completely uploaded", entry.Path)
	}

	entry.EndOffset = int(endOffset)
	entryList.uploadedChunkOffset = entry.EndOffset 
	if entry.EndOffset == entryList.UploadedChunkLengths[entryList.uploadedChunkIndex] {
		entryList.uploadedChunkIndex++
		entryList.uploadedChunkOffset = 0
	}

	err = entryList.checkEntry(entry)
	if err != nil {
		return false, err
	}

	return false, entryOut(entry)
}

// Iterate through the entries in this entry list
func (entryList *EntryList) ReadEntries(entryOut func(*Entry)error) (error) {

	entryList.modifiedEntryIndex = 0
	entryList.uploadedChunkIndex = 0
	entryList.uploadedChunkOffset = 0

	if entryList.onDiskFile == nil {
		for _, entry := range entryList.entries {
			skipped, err := entryList.fillAndSendEntry(entry.Copy(), entryOut)
			if err != nil {
				return err
			}
			if skipped {
				continue
			}
		}
	} else {
		_, err := entryList.onDiskFile.Seek(0, os.SEEK_SET)
		if err != nil {
			return err
		}
		decoder := msgpack.NewDecoder(entryList.onDiskFile)

		_, err = decoder.DecodeString()
		if err != nil {
			return err
		}
	
    	for _, err = decoder.PeekCode(); err == nil; _, err = decoder.PeekCode() {	
			entry, err := DecodeEntryWithHash(decoder)
			if err != nil {
				return err
			}
			skipped, err := entryList.fillAndSendEntry(entry, entryOut)
			if err != nil {
				return err
			}
			if skipped {
				continue
			}
		}

		if err != io.EOF {
			return err
		}
	}

	return nil
}

// When saving an incomplete snapshot, the on-disk entry list ('incomplete_files') is renamed to
// 'incomplete_snapshot', and this EntryList struct is saved as 'incomplete_chunks'.
func (entryList *EntryList) SaveIncompleteSnapshot() {
	entryList.uploadedChunkLock.Lock()
	defer entryList.uploadedChunkLock.Unlock()

	if entryList.onDiskFile == nil {
		err := entryList.createOnDiskFile()
		if err != nil {
			LOG_WARN("INCOMPLETE_SAVE", "Failed to create the incomplete snapshot file: %v", err)
			return
		}

		for _, entry := range entryList.entries {

			err = entry.EncodeWithHash(entryList.encoder)
			if err != nil {
				LOG_WARN("INCOMPLETE_SAVE", "Failed to save the entry %s: %v", entry.Path, err)
				return
			}
		}
	}

	err := entryList.onDiskFile.Close()
	if err != nil {
		LOG_WARN("INCOMPLETE_SAVE", "Failed to close the on-disk file: %v", err)
		return
	}

	filePath := path.Join(entryList.cachePath, "incomplete_snapshot")
	if _, err := os.Stat(filePath); err == nil {
		err = os.Remove(filePath)
		if err != nil {
			LOG_WARN("INCOMPLETE_REMOVE", "Failed to remove previous incomplete snapshot: %v", err)
		}
	}

	err = os.Rename(path.Join(entryList.cachePath, "incomplete_files"), filePath)
	if err != nil {
		LOG_WARN("INCOMPLETE_SAVE", "Failed to rename the incomplete snapshot file: %v", err)
		return
	}

	chunkFile := path.Join(entryList.cachePath, "incomplete_chunks")
	file, err := os.OpenFile(chunkFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		LOG_WARN("INCOMPLETE_SAVE", "Failed to create the incomplete chunk file: %v", err)
		return
	}

	defer file.Close()
	encoder := msgpack.NewEncoder(file)

	entryList.Checksum = entryList.CalculateChecksum()

	err = encoder.Encode(entryList)
	if err != nil {
		LOG_WARN("INCOMPLETE_SAVE", "Failed to save the incomplete snapshot: %v", err)
		return
	}

	LOG_INFO("INCOMPLETE_SAVE", "Incomplete snapshot saved to %s", filePath)	
}

// Calculate a checksum for this entry list
func (entryList *EntryList) CalculateChecksum() string{

	hasher := sha256.New()
	for _, s := range entryList.UploadedChunkHashes {
		hasher.Write([]byte(s))
	}

	buffer := make([]byte, 8)
	for _, i := range entryList.UploadedChunkLengths {
		binary.LittleEndian.PutUint64(buffer, uint64(i))
		hasher.Write(buffer)
	}

	for _, s := range entryList.PreservedChunkHashes {
		hasher.Write([]byte(s))
	}

	for _, i := range entryList.PreservedChunkLengths {
		binary.LittleEndian.PutUint64(buffer, uint64(i))
		hasher.Write(buffer)
	}

	for _, entry := range entryList.ModifiedEntries {
		binary.LittleEndian.PutUint64(buffer, uint64(entry.Size))
		hasher.Write(buffer)
		hasher.Write([]byte(entry.Hash))
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

// Check if all chunks exist in 'chunkCache'
func (entryList *EntryList) CheckChunks(config *Config, chunkCache map[string]bool) bool {
	for _, chunkHash := range entryList.UploadedChunkHashes {
		chunkID := config.GetChunkIDFromHash(chunkHash)
		if _, ok := chunkCache[chunkID]; !ok {
			return false
		}
	}
	for _, chunkHash := range entryList.PreservedChunkHashes {
		chunkID := config.GetChunkIDFromHash(chunkHash)
		if _, ok := chunkCache[chunkID]; !ok {
			return false
		}
	}

	return true

}

// Recover the on disk file from 'incomplete_snapshot', and restore the EntryList struct
// from 'incomplete_chunks'
func loadIncompleteSnapshot(snapshotID string, cachePath string) *EntryList {

	onDiskFilePath := path.Join(cachePath, "incomplete_snapshot")
	entryListFilePath := path.Join(cachePath, "incomplete_chunks")

	if _, err := os.Stat(onDiskFilePath); os.IsNotExist(err) {
		return nil
	}

	if _, err := os.Stat(entryListFilePath); os.IsNotExist(err) {
		return nil
	}

	entryList := &EntryList {}
	entryListFile, err := os.OpenFile(entryListFilePath, os.O_RDONLY, 0600)
	if err != nil {
		LOG_WARN("INCOMPLETE_LOAD", "Failed to open the incomplete snapshot: %v", err)
		return nil
	}

	defer entryListFile.Close()
	decoder := msgpack.NewDecoder(entryListFile)
	err = decoder.Decode(&entryList)
	if err != nil {
		LOG_WARN("INCOMPLETE_LOAD", "Failed to load the incomplete snapshot: %v", err)
		return nil
	}

	checksum := entryList.CalculateChecksum()
	if checksum != entryList.Checksum {
		LOG_WARN("INCOMPLETE_LOAD", "Failed to load the incomplete snapshot: checksum mismatched")
		return nil
	}

	onDiskFile, err := os.OpenFile(onDiskFilePath, os.O_RDONLY, 0600)
	if err != nil {
		LOG_WARN("INCOMPLETE_LOAD", "Failed to open the on disk file for the incomplete snapshot: %v", err)
		return nil
	}

	decoder = msgpack.NewDecoder(onDiskFile)
	token, err := decoder.DecodeString()
	if err != nil {
		LOG_WARN("INCOMPLETE_LOAD", "Failed to read the token for the incomplete snapshot: %v", err)
		onDiskFile.Close()
		return nil
	}

	if token != entryList.Token {
		LOG_WARN("INCOMPLETE_LOAD", "Mismatched tokens in the incomplete snapshot")
		onDiskFile.Close()
		return nil
	}

	entryList.onDiskFile = onDiskFile

	for i, hash := range entryList.UploadedChunkHashes {
		if len(hash) == 0 {
			// An empty hash means the chunk has not been uploaded in previous run
			entryList.UploadedChunkHashes = entryList.UploadedChunkHashes[0:i]
			entryList.UploadedChunkLengths = entryList.UploadedChunkLengths[0:i]
			break
		}
	}

	LOG_INFO("INCOMPLETE_LOAD", "Previous incomlete backup contains %d files and %d chunks",
	         entryList.NumberOfEntries, len(entryList.PreservedChunkLengths) + len(entryList.UploadedChunkHashes))

	return entryList
}

// Delete the two incomplete files.
func deleteIncompleteSnapshot(cachePath string) {

	for _, file := range []string{"incomplete_snapshot", "incomplete_chunks"} {
		filePath := path.Join(cachePath, file)
		if _, err := os.Stat(filePath); err == nil {
			err = os.Remove(filePath)
			if err != nil {
				LOG_WARN("INCOMPLETE_REMOVE", "Failed to remove the incomplete snapshot: %v", err)
				return
			}
		}
	}


}