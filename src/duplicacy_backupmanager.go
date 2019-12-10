// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BackupManager performs the two major operations, backup and restore, and passes other operations, mostly related to
// snapshot management, to the snapshot manager.

type BackupManager struct {
	snapshotID string  // Unique id for each repository
	storage    Storage // the storage for storing backups

	SnapshotManager *SnapshotManager // the snapshot manager
	snapshotCache   *FileStorage     // for copies of chunks needed by snapshots

	config *Config // contains a number of options

	nobackupFile string // don't backup directory when this file name is found
	filtersFile string  // the path to the filters file
}

func (manager *BackupManager) SetDryRun(dryRun bool) {
	manager.config.dryRun = dryRun
}

// CreateBackupManager creates a backup manager using the specified 'storage'.  'snapshotID' is a unique id to
// identify snapshots created for this repository.  'top' is the top directory of the repository.  'password' is the
// master key which can be nil if encryption is not enabled.
func CreateBackupManager(snapshotID string, storage Storage, top string, password string, nobackupFile string, filtersFile string) *BackupManager {

	config, _, err := DownloadConfig(storage, password)
	if err != nil {
		LOG_ERROR("STORAGE_CONFIG", "Failed to download the configuration file from the storage: %v", err)
		return nil
	}
	if config == nil {
		LOG_ERROR("STORAGE_NOT_CONFIGURED", "The storage has not been initialized")
		return nil
	}

	snapshotManager := CreateSnapshotManager(config, storage)

	backupManager := &BackupManager{
		snapshotID: snapshotID,
		storage:    storage,

		SnapshotManager: snapshotManager,

		config: config,

		nobackupFile: nobackupFile,
		filtersFile: filtersFile,
	}

	if IsDebugging() {
		config.Print()
	}

	return backupManager
}

// loadRSAPrivateKey loads the specifed private key file for decrypting file chunks
func (manager *BackupManager) LoadRSAPrivateKey(keyFile string, passphrase string) {
	manager.config.loadRSAPrivateKey(keyFile, passphrase)
}

// SetupSnapshotCache creates the snapshot cache, which is merely a local storage under the default .duplicacy
// directory
func (manager *BackupManager) SetupSnapshotCache(storageName string) bool {

	preferencePath := GetDuplicacyPreferencePath()
	cacheDir := path.Join(preferencePath, "cache", storageName)

	storage, err := CreateFileStorage(cacheDir, false, 1)
	if err != nil {
		LOG_ERROR("BACKUP_CACHE", "Failed to create the snapshot cache dir: %v", err)
		return false
	}

	for _, subdir := range []string{"chunks", "snapshots"} {
		err := os.Mkdir(path.Join(cacheDir, subdir), 0744)
		if err != nil && !os.IsExist(err) {
			LOG_ERROR("BACKUP_CACHE", "Failed to create the snapshot cache subdir: %v", err)
			return false
		}
	}

	storage.SetDefaultNestingLevels([]int{1}, 1)
	manager.snapshotCache = storage
	manager.SnapshotManager.snapshotCache = storage
	return true
}


// setEntryContent sets the 4 content pointers for each entry in 'entries'.  'offset' indicates the value
// to be added to the StartChunk and EndChunk points, used when intending to append 'entries' to the
// original unchanged entry list.
//
// This function assumes the Size field of each entry is equal to the length of the chunk content that belong
// to the file.
func setEntryContent(entries []*Entry, chunkLengths []int, offset int) {
	if len(entries) == 0 {
		return
	}

	// The following code works by iterating over 'entries' and 'chunkLength' and keeping track of the
	// accumulated total file size and the accumulated total chunk size.
	i := 0
	totalChunkSize := int64(0)
	totalFileSize := entries[i].Size
	entries[i].StartChunk = 0 + offset
	entries[i].StartOffset = 0
	for j, length := range chunkLengths {

		for totalChunkSize+int64(length) >= totalFileSize {
			entries[i].EndChunk = j + offset
			entries[i].EndOffset = int(totalFileSize - totalChunkSize)

			i++
			if i >= len(entries) {
				break
			}

			// If the current file ends at the end of the current chunk, the next file will
			// start at the next chunk
			if totalChunkSize+int64(length) == totalFileSize {
				entries[i].StartChunk = j + 1 + offset
				entries[i].StartOffset = 0
			} else {
				entries[i].StartChunk = j + offset
				entries[i].StartOffset = int(totalFileSize - totalChunkSize)
			}

			totalFileSize += entries[i].Size
		}

		if i >= len(entries) {
			break
		}
		totalChunkSize += int64(length)
	}

	// If there are some unvisited entries (which happens when saving an incomplete snapshot),
	// set their sizes to -1 so they won't be saved to the incomplete snapshot
	for j := i; j < len(entries); j++ {
		entries[j].Size = -1
	}
}

// Backup creates a snapshot for the repository 'top'.  If 'quickMode' is true, only files with different sizes
// or timestamps since last backup will be uploaded (however the snapshot is still a full snapshot that shares
// unmodified files with last backup).  Otherwise (or if this is the first backup), the entire repository will
// be scanned to create the snapshot.  'tag' is the tag assigned to the new snapshot.
func (manager *BackupManager) Backup(top string, quickMode bool, threads int, tag string,
	showStatistics bool, shadowCopy bool, shadowCopyTimeout int, enumOnly bool) bool {

	var err error
	top, err = filepath.Abs(top)
	if err != nil {
		LOG_ERROR("REPOSITORY_ERR", "Failed to obtain the absolute path of the repository: %v", err)
		return false
	}

	startTime := time.Now().Unix()

	LOG_DEBUG("BACKUP_PARAMETERS", "top: %s, quick: %t, tag: %s", top, quickMode, tag)

	if manager.config.rsaPublicKey != nil && len(manager.config.FileKey) > 0 {
		LOG_INFO("BACKUP_KEY", "RSA encryption is enabled" )
	}

	remoteSnapshot := manager.SnapshotManager.downloadLatestSnapshot(manager.snapshotID)
	if remoteSnapshot == nil {
		remoteSnapshot = CreateEmptySnapshot(manager.snapshotID)
		LOG_INFO("BACKUP_START", "No previous backup found")
	} else {
		LOG_INFO("BACKUP_START", "Last backup at revision %d found", remoteSnapshot.Revision)
	}

	shadowTop := CreateShadowCopy(top, shadowCopy, shadowCopyTimeout)
	defer DeleteShadowCopy()

	LOG_INFO("BACKUP_INDEXING", "Indexing %s", top)
	localSnapshot, skippedDirectories, skippedFiles, err := CreateSnapshotFromDirectory(manager.snapshotID, shadowTop,
		                                                                                manager.nobackupFile, manager.filtersFile)
	if err != nil {
		LOG_ERROR("SNAPSHOT_LIST", "Failed to list the directory %s: %v", top, err)
		return false
	}

	if enumOnly {
		return true
	}

	// This cache contains all chunks referenced by last snasphot. Any other chunks will lead to a call to
	// UploadChunk.
	chunkCache := make(map[string]bool)

	var incompleteSnapshot *Snapshot

	// A revision number of 0 means this is the initial backup
	if remoteSnapshot.Revision > 0 {
		// Add all chunks in the last snapshot to the cache
		for _, chunkID := range manager.SnapshotManager.GetSnapshotChunks(remoteSnapshot, true) {
			chunkCache[chunkID] = true
		}
	} else {

		// In quick mode, attempt to load the incomplete snapshot from last incomplete backup if there is one.
		if quickMode {
			incompleteSnapshot = LoadIncompleteSnapshot()
		}

		// If the listing operation is fast or there is an incomplete snapshot, list all chunks and
		// put them in the cache.
		if manager.storage.IsFastListing() || incompleteSnapshot != nil {
			LOG_INFO("BACKUP_LIST", "Listing all chunks")
			allChunks, _ := manager.SnapshotManager.ListAllFiles(manager.storage, "chunks/")

			for _, chunk := range allChunks {
				if len(chunk) == 0 || chunk[len(chunk)-1] == '/' {
					continue
				}

				if strings.HasSuffix(chunk, ".fsl") {
					continue
				}

				chunk = strings.Replace(chunk, "/", "", -1)
				chunkCache[chunk] = true
			}
		}

		if incompleteSnapshot != nil {

			// This is the last chunk from the incomplete snapshot that can be found in the cache
			lastCompleteChunk := -1
			for i, chunkHash := range incompleteSnapshot.ChunkHashes {
				chunkID := manager.config.GetChunkIDFromHash(chunkHash)
				if _, ok := chunkCache[chunkID]; ok {
					lastCompleteChunk = i
				} else {
					break
				}
			}

			LOG_DEBUG("CHUNK_INCOMPLETE", "The incomplete snapshot contains %d files and %d chunks", len(incompleteSnapshot.Files), len(incompleteSnapshot.ChunkHashes))
			LOG_DEBUG("CHUNK_INCOMPLETE", "Last chunk in the incomplete snapshot that exist in the storage: %d", lastCompleteChunk)

			// Only keep those files whose chunks exist in the cache
			var files []*Entry
			for _, file := range incompleteSnapshot.Files {
				if file.StartChunk <= lastCompleteChunk && file.EndChunk <= lastCompleteChunk {
					files = append(files, file)
				} else {
					break
				}
			}
			incompleteSnapshot.Files = files

			// Remove incomplete chunks (they may not have been uploaded)
			incompleteSnapshot.ChunkHashes = incompleteSnapshot.ChunkHashes[:lastCompleteChunk+1]
			incompleteSnapshot.ChunkLengths = incompleteSnapshot.ChunkLengths[:lastCompleteChunk+1]
			remoteSnapshot = incompleteSnapshot
			LOG_INFO("FILE_SKIP", "Skipped %d files from previous incomplete backup", len(files))
		}

	}

	var numberOfNewFileChunks int64        // number of new file chunks
	var totalUploadedFileChunkLength int64 // total length of uploaded file chunks
	var totalUploadedFileChunkBytes int64  // how many actual bytes have been uploaded

	var totalUploadedSnapshotChunkLength int64 // size of uploaded snapshot chunks
	var totalUploadedSnapshotChunkBytes int64  // how many actual bytes have been uploaded

	localSnapshot.Revision = remoteSnapshot.Revision + 1

	var totalModifiedFileSize int64    // total size of modified files
	var uploadedModifiedFileSize int64 // portions that have been uploaded (including cache hits)

	var modifiedEntries []*Entry  // Files that has been modified or newly created
	var preservedEntries []*Entry // Files unchanges

	// If the quick mode is disable and there isn't an incomplete snapshot from last (failed) backup,
	// we simply treat all files as if they were new, and break them into chunks.
	// Otherwise, we need to find those that are new or recently modified

	if (remoteSnapshot.Revision == 0 || !quickMode) && incompleteSnapshot == nil {
		modifiedEntries = localSnapshot.Files
		for _, entry := range modifiedEntries {
			totalModifiedFileSize += entry.Size
		}
	} else {

		var i, j int
		for i < len(localSnapshot.Files) {

			local := localSnapshot.Files[i]

			if !local.IsFile() || local.Size == 0 {
				i++
				continue
			}

			var remote *Entry
			if j >= len(remoteSnapshot.Files) {
				totalModifiedFileSize += local.Size
				modifiedEntries = append(modifiedEntries, local)
				i++
			} else if remote = remoteSnapshot.Files[j]; !remote.IsFile() {
				j++
			} else if local.Path == remote.Path {
				if local.IsSameAs(remote) {
					local.Hash = remote.Hash
					local.StartChunk = remote.StartChunk
					local.StartOffset = remote.StartOffset
					local.EndChunk = remote.EndChunk
					local.EndOffset = remote.EndOffset
					preservedEntries = append(preservedEntries, local)
				} else {
					totalModifiedFileSize += local.Size
					modifiedEntries = append(modifiedEntries, local)
				}
				i++
				j++
			} else if local.Compare(remote) < 0 {
				totalModifiedFileSize += local.Size
				modifiedEntries = append(modifiedEntries, local)
				i++
			} else {
				j++
			}
		}

		// Must sort files by their 'StartChunk', so the chunk indices form a monotonically increasing sequence
		sort.Sort(ByChunk(preservedEntries))
	}

	var preservedChunkHashes []string
	var preservedChunkLengths []int

	// For each preserved file, adjust the StartChunk and EndChunk pointers.  This is done by finding gaps
	// between these indices and subtracting the number of deleted chunks.
	last := -1
	deletedChunks := 0
	for _, entry := range preservedEntries {

		if entry.StartChunk > last {
			deletedChunks += entry.StartChunk - last - 1
		}

		for i := entry.StartChunk; i <= entry.EndChunk; i++ {
			if i == last {
				continue
			}
			preservedChunkHashes = append(preservedChunkHashes, remoteSnapshot.ChunkHashes[i])
			preservedChunkLengths = append(preservedChunkLengths, remoteSnapshot.ChunkLengths[i])
		}

		last = entry.EndChunk

		entry.StartChunk -= deletedChunks
		entry.EndChunk -= deletedChunks
	}

	var uploadedEntries []*Entry
	var uploadedChunkHashes []string
	var uploadedChunkLengths []int
	var uploadedChunkLock = &sync.Mutex{}

	// Set all file sizes to -1 to indicate they haven't been processed.   This must be done before creating the file
	// reader because the file reader may skip inaccessible files on construction.
	for _, entry := range modifiedEntries {
		entry.Size = -1
	}

	// the file reader implements the Reader interface. When an EOF is encounter, it opens the next file unless it
	// is the last file.
	fileReader := CreateFileReader(shadowTop, modifiedEntries)

	startUploadingTime := time.Now().Unix()

	lastUploadingTime := time.Now().Unix()

	keepUploadAlive := int64(1800)

	if os.Getenv("DUPLICACY_UPLOAD_KEEPALIVE") != "" {
		value, _ := strconv.Atoi(os.Getenv("DUPLICACY_UPLOAD_KEEPALIVE"))
		if value < 10 {
			value = 10
		}
		LOG_INFO("UPLOAD_KEEPALIVE", "Setting KeepUploadAlive to %d", value)
		keepUploadAlive = int64(value)
	}

	// Fail at the chunk specified by DUPLICACY_FAIL_CHUNK to simulate a backup error
	chunkToFail := -1
	if value, found := os.LookupEnv("DUPLICACY_FAIL_CHUNK"); found {
		chunkToFail, _ = strconv.Atoi(value)
		LOG_INFO("SNAPSHOT_FAIL", "Will abort the backup on chunk %d", chunkToFail)
	}

	chunkMaker := CreateChunkMaker(manager.config, false)
	chunkUploader := CreateChunkUploader(manager.config, manager.storage, nil, threads, nil)

	localSnapshotReady := false
	var once sync.Once

	if remoteSnapshot.Revision == 0 {
		// In case an error occurs during the initial backup, save the incomplete snapshot
		RunAtError = func() {
			once.Do(
				func() {
					if !localSnapshotReady {
						// Lock it to gain exclusive access to uploadedChunkHashes and uploadedChunkLengths
						uploadedChunkLock.Lock()
						setEntryContent(uploadedEntries, uploadedChunkLengths, len(preservedChunkHashes))
						if len(preservedChunkHashes) > 0 {
							//localSnapshot.Files = preservedEntries
							//localSnapshot.Files = append(preservedEntries, uploadedEntries...)
							localSnapshot.ChunkHashes = preservedChunkHashes
							localSnapshot.ChunkHashes = append(localSnapshot.ChunkHashes, uploadedChunkHashes...)
							localSnapshot.ChunkLengths = preservedChunkLengths
							localSnapshot.ChunkLengths = append(localSnapshot.ChunkLengths, uploadedChunkLengths...)
						} else {
							//localSnapshot.Files = uploadedEntries
							localSnapshot.ChunkHashes = uploadedChunkHashes
							localSnapshot.ChunkLengths = uploadedChunkLengths
						}
						uploadedChunkLock.Unlock()
					}
					SaveIncompleteSnapshot(localSnapshot)
				})
		}
	}

	if fileReader.CurrentFile != nil {

		LOG_TRACE("PACK_START", "Packing %s", fileReader.CurrentEntry.Path)

		chunkIndex := 0
		if threads < 1 {
			threads = 1
		}
		if threads > 1 {
			LOG_INFO("BACKUP_THREADS", "Use %d uploading threads", threads)
		}

		var numberOfCollectedChunks int64

		completionFunc := func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
			action := "Skipped"
			if skipped {
				LOG_DEBUG("CHUNK_CACHE", "Skipped chunk %s in cache", chunk.GetID())
			} else {
				if uploadSize > 0 {
					atomic.AddInt64(&numberOfNewFileChunks, 1)
					atomic.AddInt64(&totalUploadedFileChunkLength, int64(chunkSize))
					atomic.AddInt64(&totalUploadedFileChunkBytes, int64(uploadSize))
					action = "Uploaded"
				} else {
					LOG_DEBUG("CHUNK_EXIST", "Skipped chunk %s in the storage", chunk.GetID())
				}
			}

			uploadedModifiedFileSize := atomic.AddInt64(&uploadedModifiedFileSize, int64(chunkSize))

			if (IsTracing() || showStatistics) && totalModifiedFileSize > 0 {
				now := time.Now().Unix()
				if now <= startUploadingTime {
					now = startUploadingTime + 1
				}
				speed := uploadedModifiedFileSize / (now - startUploadingTime)
				remainingTime := int64(0)
				if speed > 0 {
					remainingTime = (totalModifiedFileSize-uploadedModifiedFileSize)/speed + 1
				}
				percentage := float32(uploadedModifiedFileSize * 1000 / totalModifiedFileSize)
				LOG_INFO("UPLOAD_PROGRESS", "%s chunk %d size %d, %sB/s %s %.1f%%", action, chunkIndex,
					chunkSize, PrettySize(speed), PrettyTime(remainingTime), percentage/10)
			}

			atomic.AddInt64(&numberOfCollectedChunks, 1)
			manager.config.PutChunk(chunk)
		}
		chunkUploader.completionFunc = completionFunc
		chunkUploader.Start()

		// Break files into chunks
		chunkMaker.ForEachChunk(
			fileReader.CurrentFile,
			func(chunk *Chunk, final bool) {

				hash := chunk.GetHash()
				chunkID := chunk.GetID()
				chunkSize := chunk.GetLength()

				chunkIndex++

				_, found := chunkCache[chunkID]
				if found {
					if time.Now().Unix()-lastUploadingTime > keepUploadAlive {
						LOG_INFO("UPLOAD_KEEPALIVE", "Skip chunk cache to keep connection alive")
						found = false
					}
				}

				if found {
					completionFunc(chunk, chunkIndex, true, chunkSize, 0)
				} else {
					lastUploadingTime = time.Now().Unix()
					chunkCache[chunkID] = true

					chunkUploader.StartChunk(chunk, chunkIndex)
				}

				// Must lock it because the RunAtError function called by other threads may access these two slices
				uploadedChunkLock.Lock()
				uploadedChunkHashes = append(uploadedChunkHashes, hash)
				uploadedChunkLengths = append(uploadedChunkLengths, chunkSize)
				uploadedChunkLock.Unlock()

				if len(uploadedChunkHashes) == chunkToFail {
					LOG_ERROR("SNAPSHOT_FAIL", "Artificially fail the chunk %d for testing purposes", chunkToFail)
				}

			},
			func(fileSize int64, hash string) (io.Reader, bool) {

				// Must lock here because the RunAtError function called by other threads may access uploadedEntries
				uploadedChunkLock.Lock()
				defer uploadedChunkLock.Unlock()

				// This function is called when a new file is needed
				entry := fileReader.CurrentEntry
				entry.Hash = hash
				entry.Size = fileSize
				uploadedEntries = append(uploadedEntries, entry)

				if !showStatistics || IsTracing() || RunInBackground {
					LOG_INFO("PACK_END", "Packed %s (%d)", entry.Path, entry.Size)
				}

				fileReader.NextFile()

				if fileReader.CurrentFile != nil {
					LOG_TRACE("PACK_START", "Packing %s", fileReader.CurrentEntry.Path)
					return fileReader.CurrentFile, true
				}
				return nil, false
			})

		chunkUploader.Stop()

		// We can't set the offsets in the ForEachChunk loop because in that loop, when switching to a new file, the
		// data in the buffer may not have been pushed into chunks; it may happen that new chunks can be created
		// aftwards, before reaching the end of the current file.
		//
		// Therefore, we saved uploaded entries and then do a loop here to set offsets for them.
		setEntryContent(uploadedEntries, uploadedChunkLengths, len(preservedChunkHashes))
	}

	if len(preservedChunkHashes) > 0 {
		localSnapshot.ChunkHashes = preservedChunkHashes
		localSnapshot.ChunkHashes = append(localSnapshot.ChunkHashes, uploadedChunkHashes...)
		localSnapshot.ChunkLengths = preservedChunkLengths
		localSnapshot.ChunkLengths = append(localSnapshot.ChunkLengths, uploadedChunkLengths...)
	} else {
		localSnapshot.ChunkHashes = uploadedChunkHashes
		localSnapshot.ChunkLengths = uploadedChunkLengths
	}

	localSnapshotReady = true

	localSnapshot.EndTime = time.Now().Unix()

	err = manager.SnapshotManager.CheckSnapshot(localSnapshot)
	if err != nil {
		RunAtError = func() {} // Don't save the incomplete snapshot
		LOG_ERROR("SNAPSHOT_CHECK", "The snapshot contains an error: %v", err)
		return false
	}

	localSnapshot.Tag = tag
	localSnapshot.Options = ""
	if !quickMode || remoteSnapshot.Revision == 0 {
		localSnapshot.Options = "-hash"
	}

	if _, found := os.LookupEnv("DUPLICACY_FAIL_SNAPSHOT"); found {
		LOG_ERROR("SNAPSHOT_FAIL", "Artificially fail the backup for testing purposes")
		return false
	}

	if shadowCopy {
		if localSnapshot.Options == "" {
			localSnapshot.Options = "-vss"
		} else {
			localSnapshot.Options += " -vss"
		}
	}

	var preservedFileSize int64
	var uploadedFileSize int64
	var totalFileChunkLength int64
	for _, file := range preservedEntries {
		preservedFileSize += file.Size
	}
	for _, file := range uploadedEntries {
		uploadedFileSize += file.Size
	}
	for _, length := range localSnapshot.ChunkLengths {
		totalFileChunkLength += int64(length)
	}

	localSnapshot.FileSize = preservedFileSize + uploadedFileSize
	localSnapshot.NumberOfFiles = int64(len(preservedEntries) + len(uploadedEntries))

	totalSnapshotChunkLength, numberOfNewSnapshotChunks,
		totalUploadedSnapshotChunkLength, totalUploadedSnapshotChunkBytes :=
		manager.UploadSnapshot(chunkMaker, chunkUploader, top, localSnapshot, chunkCache)

	if showStatistics && !RunInBackground {
		for _, entry := range uploadedEntries {
			LOG_INFO("UPLOAD_FILE", "Uploaded %s (%d)", entry.Path, entry.Size)
		}
	}

	for _, dir := range skippedDirectories {
		LOG_WARN("SKIP_DIRECTORY", "Subdirectory %s cannot be listed", dir)
	}

	for _, file := range fileReader.SkippedFiles {
		LOG_WARN("SKIP_FILE", "File %s cannot be opened", file)
	}
	skippedFiles = append(skippedFiles, fileReader.SkippedFiles...)

	if !manager.config.dryRun {
		manager.SnapshotManager.CleanSnapshotCache(localSnapshot, nil)
	}
	LOG_INFO("BACKUP_END", "Backup for %s at revision %d completed", top, localSnapshot.Revision)

	RunAtError = func() {}
	RemoveIncompleteSnapshot()

	totalSnapshotChunks := len(localSnapshot.FileSequence) + len(localSnapshot.ChunkSequence) +
		len(localSnapshot.LengthSequence)
	if showStatistics {

		LOG_INFO("BACKUP_STATS", "Files: %d total, %s bytes; %d new, %s bytes",
			len(preservedEntries)+len(uploadedEntries),
			PrettyNumber(preservedFileSize+uploadedFileSize),
			len(uploadedEntries), PrettyNumber(uploadedFileSize))

		LOG_INFO("BACKUP_STATS", "File chunks: %d total, %s bytes; %d new, %s bytes, %s bytes uploaded",
			len(localSnapshot.ChunkHashes), PrettyNumber(totalFileChunkLength),
			numberOfNewFileChunks, PrettyNumber(totalUploadedFileChunkLength),
			PrettyNumber(totalUploadedFileChunkBytes))

		LOG_INFO("BACKUP_STATS", "Metadata chunks: %d total, %s bytes; %d new, %s bytes, %s bytes uploaded",
			totalSnapshotChunks, PrettyNumber(totalSnapshotChunkLength),
			numberOfNewSnapshotChunks, PrettyNumber(totalUploadedSnapshotChunkLength),
			PrettyNumber(totalUploadedSnapshotChunkBytes))

		LOG_INFO("BACKUP_STATS", "All chunks: %d total, %s bytes; %d new, %s bytes, %s bytes uploaded",
			len(localSnapshot.ChunkHashes)+totalSnapshotChunks,
			PrettyNumber(totalFileChunkLength+totalSnapshotChunkLength),
			int(numberOfNewFileChunks)+numberOfNewSnapshotChunks,
			PrettyNumber(totalUploadedFileChunkLength+totalUploadedSnapshotChunkLength),
			PrettyNumber(totalUploadedFileChunkBytes+totalUploadedSnapshotChunkBytes))

		now := time.Now().Unix()
		if now == startTime {
			now = startTime + 1
		}
		LOG_INFO("BACKUP_STATS", "Total running time: %s", PrettyTime(now-startTime))
	}

	skipped := ""
	if len(skippedDirectories) > 0 {
		if len(skippedDirectories) == 1 {
			skipped = "1 directory"
		} else {
			skipped = fmt.Sprintf("%d directories", len(skippedDirectories))
		}
	}

	if len(skippedFiles) > 0 {
		if len(skipped) > 0 {
			skipped += " and "
		}
		if len(skippedFiles) == 1 {
			skipped += "1 file"
		} else {
			skipped += fmt.Sprintf("%d files", len(skippedFiles))
		}
	}

	if len(skipped) > 0 {
		if len(skippedDirectories)+len(skippedFiles) == 1 {
			skipped += " was"
		} else {
			skipped += " were"
		}

		skipped += " not included due to access errors"
		LOG_WARN("BACKUP_SKIPPED", skipped)
	}

	return true
}

// Restore downloads the specified snapshot, compares it with what's on the repository, and then downloads
// files that are different. 'base' is a directory that contains files at a different revision which can
// serve as a local cache to avoid download chunks available locally.  It is perfectly ok for 'base' to be
// the same as 'top'.  'quickMode' will bypass files with unchanged sizes and timestamps.  'deleteMode' will
// remove local files that don't exist in the snapshot. 'patterns' is used to include/exclude certain files.
func (manager *BackupManager) Restore(top string, revision int, inPlace bool, quickMode bool, threads int, overwrite bool,
	deleteMode bool, setOwner bool, showStatistics bool, patterns []string) bool {

	startTime := time.Now().Unix()

	LOG_DEBUG("RESTORE_PARAMETERS", "top: %s, revision: %d, in-place: %t, quick: %t, delete: %t",
		top, revision, inPlace, quickMode, deleteMode)

	if !strings.HasPrefix(GetDuplicacyPreferencePath(), top) {
		LOG_INFO("RESTORE_INPLACE", "Forcing in-place mode with a non-default preference path")
		inPlace = true
	}

	if len(patterns) > 0 {
		for _, pattern := range patterns {
			LOG_TRACE("RESTORE_PATTERN", "%s", pattern)
		}
	}

	_, err := os.Stat(top)
	if os.IsNotExist(err) {
		err = os.Mkdir(top, 0744)
		if err != nil {
			LOG_ERROR("RESTORE_MKDIR", "Can't create the directory to be restored: %v", err)
			return false
		}
	}

	// How will behave restore when repo created using -repo-dir ,??
	err = os.Mkdir(path.Join(top, DUPLICACY_DIRECTORY), 0744)
	if err != nil && !os.IsExist(err) {
		LOG_ERROR("RESTORE_MKDIR", "Failed to create the preference directory: %v", err)
		return false
	}

	remoteSnapshot := manager.SnapshotManager.DownloadSnapshot(manager.snapshotID, revision)
	manager.SnapshotManager.DownloadSnapshotContents(remoteSnapshot, patterns, true)

	localSnapshot, _, _, err := CreateSnapshotFromDirectory(manager.snapshotID, top, manager.nobackupFile,
		                                                    manager.filtersFile)
	if err != nil {
		LOG_ERROR("SNAPSHOT_LIST", "Failed to list the repository: %v", err)
		return false
	}

	LOG_INFO("RESTORE_START", "Restoring %s to revision %d", top, revision)

	var includedFiles []*Entry

	// Include/exclude some files if needed
	if len(patterns) > 0 {
		for _, file := range remoteSnapshot.Files {

			if MatchPath(file.Path, patterns) {
				includedFiles = append(includedFiles, file)
			}
		}

		remoteSnapshot.Files = includedFiles
	}

	// local files that don't exist in the remote snapshot
	var extraFiles []string

	// These will store files to be downloaded.
	fileEntries := make([]*Entry, 0, len(remoteSnapshot.Files)/2)

	var totalFileSize int64
	var downloadedFileSize int64

	i := 0
	for _, entry := range remoteSnapshot.Files {

		skipped := false
		// Find local files that don't exist in the remote snapshot
		for i < len(localSnapshot.Files) {
			local := localSnapshot.Files[i]
			compare := entry.Compare(local)
			if compare > 0 {
				extraFiles = append(extraFiles, local.Path)
				i++
				continue
			} else {
				if compare == 0 {
					i++
					if quickMode && local.IsSameAs(entry) {
						LOG_TRACE("RESTORE_SKIP", "File %s unchanged (by size and timestamp)", local.Path)
						skipped = true
					}
				}
				break
			}
		}

		if skipped {
			continue
		}

		fullPath := joinPath(top, entry.Path)
		if entry.IsLink() {
			stat, err := os.Lstat(fullPath)
			if stat != nil {
				if stat.Mode()&os.ModeSymlink != 0 {
					isRegular, link, err := Readlink(fullPath)
					if err == nil && link == entry.Link && !isRegular {
						entry.RestoreMetadata(fullPath, nil, setOwner)
						continue
					}
				}

				os.Remove(fullPath)
			}

			err = os.Symlink(entry.Link, fullPath)
			if err != nil {
				LOG_ERROR("RESTORE_SYMLINK", "Can't create symlink %s: %v", entry.Path, err)
				return false
			}
			entry.RestoreMetadata(fullPath, nil, setOwner)
			LOG_TRACE("DOWNLOAD_DONE", "Symlink %s updated", entry.Path)
		} else if entry.IsDir() {
			stat, err := os.Stat(fullPath)

			if err == nil && !stat.IsDir() {
				LOG_ERROR("RESTORE_NOTDIR", "The path %s is not a directory", fullPath)
				return false
			}

			if os.IsNotExist(err) {
				// In the first pass of directories, set the directory to be user readable so we can create new files
				// under it.
				err = os.MkdirAll(fullPath, 0700)
				if err != nil && !os.IsExist(err) {
					LOG_ERROR("RESTORE_MKDIR", "%v", err)
					return false
				}
			}
		} else {
			// We can't download files here since fileEntries needs to be sorted
			fileEntries = append(fileEntries, entry)
			totalFileSize += entry.Size
		}
	}

	for i < len(localSnapshot.Files) {
		extraFiles = append(extraFiles, localSnapshot.Files[i].Path)
		i++
	}

	// Sort entries by their starting chunks in order to linearize the access to the chunk chain.
	sort.Sort(ByChunk(fileEntries))

	chunkDownloader := CreateChunkDownloader(manager.config, manager.storage, nil, showStatistics, threads)
	chunkDownloader.AddFiles(remoteSnapshot, fileEntries)

	chunkMaker := CreateChunkMaker(manager.config, true)

	startDownloadingTime := time.Now().Unix()

	var downloadedFiles []*Entry
	// Now download files one by one
	for _, file := range fileEntries {

		fullPath := joinPath(top, file.Path)
		stat, _ := os.Stat(fullPath)
		if stat != nil {
			if quickMode {
				if file.IsSameAsFileInfo(stat) {
					LOG_TRACE("RESTORE_SKIP", "File %s unchanged (by size and timestamp)", file.Path)
					continue
				}
			}

			if file.Size == 0 && file.IsSameAsFileInfo(stat) {
				LOG_TRACE("RESTORE_SKIP", "File %s unchanged (size 0)", file.Path)
				continue
			}
		} else {
			parent, _ := SplitDir(fullPath)
			err = os.MkdirAll(parent, 0744)
			if err != nil {
				LOG_ERROR("DOWNLOAD_MKDIR", "Failed to create directory: %v", err)
			}
		}

		// Handle zero size files.
		if file.Size == 0 {
			newFile, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.GetPermissions())
			if err != nil {
				LOG_ERROR("DOWNLOAD_OPEN", "Failed to create empty file: %v", err)
				return false
			}
			newFile.Close()

			file.RestoreMetadata(fullPath, nil, setOwner)
			if !showStatistics {
				LOG_INFO("DOWNLOAD_DONE", "Downloaded %s (0)", file.Path)
			}

			continue
		}

		if manager.RestoreFile(chunkDownloader, chunkMaker, file, top, inPlace, overwrite, showStatistics,
			totalFileSize, downloadedFileSize, startDownloadingTime) {
			downloadedFileSize += file.Size
			downloadedFiles = append(downloadedFiles, file)
		}
		file.RestoreMetadata(fullPath, nil, setOwner)
	}

	if deleteMode && len(patterns) == 0 {
		// Reverse the order to make sure directories are empty before being deleted
		for i := range extraFiles {
			file := extraFiles[len(extraFiles)-1-i]
			fullPath := joinPath(top, file)
			os.Remove(fullPath)
			LOG_INFO("RESTORE_DELETE", "Deleted %s", file)
		}
	}

	for _, entry := range remoteSnapshot.Files {
		if entry.IsDir() && !entry.IsLink() {
			dir := joinPath(top, entry.Path)
			entry.RestoreMetadata(dir, nil, setOwner)
		}
	}

	if showStatistics {
		for _, file := range downloadedFiles {
			LOG_INFO("DOWNLOAD_DONE", "Downloaded %s (%d)", file.Path, file.Size)
		}
	}

	LOG_INFO("RESTORE_END", "Restored %s to revision %d", top, revision)
	if showStatistics {
		LOG_INFO("RESTORE_STATS", "Files: %d total, %s bytes", len(fileEntries), PrettySize(totalFileSize))
		LOG_INFO("RESTORE_STATS", "Downloaded %d file, %s bytes, %d chunks",
			len(downloadedFiles), PrettySize(downloadedFileSize), chunkDownloader.numberOfDownloadedChunks)
	}

	runningTime := time.Now().Unix() - startTime
	if runningTime == 0 {
		runningTime = 1
	}

	LOG_INFO("RESTORE_STATS", "Total running time: %s", PrettyTime(runningTime))

	chunkDownloader.Stop()

	return true
}

// fileEncoder encodes one file at a time to avoid loading the full json description of the entire file tree
// in the memory
type fileEncoder struct {
	top            string
	readAttributes bool
	files          []*Entry
	currentIndex   int
	buffer         *bytes.Buffer
}

// Read reads data from the embedded buffer
func (encoder fileEncoder) Read(data []byte) (n int, err error) {
	return encoder.buffer.Read(data)
}

// NextFile switches to the next file and generates its json description in the buffer.  It also takes care of
// the ending ']' and the commas between files.
func (encoder *fileEncoder) NextFile() (io.Reader, bool) {
	if encoder.currentIndex == len(encoder.files) {
		return nil, false
	}
	if encoder.currentIndex == len(encoder.files)-1 {
		encoder.buffer.Write([]byte("]"))
		encoder.currentIndex++
		return encoder, true
	}

	encoder.currentIndex++
	entry := encoder.files[encoder.currentIndex]
	if encoder.readAttributes {
		entry.ReadAttributes(encoder.top)
	}
	description, err := json.Marshal(entry)
	if err != nil {
		LOG_FATAL("SNAPSHOT_ENCODE", "Failed to encode file %s: %v", encoder.files[encoder.currentIndex].Path, err)
		return nil, false
	}

	if encoder.readAttributes {
		entry.Attributes = nil
	}

	if encoder.currentIndex != 0 {
		encoder.buffer.Write([]byte(","))
	}
	encoder.buffer.Write(description)
	return encoder, true
}

// UploadSnapshot uploads the specified snapshot to the storage. It turns Files, ChunkHashes, and ChunkLengths into
// sequences of chunks, and uploads these chunks, and finally the snapshot file.
func (manager *BackupManager) UploadSnapshot(chunkMaker *ChunkMaker, uploader *ChunkUploader, top string, snapshot *Snapshot,
	chunkCache map[string]bool) (totalSnapshotChunkSize int64,
	numberOfNewSnapshotChunks int, totalUploadedSnapshotChunkSize int64,
	totalUploadedSnapshotChunkBytes int64) {

	uploader.snapshotCache = manager.snapshotCache

	completionFunc := func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
		if skipped {
			LOG_DEBUG("CHUNK_CACHE", "Skipped snapshot chunk %s in cache", chunk.GetID())
		} else {
			if uploadSize > 0 {
				numberOfNewSnapshotChunks++
				totalUploadedSnapshotChunkSize += int64(chunkSize)
				totalUploadedSnapshotChunkBytes += int64(uploadSize)
			} else {
				LOG_DEBUG("CHUNK_EXIST", "Skipped snapshot chunk %s in the storage", chunk.GetID())
			}
		}

		manager.config.PutChunk(chunk)
	}

	uploader.completionFunc = completionFunc
	uploader.Start()

	// uploadSequenceFunc uploads chunks read from 'reader'.
	uploadSequenceFunc := func(reader io.Reader,
		nextReader func(size int64, hash string) (io.Reader, bool)) (sequence []string) {

		chunkMaker.ForEachChunk(reader,
			func(chunk *Chunk, final bool) {
				totalSnapshotChunkSize += int64(chunk.GetLength())
				chunkID := chunk.GetID()
				if _, found := chunkCache[chunkID]; found {
					completionFunc(chunk, 0, true, chunk.GetLength(), 0)
				} else {
					uploader.StartChunk(chunk, len(sequence))
				}
				sequence = append(sequence, chunk.GetHash())
			},
			nextReader)

		return sequence
	}

	sequences := []string{"chunks", "lengths"}
	// The file list is assumed not to be too large when fixed-size chunking is used
	if chunkMaker.minimumChunkSize == chunkMaker.maximumChunkSize {
		sequences = append(sequences, "files")
	}

	// Chunk and length sequences can be encoded and loaded into memory directly
	for _, sequenceType := range sequences {
		contents, err := snapshot.MarshalSequence(sequenceType)

		if err != nil {
			LOG_ERROR("SNAPSHOT_MARSHAL", "Failed to encode the %s in the snapshot %s: %v",
				sequenceType, manager.snapshotID, err)
			return int64(0), 0, int64(0), int64(0)
		}

		sequence := uploadSequenceFunc(bytes.NewReader(contents),
			func(fileSize int64, hash string) (io.Reader, bool) {
				return nil, false
			})
		snapshot.SetSequence(sequenceType, sequence)
	}

	// File sequence may be too big to fit into the memory.  So we encode files one by one and take advantages of
	// the multi-reader capability of the chunk maker.
	if chunkMaker.minimumChunkSize != chunkMaker.maximumChunkSize {
		encoder := fileEncoder{
			top:            top,
			readAttributes: snapshot.discardAttributes,
			files:          snapshot.Files,
			currentIndex:   -1,
			buffer:         new(bytes.Buffer),
		}

		encoder.buffer.Write([]byte("["))
		sequence := uploadSequenceFunc(encoder,
			func(fileSize int64, hash string) (io.Reader, bool) {
				return encoder.NextFile()
			})
		snapshot.SetSequence("files", sequence)
	}

	uploader.Stop()

	description, err := snapshot.MarshalJSON()
	if err != nil {
		LOG_ERROR("SNAPSHOT_MARSHAL", "Failed to encode the snapshot %s: %v", manager.snapshotID, err)
		return int64(0), 0, int64(0), int64(0)
	}

	path := fmt.Sprintf("snapshots/%s/%d", manager.snapshotID, snapshot.Revision)
	if !manager.config.dryRun {
		manager.SnapshotManager.UploadFile(path, path, description)
	}
	return totalSnapshotChunkSize, numberOfNewSnapshotChunks, totalUploadedSnapshotChunkSize, totalUploadedSnapshotChunkBytes
}

// Restore downloads a file from the storage.  If 'inPlace' is false, the download file is saved first to a temporary
// file under the .duplicacy directory and then replaces the existing one.  Otherwise, the existing file will be
// overwritten directly.
func (manager *BackupManager) RestoreFile(chunkDownloader *ChunkDownloader, chunkMaker *ChunkMaker, entry *Entry, top string, inPlace bool, overwrite bool,
	showStatistics bool, totalFileSize int64, downloadedFileSize int64, startTime int64) bool {

	LOG_TRACE("DOWNLOAD_START", "Downloading %s", entry.Path)

	var existingFile, newFile *os.File
	var err error

	preferencePath := GetDuplicacyPreferencePath()
	temporaryPath := path.Join(preferencePath, "temporary")
	fullPath := joinPath(top, entry.Path)

	defer func() {
		if existingFile != nil {
			existingFile.Close()
		}
		if newFile != nil {
			newFile.Close()
		}

		if temporaryPath != fullPath {
			os.Remove(temporaryPath)
		}
	}()

	// These are used to break the existing file into chunks.
	var existingChunks []string
	var existingLengths []int

	// These are to enable fast lookup of what chunks are available in the existing file.
	offsetMap := make(map[string]int64)
	lengthMap := make(map[string]int)
	var offset int64

	// If the file is newly created (needed by sparse file optimization)
	isNewFile := false

	existingFile, err = os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			// macOS has no sparse file support
			if inPlace && entry.Size > 100*1024*1024 && runtime.GOOS != "darwin" {
				// Create an empty sparse file
				existingFile, err = os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
				if err != nil {
					LOG_ERROR("DOWNLOAD_CREATE", "Failed to create the file %s for in-place writing: %v", fullPath, err)
					return false
				}

				n := int64(1)
				// There is a go bug on Windows (https://github.com/golang/go/issues/21681) that causes Seek to fail
				// if the lower 32 bit of the offset argument is 0xffffffff.  Therefore we need to avoid that value by increasing n.
				if uint32(entry.Size) == 0 && (entry.Size>>32) > 0 {
					n = int64(2)
				}
				_, err = existingFile.Seek(entry.Size-n, 0)
				if err != nil {
					LOG_ERROR("DOWNLOAD_CREATE", "Failed to resize the initial file %s for in-place writing: %v", fullPath, err)
					return false
				}
				_, err = existingFile.Write([]byte("\x00\x00")[:n])
				if err != nil {
					LOG_ERROR("DOWNLOAD_CREATE", "Failed to initialize the sparse file %s for in-place writing: %v", fullPath, err)
					return false
				}
				existingFile.Close()
				existingFile, err = os.Open(fullPath)
				if err != nil {
					LOG_ERROR("DOWNLOAD_OPEN", "Can't reopen the initial file just created: %v", err)
					return false
				}
				isNewFile = true
			}
		} else {
			LOG_TRACE("DOWNLOAD_OPEN", "Can't open the existing file: %v", err)
		}
	} else {
		if !overwrite {
			LOG_ERROR("DOWNLOAD_OVERWRITE",
				"File %s already exists.  Please specify the -overwrite option to continue", entry.Path)
			return false
		}
	}

	// The key in this map is the number of zeroes.  The value is the corresponding hash.
	knownHashes := make(map[int]string)

	fileHash := ""
	if existingFile != nil {

		if inPlace {
			// In inplace mode, we only consider chunks in the existing file with the same offsets, so we
			// break the original file at offsets retrieved from the backup
			fileHasher := manager.config.NewFileHasher()
			buffer := make([]byte, 64*1024)
			err = nil
			isSkipped := false
			// We set to read one more byte so the file hash will be different if the file to be restored is a
			// truncated portion of the existing file
			for i := entry.StartChunk; i <= entry.EndChunk+1; i++ {
				hasher := manager.config.NewKeyedHasher(manager.config.HashKey)
				chunkSize := 0
				if i == entry.StartChunk {
					chunkSize = chunkDownloader.taskList[i].chunkLength - entry.StartOffset
				} else if i == entry.EndChunk {
					chunkSize = entry.EndOffset
				} else if i > entry.StartChunk && i < entry.EndChunk {
					chunkSize = chunkDownloader.taskList[i].chunkLength
				} else {
					chunkSize = 1 // the size of extra chunk beyond EndChunk
				}
				count := 0

				if isNewFile {
					if hash, found := knownHashes[chunkSize]; found {
						// We have read the same number of zeros before, so we just retrieve the hash from the map
						existingChunks = append(existingChunks, hash)
						existingLengths = append(existingLengths, chunkSize)
						offsetMap[hash] = offset
						lengthMap[hash] = chunkSize
						offset += int64(chunkSize)
						isSkipped = true
						continue
					}
				}

				if isSkipped {
					_, err := existingFile.Seek(offset, 0)
					if err != nil {
						LOG_ERROR("DOWNLOAD_SEEK", "Failed to seek to offset %d: %v", offset, err)
					}
					isSkipped = false
				}

				for count < chunkSize {
					n := chunkSize - count
					if n > cap(buffer) {
						n = cap(buffer)
					}
					n, err := existingFile.Read(buffer[:n])
					if n > 0 {
						hasher.Write(buffer[:n])
						fileHasher.Write(buffer[:n])
						count += n
					}
					if err == io.EOF {
						break
					}
					if err != nil {
						LOG_ERROR("DOWNLOAD_SPLIT", "Failed to read existing file: %v", err)
						return false
					}
				}
				if count > 0 {
					hash := string(hasher.Sum(nil))
					existingChunks = append(existingChunks, hash)
					existingLengths = append(existingLengths, chunkSize)
					offsetMap[hash] = offset
					lengthMap[hash] = chunkSize
					offset += int64(chunkSize)
					if isNewFile {
						knownHashes[chunkSize] = hash
					}
				}

				if err == io.EOF {
					break
				}
			}

			fileHash = hex.EncodeToString(fileHasher.Sum(nil))
		} else {
			// If it is not inplace, we want to reuse any chunks in the existing file regardless their offets, so
			// we run the chunk maker to split the original file.
			chunkMaker.ForEachChunk(
				existingFile,
				func(chunk *Chunk, final bool) {
					hash := chunk.GetHash()
					chunkSize := chunk.GetLength()
					existingChunks = append(existingChunks, hash)
					existingLengths = append(existingLengths, chunkSize)
					offsetMap[hash] = offset
					lengthMap[hash] = chunkSize
					offset += int64(chunkSize)
				},
				func(fileSize int64, hash string) (io.Reader, bool) {
					fileHash = hash
					return nil, false
				})
		}
		if fileHash == entry.Hash && fileHash != "" {
			LOG_TRACE("DOWNLOAD_SKIP", "File %s unchanged (by hash)", entry.Path)
			return false
		}
	}

	for i := entry.StartChunk; i <= entry.EndChunk; i++ {
		if _, found := offsetMap[chunkDownloader.taskList[i].chunkHash]; !found {
			chunkDownloader.taskList[i].needed = true
		}
	}

	chunkDownloader.Prefetch(entry)

	if inPlace {

		LOG_TRACE("DOWNLOAD_INPLACE", "Updating %s in place", fullPath)

		if existingFile == nil {
			// Create an empty file
			existingFile, err = os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
			if err != nil {
				LOG_ERROR("DOWNLOAD_CREATE", "Failed to create the file %s for in-place writing", fullPath)
			}
		} else {
			// Close and reopen in a different mode
			existingFile.Close()
			existingFile, err = os.OpenFile(fullPath, os.O_RDWR, 0)
			if err != nil {
				LOG_ERROR("DOWNLOAD_OPEN", "Failed to open the file %s for in-place writing", fullPath)
				return false
			}
		}

		existingFile.Seek(0, 0)

		j := 0
		offset := int64(0)
		existingOffset := int64(0)
		hasher := manager.config.NewFileHasher()

		for i := entry.StartChunk; i <= entry.EndChunk; i++ {

			for existingOffset < offset && j < len(existingChunks) {
				existingOffset += int64(existingLengths[j])
				j++
			}

			hash := chunkDownloader.taskList[i].chunkHash

			start := 0
			if i == entry.StartChunk {
				start = entry.StartOffset
			}
			end := chunkDownloader.taskList[i].chunkLength
			if i == entry.EndChunk {
				end = entry.EndOffset
			}

			_, err = existingFile.Seek(offset, 0)
			if err != nil {
				LOG_ERROR("DOWNLOAD_SEEK", "Failed to set the offset to %d for file %s: %v", offset, fullPath, err)
				return false
			}

			// Check if the chunk is available in the existing file
			if existingOffset == offset && start == 0 && j < len(existingChunks) &&
				end == existingLengths[j] && existingChunks[j] == hash {
				// Identical chunk found.  Run it through the hasher in order to compute the file hash.
				_, err := io.CopyN(hasher, existingFile, int64(existingLengths[j]))
				if err != nil {
					LOG_ERROR("DOWNLOAD_READ", "Failed to read the existing chunk %s: %v", hash, err)
					return false
				}
				if IsDebugging() {
					LOG_DEBUG("DOWNLOAD_UNCHANGED", "Chunk %s is unchanged", manager.config.GetChunkIDFromHash(hash))
				}
			} else {
				chunk := chunkDownloader.WaitForChunk(i)
				_, err = existingFile.Write(chunk.GetBytes()[start:end])
				if err != nil {
					LOG_ERROR("DOWNLOAD_WRITE", "Failed to write to the file: %v", err)
					return false
				}
				hasher.Write(chunk.GetBytes()[start:end])
			}

			offset += int64(end - start)
		}

		// Must truncate the file if the new size is smaller
		if err = existingFile.Truncate(offset); err != nil {
			LOG_ERROR("DOWNLOAD_TRUNCATE", "Failed to truncate the file at %d: %v", offset, err)
			return false
		}

		// Verify the download by hash
		hash := hex.EncodeToString(hasher.Sum(nil))
		if hash != entry.Hash && hash != "" && entry.Hash != "" && !strings.HasPrefix(entry.Hash, "#") {
			LOG_ERROR("DOWNLOAD_HASH", "File %s has a mismatched hash: %s instead of %s (in-place)",
				fullPath, "", entry.Hash)
			return false
		}

	} else {

		// Create the temporary file.
		newFile, err = os.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			LOG_ERROR("DOWNLOAD_OPEN", "Failed to open file for writing: %v", err)
			return false
		}

		hasher := manager.config.NewFileHasher()

		var localChunk *Chunk
		defer chunkDownloader.config.PutChunk(localChunk)

		var offset int64
		for i := entry.StartChunk; i <= entry.EndChunk; i++ {

			hasLocalCopy := false
			var data []byte

			hash := chunkDownloader.taskList[i].chunkHash
			if existingFile != nil {
				if offset, ok := offsetMap[hash]; ok {
					// Retrieve the chunk from the existing file.
					length := lengthMap[hash]
					existingFile.Seek(offset, 0)
					if localChunk == nil {
						localChunk = chunkDownloader.config.GetChunk()
					}
					localChunk.Reset(true)
					_, err = io.CopyN(localChunk, existingFile, int64(length))
					if err == nil {
						hasLocalCopy = true
						data = localChunk.GetBytes()
						if IsDebugging() {
							LOG_DEBUG("DOWNLOAD_LOCAL_COPY", "Local copy for chunk %s is available",
								manager.config.GetChunkIDFromHash(hash))
						}
					}
				}
			}

			if !hasLocalCopy {
				chunk := chunkDownloader.WaitForChunk(i)
				// If the chunk was downloaded from the storage, we may still need a portion of it.
				start := 0
				if i == entry.StartChunk {
					start = entry.StartOffset
				}
				end := chunk.GetLength()
				if i == entry.EndChunk {
					end = entry.EndOffset
				}
				data = chunk.GetBytes()[start:end]
			}

			_, err = newFile.Write(data)
			if err != nil {
				LOG_ERROR("DOWNLOAD_WRITE", "Failed to write file: %v", err)
				return false
			}

			hasher.Write(data)
			offset += int64(len(data))
		}

		hash := hex.EncodeToString(hasher.Sum(nil))
		if hash != entry.Hash && hash != "" && entry.Hash != "" && !strings.HasPrefix(entry.Hash, "#") {
			LOG_ERROR("DOWNLOAD_HASH", "File %s has a mismatched hash: %s instead of %s",
				entry.Path, hash, entry.Hash)
			return false
		}

		if existingFile != nil {
			existingFile.Close()
			existingFile = nil
		}

		newFile.Close()
		newFile = nil

		err = os.Remove(fullPath)
		if err != nil && !os.IsNotExist(err) {
			LOG_ERROR("DOWNLOAD_REMOVE", "Failed to remove the old file: %v", err)
			return false
		}

		err = os.Rename(temporaryPath, fullPath)
		if err != nil {
			LOG_ERROR("DOWNLOAD_RENAME", "Failed to rename the file %s to %s: %v", temporaryPath, fullPath, err)
			return false
		}
	}

	if !showStatistics {
		LOG_INFO("DOWNLOAD_DONE", "Downloaded %s (%d)", entry.Path, entry.Size)
	}
	return true
}

// CopySnapshots copies the specified snapshots from one storage to the other.
func (manager *BackupManager) CopySnapshots(otherManager *BackupManager, snapshotID string,
	revisionsToBeCopied []int, threads int) bool {

	if !manager.config.IsCompatiableWith(otherManager.config) {
		LOG_ERROR("CONFIG_INCOMPATIBLE", "Two storages are not compatible for the copy operation")
		return false
	}

	if snapshotID == "" && len(revisionsToBeCopied) > 0 {
		LOG_ERROR("SNAPSHOT_ERROR", "You must specify the snapshot id when one or more revisions are specified.")
		return false
	}

	revisionMap := make(map[string]map[int]bool)

	_, found := revisionMap[snapshotID]
	if !found {
		revisionMap[snapshotID] = make(map[int]bool)
	}

	for _, revision := range revisionsToBeCopied {
		revisionMap[snapshotID][revision] = true
	}

	var snapshots []*Snapshot
	var snapshotIDs []string
	var err error

	if snapshotID == "" {
		snapshotIDs, err = manager.SnapshotManager.ListSnapshotIDs()
		if err != nil {
			LOG_ERROR("COPY_LIST", "Failed to list all snapshot ids: %v", err)
			return false
		}
	} else {
		snapshotIDs = []string{snapshotID}
	}

	for _, id := range snapshotIDs {
		_, found := revisionMap[id]
		if !found {
			revisionMap[id] = make(map[int]bool)
		}
		revisions, err := manager.SnapshotManager.ListSnapshotRevisions(id)
		if err != nil {
			LOG_ERROR("SNAPSHOT_LIST", "Failed to list all revisions for snapshot %s: %v", id, err)
			return false
		}

		for _, revision := range revisions {
			if len(revisionsToBeCopied) > 0 {
				if _, found := revisionMap[id][revision]; found {
					revisionMap[id][revision] = true
				} else {
					revisionMap[id][revision] = false
					continue
				}
			} else {
				revisionMap[id][revision] = true
			}

			snapshotPath := fmt.Sprintf("snapshots/%s/%d", id, revision)
			exist, _, _, err := otherManager.storage.GetFileInfo(0, snapshotPath)
			if err != nil {
				LOG_ERROR("SNAPSHOT_INFO", "Failed to check if there is a snapshot %s at revision %d: %v",
					id, revision, err)
				return false
			}

			if exist {
				LOG_INFO("SNAPSHOT_EXIST", "Snapshot %s at revision %d already exists at the destination storage",
					id, revision)
				revisionMap[id][revision] = false
				continue
			}

			snapshot := manager.SnapshotManager.DownloadSnapshot(id, revision)
			snapshots = append(snapshots, snapshot)
		}

	}

	if len(snapshots) == 0 {
		LOG_INFO("SNAPSHOT_COPY", "Nothing to copy, all snapshot revisions exist at the destination.")
		return true
	}

	chunks := make(map[string]bool)
	otherChunks := make(map[string]bool)

	for _, snapshot := range snapshots {

		if revisionMap[snapshot.ID][snapshot.Revision] == false {
			continue
		}

		LOG_TRACE("SNAPSHOT_COPY", "Copying snapshot %s at revision %d", snapshot.ID, snapshot.Revision)

		for _, chunkHash := range snapshot.FileSequence {
			if _, found := chunks[chunkHash]; !found {
				chunks[chunkHash] = true
			}
		}

		for _, chunkHash := range snapshot.ChunkSequence {
			if _, found := chunks[chunkHash]; !found {
				chunks[chunkHash] = true
			}
		}

		for _, chunkHash := range snapshot.LengthSequence {
			if _, found := chunks[chunkHash]; !found {
				chunks[chunkHash] = true
			}
		}

		description := manager.SnapshotManager.DownloadSequence(snapshot.ChunkSequence)
		err := snapshot.LoadChunks(description)
		if err != nil {
			LOG_ERROR("SNAPSHOT_CHUNK", "Failed to load chunks for snapshot %s at revision %d: %v",
				snapshot.ID, snapshot.Revision, err)
			return false
		}

		for _, chunkHash := range snapshot.ChunkHashes {
			if _, found := chunks[chunkHash]; !found {
				chunks[chunkHash] = true
			}
		}

		snapshot.ChunkHashes = nil
	}

	otherChunkFiles, otherChunkSizes := otherManager.SnapshotManager.ListAllFiles(otherManager.storage, "chunks/")

	for i, otherChunkID := range otherChunkFiles {
		otherChunkID = strings.Replace(otherChunkID, "/", "", -1)
		if len(otherChunkID) != 64 {
			continue
		}
		if otherChunkSizes[i] == 0 {
			LOG_DEBUG("SNAPSHOT_COPY", "Chunk %s has length = 0", otherChunkID)
			continue
		}
		otherChunks[otherChunkID] = false
	}

	LOG_DEBUG("SNAPSHOT_COPY", "Found %d chunks on destination storage", len(otherChunks))

	chunksToCopy := 0
	chunksToSkip := 0

	for chunkHash := range chunks {
		otherChunkID := otherManager.config.GetChunkIDFromHash(chunkHash)
		if _, found := otherChunks[otherChunkID]; found {
			chunksToSkip++
		} else {
			chunksToCopy++
		}
	}

	LOG_DEBUG("SNAPSHOT_COPY", "Chunks to copy = %d, to skip = %d, total = %d", chunksToCopy, chunksToSkip, chunksToCopy+chunksToSkip)
	LOG_DEBUG("SNAPSHOT_COPY", "Total chunks in source snapshot revisions = %d\n", len(chunks))

	chunkDownloader := CreateChunkDownloader(manager.config, manager.storage, nil, false, threads)

	chunkUploader := CreateChunkUploader(otherManager.config, otherManager.storage, nil, threads,
		func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
			if skipped {
				LOG_INFO("SNAPSHOT_COPY", "Chunk %s (%d/%d) exists at the destination", chunk.GetID(), chunkIndex, len(chunks))
			} else {
				LOG_INFO("SNAPSHOT_COPY", "Chunk %s (%d/%d) copied to the destination", chunk.GetID(), chunkIndex, len(chunks))
			}
			otherManager.config.PutChunk(chunk)
		})

	chunkUploader.Start()

	totalCopied := 0
	totalSkipped := 0
	chunkIndex := 0

	for chunkHash := range chunks {
		chunkIndex++
		chunkID := manager.config.GetChunkIDFromHash(chunkHash)
		newChunkID := otherManager.config.GetChunkIDFromHash(chunkHash)
		if _, found := otherChunks[newChunkID]; !found {
			LOG_DEBUG("SNAPSHOT_COPY", "Copying chunk %s to %s", chunkID, newChunkID)
			i := chunkDownloader.AddChunk(chunkHash)
			chunk := chunkDownloader.WaitForChunk(i)
			newChunk := otherManager.config.GetChunk()
			newChunk.Reset(true)
			newChunk.Write(chunk.GetBytes())
			newChunk.encryptionVersion = chunk.encryptionVersion
			chunkUploader.StartChunk(newChunk, chunkIndex)
			totalCopied++
		} else {
			LOG_INFO("SNAPSHOT_COPY", "Chunk %s (%d/%d) skipped at the destination", chunkID, chunkIndex, len(chunks))
			totalSkipped++
		}
	}

	chunkDownloader.Stop()
	chunkUploader.Stop()

	LOG_INFO("SNAPSHOT_COPY", "Copy complete, %d total chunks, %d chunks copied, %d skipped", totalCopied+totalSkipped, totalCopied, totalSkipped)

	for _, snapshot := range snapshots {
		if revisionMap[snapshot.ID][snapshot.Revision] == false {
			continue
		}
		otherManager.storage.CreateDirectory(0, fmt.Sprintf("snapshots/%s", snapshot.ID))
		description, _ := snapshot.MarshalJSON()
		path := fmt.Sprintf("snapshots/%s/%d", snapshot.ID, snapshot.Revision)
		otherManager.SnapshotManager.UploadFile(path, path, description)
		LOG_INFO("SNAPSHOT_COPY", "Copied snapshot %s at revision %d", snapshot.ID, snapshot.Revision)
	}

	return true
}
