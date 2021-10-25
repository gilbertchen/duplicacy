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

    "github.com/vmihailenco/msgpack"
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
	excludeByAttribute bool // don't backup file based on file attribute

	cachePath string
}

func (manager *BackupManager) SetDryRun(dryRun bool) {
	manager.config.dryRun = dryRun
}

// CreateBackupManager creates a backup manager using the specified 'storage'.  'snapshotID' is a unique id to
// identify snapshots created for this repository.  'top' is the top directory of the repository.  'password' is the
// master key which can be nil if encryption is not enabled.
func CreateBackupManager(snapshotID string, storage Storage, top string, password string, nobackupFile string, filtersFile string, excludeByAttribute bool) *BackupManager {

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

		excludeByAttribute: excludeByAttribute,
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

	manager.cachePath = path.Join(preferencePath, "cache", storageName)

	storage.SetDefaultNestingLevels([]int{1}, 1)
	manager.snapshotCache = storage
	manager.SnapshotManager.snapshotCache = storage
	return true
}

// Backup creates a snapshot for the repository 'top'.  If 'quickMode' is true, only files with different sizes
// or timestamps since last backup will be uploaded (however the snapshot is still a full snapshot that shares
// unmodified files with last backup).  Otherwise (or if this is the first backup), the entire repository will
// be scanned to create the snapshot.  'tag' is the tag assigned to the new snapshot.
func (manager *BackupManager) Backup(top string, quickMode bool, threads int, tag string,
	showStatistics bool, shadowCopy bool, shadowCopyTimeout int, enumOnly bool, metadataChunkSize int, maximumInMemoryEntries int) bool {

	var err error
	top, err = filepath.Abs(top)
	if err != nil {
		LOG_ERROR("REPOSITORY_ERR", "Failed to obtain the absolute path of the repository: %v", err)
		return false
	}

	startTime := time.Now().Unix()

	LOG_DEBUG("BACKUP_PARAMETERS", "top: %s, quick: %t, tag: %s", top, quickMode, tag)

	if manager.config.DataShards != 0 && manager.config.ParityShards != 0 {
		LOG_INFO("BACKUP_ERASURECODING", "Erasure coding is enabled with %d data shards and %d parity shards",
		         manager.config.DataShards, manager.config.ParityShards)
	}

	if manager.config.rsaPublicKey != nil && len(manager.config.FileKey) > 0 {
		LOG_INFO("BACKUP_KEY", "RSA encryption is enabled")
	}

	if manager.excludeByAttribute {
		LOG_INFO("BACKUP_EXCLUDE", "Exclude files with no-backup attributes")
	}

	remoteSnapshot := manager.SnapshotManager.downloadLatestSnapshot(manager.snapshotID)
	if remoteSnapshot == nil {
		LOG_INFO("BACKUP_START", "No previous backup found")
		remoteSnapshot = CreateEmptySnapshot(manager.snapshotID)
	} else {
		LOG_INFO("BACKUP_START", "Last backup at revision %d found", remoteSnapshot.Revision)
	}

	hashMode := remoteSnapshot.Revision == 0 || !quickMode

	// This cache contains all chunks referenced by last snasphot. Any other chunks will lead to a call to
	// UploadChunk.
	chunkCache := make(map[string]bool)

	// A revision number of 0 means this is the initial backup
	if remoteSnapshot.Revision > 0 {
		manager.SnapshotManager.DownloadSnapshotSequences(remoteSnapshot)
		// Add all chunks in the last snapshot to the cache
		for _, chunkID := range manager.SnapshotManager.GetSnapshotChunks(remoteSnapshot, true) {
			chunkCache[chunkID] = true
		}
	}

	var incompleteSnapshot *EntryList
	if hashMode {
		incompleteSnapshot = loadIncompleteSnapshot(manager.snapshotID, manager.cachePath)
	}

	// If the listing operation is fast and this is an initial backup, list all chunks and
	// put them in the cache.
	if (manager.storage.IsFastListing() && remoteSnapshot.Revision == 0) {
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

		// Make sure that all chunks in the incomplete snapshot must exist in the storage
		if incompleteSnapshot != nil && !incompleteSnapshot.CheckChunks(manager.config, chunkCache) {
			LOG_WARN("INCOMPLETE_DISCARD", "The incomplete snapshot can't be used as it contains chunks not in the storage")
			incompleteSnapshot = nil
		}
	}

	// Copy over chunks from the incomplete snapshot
	if incompleteSnapshot != nil {
		remoteSnapshot.ChunkHashes = append(incompleteSnapshot.PreservedChunkHashes, incompleteSnapshot.UploadedChunkHashes...)
		remoteSnapshot.ChunkLengths = append(incompleteSnapshot.PreservedChunkLengths, incompleteSnapshot.UploadedChunkLengths...)
	}

	shadowTop := CreateShadowCopy(top, shadowCopy, shadowCopyTimeout)
	defer DeleteShadowCopy()

	var totalModifiedFileSize int64    // total size of modified files
	var uploadedModifiedFileSize int64 // portions that have been uploaded (including cache hits)
	var preservedFileSize int64 // total size of unmodified files

	localSnapshot := CreateEmptySnapshot(manager.snapshotID)
	localSnapshot.Revision = remoteSnapshot.Revision + 1

	localListingChannel := make(chan *Entry)
	remoteListingChannel := make(chan *Entry)
	chunkOperator := CreateChunkOperator(manager.config, manager.storage, manager.snapshotCache, showStatistics, threads, false)

	var skippedDirectories []string
	var skippedFiles []string

	LOG_INFO("BACKUP_INDEXING", "Indexing %s", top)
	go func() {
		// List local files
		defer CatchLogException()
		localSnapshot.ListLocalFiles(shadowTop, manager.nobackupFile, manager.filtersFile, manager.excludeByAttribute, localListingChannel, &skippedDirectories, &skippedFiles)
	} ()

	go func() {
		// List remote files
		defer CatchLogException()

		if incompleteSnapshot != nil {
			// If there is an incomplete snapshot, always use it
			incompleteSnapshot.ReadEntries(func(entry *Entry) error {
				remoteListingChannel <- entry
				return nil
			})
		} else if hashMode {
			// No need to list remote files for a hash mode backup
		} else {
			// List remote files in the previous snapshot
			remoteSnapshot.ListRemoteFiles(manager.config, chunkOperator, func(entry *Entry) bool {
				remoteListingChannel <- entry
				return true
			})
		}
		close(remoteListingChannel)
	} ()

	// Create the local file list
	localEntryList, err := CreateEntryList(manager.snapshotID, manager.cachePath, maximumInMemoryEntries)
	if err != nil {
		LOG_ERROR("BACKUP_CREATE", "Failed to create the entry list: %v", err)
		return false
	}
	lastPreservedChunk := -1

	// Now compare local files with remote files one by one
	var remoteEntry *Entry
	remoteListingOK := true
	for {
		localEntry := <- localListingChannel
		if localEntry == nil {
			break
		}

		// compareResult < 0: local entry has no remote counterpart
		// compareResult == 0: local entry may or may not be the same as the remote one
		// compareResult > 0: remote entry is extra - skip it and get the next remote entry while keeping the same local entry
		var compareResult int
		for {
			if remoteEntry != nil {
				compareResult = localEntry.Compare(remoteEntry)
			} else {
				if remoteListingOK {
					remoteEntry, remoteListingOK = <- remoteListingChannel
				}
				if !remoteListingOK {
					compareResult = -1
					break
				}
				compareResult = localEntry.Compare(remoteEntry)
			}

			if compareResult <= 0 {
				break
			}
			remoteEntry = nil
		}

		if compareResult == 0  {
			// No need to check if it is in hash mode -- in that case remote listing is nil
			if localEntry.IsSameAs(remoteEntry) && localEntry.IsFile() {

				localEntry.Hash = remoteEntry.Hash
				localEntry.StartOffset = remoteEntry.StartOffset
				localEntry.EndOffset = remoteEntry.EndOffset
				delta := remoteEntry.StartChunk - len(localEntryList.PreservedChunkHashes)
				if lastPreservedChunk != remoteEntry.StartChunk {
					lastPreservedChunk = remoteEntry.StartChunk
					localEntryList.AddPreservedChunk(remoteSnapshot.ChunkHashes[lastPreservedChunk], remoteSnapshot.ChunkLengths[lastPreservedChunk])
				} else {
					delta++
				}

				for i := remoteEntry.StartChunk + 1; i <= remoteEntry.EndChunk; i++ {
					localEntryList.AddPreservedChunk(remoteSnapshot.ChunkHashes[i], remoteSnapshot.ChunkLengths[i])
					lastPreservedChunk = i
				}

				localEntry.StartChunk = remoteEntry.StartChunk - delta
				localEntry.EndChunk = remoteEntry.EndChunk - delta
				preservedFileSize += localEntry.Size
			} else {
				totalModifiedFileSize += localEntry.Size
				if localEntry.Size > 0 {
					localEntry.Size = -1
				}
			}
			remoteEntry = nil
		} else {
			// compareResult must be < 0; the local file is new
			totalModifiedFileSize += localEntry.Size
			if localEntry.Size > 0 {
					// A size of -1 indicates this is a modified file that will be uploaded
					localEntry.Size = -1
			}
		}

		localEntryList.AddEntry(localEntry)
	}

	if enumOnly {
		return true
	}

	if localEntryList.NumberOfEntries == 0 {
		LOG_ERROR("SNAPSHOT_EMPTY", "No files under the repository to be backed up")
		return false
	}

	fileChunkMaker := CreateFileChunkMaker(manager.config, false)

	keepUploadAlive := int64(1800)

	if os.Getenv("DUPLICACY_UPLOAD_KEEPALIVE") != "" {
		value, _ := strconv.Atoi(os.Getenv("DUPLICACY_UPLOAD_KEEPALIVE"))
		if value < 10 {
			value = 10
		}
		LOG_INFO("UPLOAD_KEEPALIVE", "Setting KeepUploadAlive to %d second", value)
		keepUploadAlive = int64(value)
	}

	// Fail at the chunk specified by DUPLICACY_FAIL_CHUNK to simulate a backup error
	chunkToFail := -1
	if value, found := os.LookupEnv("DUPLICACY_FAIL_CHUNK"); found {
		chunkToFail, _ = strconv.Atoi(value)
		LOG_INFO("SNAPSHOT_FAIL", "Will abort the backup on chunk %d", chunkToFail)
	}

	if incompleteSnapshot != nil {
		incompleteSnapshot.CloseOnDiskFile()
	}

	var once sync.Once
	if hashMode {
		// In case an error occurs during a hash mode backup, save the incomplete snapshot
		RunAtError = func() {
			once.Do(func() {
				localEntryList.SaveIncompleteSnapshot()
			})
		}
	}

	startUploadingTime := time.Now().Unix()
	lastUploadingTime := time.Now().Unix()

	var numberOfNewFileChunks int64        // number of new file chunks
	var totalUploadedFileChunkLength int64 // total length of uploaded file chunks
	var totalUploadedFileChunkBytes int64  // how many actual bytes have been uploaded

	// This function is called when a chunk has been uploaded
	uploadChunkCompletionFunc := func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int) {

		localEntryList.AddUploadedChunk(chunkIndex, chunk.GetHash(), chunkSize)

		action := "Skipped"
		if inCache {
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

		manager.config.PutChunk(chunk)
	}

	chunkOperator.UploadCompletionFunc = uploadChunkCompletionFunc

	chunkIndex := -1
	// This function is called when the chunk maker generates a new chunk
	uploadChunkFunc := func(chunk *Chunk) {
		chunkID := chunk.GetID()
		chunkSize := chunk.GetLength()

		chunkIndex++

		_, found := chunkCache[chunkID]
		if found {
			if time.Now().Unix() - lastUploadingTime > keepUploadAlive {
				LOG_INFO("UPLOAD_KEEPALIVE", "Skip chunk cache to keep connection alive")
				found = false
			}
		}

		if found {
			uploadChunkCompletionFunc(chunk, chunkIndex, true, chunkSize, 0)
		} else {
			lastUploadingTime = time.Now().Unix()
			chunkCache[chunkID] = true

			chunkOperator.Upload(chunk, chunkIndex, false)
		}

		if chunkIndex == chunkToFail {
			LOG_ERROR("SNAPSHOT_FAIL", "Artificially fail the chunk %d for testing purposes", chunkToFail)
		}
	}

	// These are files to be uploaded; directories and links are excluded
	for i := range localEntryList.ModifiedEntries {
		entry := &localEntryList.ModifiedEntries[i]
		LOG_TRACE("PACK_START", "Packing %s", entry.Path)
		fullPath := joinPath(shadowTop, entry.Path)
		file, err := os.OpenFile(fullPath, os.O_RDONLY, 0)
		if err != nil {
			LOG_WARN("OPEN_FAILURE", "Failed to open file for reading: %v", err)
			skippedFiles = append(skippedFiles, entry.Path)
			continue
		}
		entry.Size, entry.Hash = fileChunkMaker.AddData(file, uploadChunkFunc)
		if !showStatistics || IsTracing() || RunInBackground {
			LOG_INFO("PACK_END", "Packed %s (%d)", entry.Path, entry.Size)
		}
		file.Close()

	}

	// This flushes the chunk maker (forcing all remaining data to be sent in chunks)
	fileChunkMaker.AddData(nil, uploadChunkFunc)
	chunkOperator.WaitForCompletion()

	localSnapshot.EndTime = time.Now().Unix()

	localSnapshot.Tag = tag
	localSnapshot.Options = ""
	if hashMode {
		localSnapshot.Options = "-hash"
	}

	if shadowCopy {
		if localSnapshot.Options == "" {
			localSnapshot.Options = "-vss"
		} else {
			localSnapshot.Options += " -vss"
		}
	}

	var uploadedFileSize int64
	var totalFileChunkLength int64
	for _, entry := range localEntryList.ModifiedEntries {
		uploadedFileSize += entry.Size
	}
	for _, length := range localEntryList.PreservedChunkLengths {
		totalFileChunkLength += int64(length)
	}
	for _, length := range localEntryList.UploadedChunkLengths {
		totalFileChunkLength += int64(length)
	}

	localSnapshot.FileSize = preservedFileSize + uploadedFileSize
	localSnapshot.NumberOfFiles = localEntryList.NumberOfEntries - int64(len(skippedFiles))
	localSnapshot.ChunkHashes = append(localEntryList.PreservedChunkHashes, localEntryList.UploadedChunkHashes...)
	localSnapshot.ChunkLengths = append(localEntryList.PreservedChunkLengths, localEntryList.UploadedChunkLengths...)

	totalMetadataChunkLength, numberOfNewMetadataChunks,
		totalUploadedMetadataChunkLength, totalUploadedMetadataChunkBytes :=
		manager.UploadSnapshot(chunkOperator, top, localSnapshot, localEntryList, chunkCache, metadataChunkSize)

	if showStatistics && !RunInBackground {
		for _, entry := range localEntryList.ModifiedEntries {
			if entry.Size < 0 {
				continue
			}
			LOG_INFO("UPLOAD_FILE", "Uploaded %s (%d)", entry.Path, entry.Size)
		}
	}

	for _, dir := range skippedDirectories {
		LOG_WARN("SKIP_DIRECTORY", "Subdirectory %s cannot be listed", dir)
	}

	for _, file := range skippedFiles {
		LOG_WARN("SKIP_FILE", "File %s cannot be opened", file)
	}

	if !manager.config.dryRun {
		manager.SnapshotManager.CleanSnapshotCache(localSnapshot, nil)
	}
	LOG_INFO("BACKUP_END", "Backup for %s at revision %d completed", top, localSnapshot.Revision)

	RunAtError = func() {}
	deleteIncompleteSnapshot(manager.cachePath)

	totalMetadataChunks := len(localSnapshot.FileSequence) + len(localSnapshot.ChunkSequence) +
		len(localSnapshot.LengthSequence)
	if showStatistics {

		LOG_INFO("BACKUP_STATS", "Files: %d total, %s bytes; %d new, %s bytes",
			localEntryList.NumberOfEntries - int64(len(skippedFiles)),
			PrettyNumber(preservedFileSize+uploadedFileSize),
			len(localEntryList.ModifiedEntries), PrettyNumber(uploadedFileSize))

		LOG_INFO("BACKUP_STATS", "File chunks: %d total, %s bytes; %d new, %s bytes, %s bytes uploaded",
			len(localSnapshot.ChunkHashes), PrettyNumber(totalFileChunkLength),
			numberOfNewFileChunks, PrettyNumber(totalUploadedFileChunkLength),
			PrettyNumber(totalUploadedFileChunkBytes))

		LOG_INFO("BACKUP_STATS", "Metadata chunks: %d total, %s bytes; %d new, %s bytes, %s bytes uploaded",
			totalMetadataChunks, PrettyNumber(totalMetadataChunkLength),
			numberOfNewMetadataChunks, PrettyNumber(totalUploadedMetadataChunkLength),
			PrettyNumber(totalUploadedMetadataChunkBytes))

		LOG_INFO("BACKUP_STATS", "All chunks: %d total, %s bytes; %d new, %s bytes, %s bytes uploaded",
			len(localSnapshot.ChunkHashes)+totalMetadataChunks,
			PrettyNumber(totalFileChunkLength+totalMetadataChunkLength),
			int(numberOfNewFileChunks)+numberOfNewMetadataChunks,
			PrettyNumber(totalUploadedFileChunkLength+totalUploadedMetadataChunkLength),
			PrettyNumber(totalUploadedFileChunkBytes+totalUploadedMetadataChunkBytes))

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

	chunkOperator.Stop()

	return true
}

// Restore downloads the specified snapshot, compares it with what's on the repository, and then downloads
// files that are different. 'base' is a directory that contains files at a different revision which can
// serve as a local cache to avoid download chunks available locally.  It is perfectly ok for 'base' to be
// the same as 'top'.  'quickMode' will bypass files with unchanged sizes and timestamps.  'deleteMode' will
// remove local files that don't exist in the snapshot. 'patterns' is used to include/exclude certain files.
func (manager *BackupManager) Restore(top string, revision int, inPlace bool, quickMode bool, threads int, overwrite bool,
	deleteMode bool, setOwner bool, showStatistics bool, patterns []string, allowFailures bool) int {

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
			return 0
		}
	}

	err = os.Mkdir(path.Join(top, DUPLICACY_DIRECTORY), 0744)
	if err != nil && !os.IsExist(err) {
		LOG_ERROR("RESTORE_MKDIR", "Failed to create the preference directory: %v", err)
		return 0
	}

	// local files that don't exist in the remote snapshot
	var extraFiles []string

	// These will store files/directories to be downloaded.
	fileEntries := make([]*Entry, 0)
	directoryEntries := make([]*Entry, 0)

	var totalFileSize int64
	var downloadedFileSize int64
	var failedFileCount int
	var skippedFileSize int64
	var skippedFileCount int64
	var downloadedFiles []*Entry

	localSnapshot := CreateEmptySnapshot(manager.snapshotID)

	localListingChannel := make(chan *Entry)
	remoteListingChannel := make(chan *Entry)
	chunkOperator := CreateChunkOperator(manager.config, manager.storage, manager.snapshotCache, showStatistics, threads, false)

	LOG_INFO("RESTORE_INDEXING", "Indexing %s", top)
	go func() {
		// List local files
		defer CatchLogException()
		localSnapshot.ListLocalFiles(top, manager.nobackupFile, manager.filtersFile, manager.excludeByAttribute, localListingChannel, nil, nil)
	} ()

	remoteSnapshot := manager.SnapshotManager.DownloadSnapshot(manager.snapshotID, revision)
	manager.SnapshotManager.DownloadSnapshotSequences(remoteSnapshot)
	go func() {
		// List remote files
		defer CatchLogException()
		remoteSnapshot.ListRemoteFiles(manager.config, chunkOperator, func(entry *Entry) bool {
			remoteListingChannel <- entry
			return true
		})
		close(remoteListingChannel)
	} ()

	var localEntry *Entry
	localListingOK := true

	for remoteEntry := range remoteListingChannel {

		if len(patterns) > 0 && !MatchPath(remoteEntry.Path, patterns) {
			continue
		}

		// remoteEntry is valid; now find the matching localEntry
		var compareResult int

		for {
			if localEntry == nil && localListingOK {
				localEntry, localListingOK = <- localListingChannel
			}
			if localEntry == nil {
				compareResult = 1
			} else {
				compareResult = localEntry.Compare(remoteEntry)
				if compareResult < 0 {
					extraFiles = append(extraFiles, localEntry.Path)
					localEntry = nil
					continue
				}
			}
			break
		}

		if compareResult == 0 {
			if quickMode && localEntry.IsFile() && localEntry.IsSameAs(remoteEntry) {
				LOG_TRACE("RESTORE_SKIP", "File %s unchanged (by size and timestamp)", localEntry.Path)
				skippedFileSize += localEntry.Size
				skippedFileCount++
				localEntry = nil
				continue
			}
			localEntry = nil
		}

		fullPath := joinPath(top, remoteEntry.Path)
		if remoteEntry.IsLink() {
			stat, err := os.Lstat(fullPath)
			if stat != nil {
				if stat.Mode()&os.ModeSymlink != 0 {
					isRegular, link, err := Readlink(fullPath)
					if err == nil && link == remoteEntry.Link && !isRegular {
						remoteEntry.RestoreMetadata(fullPath, nil, setOwner)
						continue
					}
				}

				os.Remove(fullPath)
			}

			err = os.Symlink(remoteEntry.Link, fullPath)
			if err != nil {
				LOG_ERROR("RESTORE_SYMLINK", "Can't create symlink %s: %v", remoteEntry.Path, err)
				return 0
			}
			remoteEntry.RestoreMetadata(fullPath, nil, setOwner)
			LOG_TRACE("DOWNLOAD_DONE", "Symlink %s updated", remoteEntry.Path)
		} else if remoteEntry.IsDir() {

			stat, err := os.Stat(fullPath)

			if err == nil && !stat.IsDir() {
				LOG_ERROR("RESTORE_NOTDIR", "The path %s is not a directory", fullPath)
				return 0
			}

			if os.IsNotExist(err) {
				// In the first pass of directories, set the directory to be user readable so we can create new files
				// under it.
				err = os.MkdirAll(fullPath, 0700)
				if err != nil && !os.IsExist(err) {
					LOG_ERROR("RESTORE_MKDIR", "%v", err)
					return 0
				}
			}
			directoryEntries = append(directoryEntries, remoteEntry)
		} else {
			// We can't download files here since fileEntries needs to be sorted
			fileEntries = append(fileEntries, remoteEntry)
			totalFileSize += remoteEntry.Size
		}
	}

	if localEntry != nil {
		extraFiles = append(extraFiles, localEntry.Path)
	}

	for localListingOK {
		localEntry, localListingOK = <- localListingChannel
		if localEntry != nil {
			extraFiles = append(extraFiles, localEntry.Path)
		}
	}

	LOG_INFO("RESTORE_START", "Restoring %s to revision %d", top, revision)

	// The same chunk may appear in the chunk list multiple times.  This is to find the first
	// occurrence for each chunk
	chunkMap := make(map[string]int)
	for i, chunk := range remoteSnapshot.ChunkHashes {
		if _, found := chunkMap[chunk]; !found {
			chunkMap[chunk] = i
		}
	}

	// For small files that span only one chunk, use the first chunk instead
	for _, file := range fileEntries {
		if file.StartChunk == file.EndChunk {
			first := chunkMap[remoteSnapshot.ChunkHashes[file.StartChunk]]
			file.StartChunk = first
			file.EndChunk = first
		}
	}

	// Sort entries by their starting chunks in order to linearize the access to the chunk chain.
	sort.Sort(ByChunk(fileEntries))

	chunkDownloader := CreateChunkDownloader(chunkOperator)

	chunkDownloader.AddFiles(remoteSnapshot, fileEntries)

	chunkMaker := CreateFileChunkMaker(manager.config, true)

	startDownloadingTime := time.Now().Unix()

	// Now download files one by one
	for _, file := range fileEntries {

		fullPath := joinPath(top, file.Path)
		stat, _ := os.Stat(fullPath)
		if stat != nil {
			if quickMode {
				if file.IsSameAsFileInfo(stat) {
					LOG_TRACE("RESTORE_SKIP", "File %s unchanged (by size and timestamp)", file.Path)
					skippedFileSize += file.Size
					skippedFileCount++
					continue
				}
			}

			if file.Size == 0 && file.IsSameAsFileInfo(stat) {
				LOG_TRACE("RESTORE_SKIP", "File %s unchanged (size 0)", file.Path)
				skippedFileSize += file.Size
				skippedFileCount++
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
				return 0
			}
			newFile.Close()

			file.RestoreMetadata(fullPath, nil, setOwner)
			if !showStatistics {
				LOG_INFO("DOWNLOAD_DONE", "Downloaded %s (0)", file.Path)
				downloadedFileSize += file.Size
				downloadedFiles = append(downloadedFiles, file)
			}

			continue
		}

		downloaded, err := manager.RestoreFile(chunkDownloader, chunkMaker, file, top, inPlace, overwrite, showStatistics,
			totalFileSize, downloadedFileSize, startDownloadingTime, allowFailures)
		if err != nil {
			// RestoreFile returned an error; if allowFailures is false RestoerFile would error out and not return so here
			// we just need to show a warning
			failedFileCount++
			LOG_WARN("DOWNLOAD_FAIL", "Failed to restore %s: %v", file.Path, err)
			continue
		}

		// No error
		if downloaded {
			// No error, file was restored
			downloadedFileSize += file.Size
			downloadedFiles = append(downloadedFiles, file)
		} else {
			// No error, file was skipped
			skippedFileSize += file.Size
			skippedFileCount++
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

	for _, entry := range directoryEntries {
		dir := joinPath(top, entry.Path)
		entry.RestoreMetadata(dir, nil, setOwner)
	}

	if showStatistics {
		for _, file := range downloadedFiles {
			LOG_INFO("DOWNLOAD_DONE", "Downloaded %s (%d)", file.Path, file.Size)
		}
	}

	if failedFileCount > 0 {
		return failedFileCount
	}

	LOG_INFO("RESTORE_END", "Restored %s to revision %d", top, revision)
	if showStatistics {
		LOG_INFO("RESTORE_STATS", "Files: %d total, %s bytes", len(fileEntries), PrettySize(totalFileSize))
		LOG_INFO("RESTORE_STATS", "Downloaded %d file, %s bytes, %d chunks",
			len(downloadedFiles), PrettySize(downloadedFileSize), chunkDownloader.numberOfDownloadedChunks)
		LOG_INFO("RESTORE_STATS", "Skipped %d file, %s bytes", skippedFileCount, PrettySize(skippedFileSize))
	}

	runningTime := time.Now().Unix() - startTime
	if runningTime == 0 {
		runningTime = 1
	}

	LOG_INFO("RESTORE_STATS", "Total running time: %s", PrettyTime(runningTime))

	chunkOperator.Stop()

	return 0
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
func (manager *BackupManager) UploadSnapshot(chunkOperator *ChunkOperator, top string, snapshot *Snapshot,
	entryList *EntryList, chunkCache map[string]bool, metadataChunkSize int) (totalMetadataChunkSize int64,
	numberOfNewMetadataChunks int, totalUploadedMetadataChunkSize int64,
	totalUploadedMetadataChunkBytes int64) {

	uploadCompletionFunc := func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int) {
		if inCache {
			LOG_DEBUG("CHUNK_CACHE", "Skipped metadata chunk %s in cache", chunk.GetID())
		} else {
			if uploadSize > 0 {
				numberOfNewMetadataChunks++
				totalUploadedMetadataChunkSize += int64(chunkSize)
				totalUploadedMetadataChunkBytes += int64(uploadSize)
			} else {
				LOG_DEBUG("CHUNK_EXIST", "Skipped metadata chunk %s in the storage", chunk.GetID())
			}
		}

		manager.config.PutChunk(chunk)
	}

	chunkOperator.UploadCompletionFunc = uploadCompletionFunc

	chunkIndex := -1
	var chunkSequence []string

	uploadChunkFunc := func(chunk *Chunk) {
		hash := chunk.GetHash()
		chunkID := chunk.GetID()
		chunkSize := chunk.GetLength()

		chunkIndex++
		totalMetadataChunkSize += int64(chunkSize)

		_, found := chunkCache[chunkID]
		if found {
			uploadCompletionFunc(chunk, chunkIndex, true, chunkSize, 0)
		} else {
			chunkCache[chunkID] = true
			chunkOperator.Upload(chunk, chunkIndex, true)
		}

		chunkSequence = append(chunkSequence, hash)
	}

	buffer := new(bytes.Buffer)
	encoder := msgpack.NewEncoder(buffer)
	metadataChunkMaker := CreateMetaDataChunkMaker(manager.config, metadataChunkSize)

    var chunkHashes  []string
	var chunkLengths []int
	lastChunk := -1

	lastEndChunk := 0

	uploadEntryInfoFunc := func(entry *Entry) error {

		delta := entry.StartChunk - len(chunkHashes) + 1
		if entry.StartChunk != lastChunk {
			chunkHashes = append(chunkHashes, snapshot.ChunkHashes[entry.StartChunk])
			chunkLengths = append(chunkLengths, snapshot.ChunkLengths[entry.StartChunk])
			delta--
		}

		for i := entry.StartChunk + 1; i <= entry.EndChunk; i++ {
			chunkHashes = append(chunkHashes, snapshot.ChunkHashes[i])
			chunkLengths = append(chunkLengths, snapshot.ChunkLengths[i])
		}

		lastChunk = entry.EndChunk
		entry.StartChunk -= delta
		entry.EndChunk -= delta

		if entry.IsFile() {
			delta := entry.EndChunk - entry.StartChunk
			entry.StartChunk -= lastEndChunk
			lastEndChunk = entry.EndChunk
			entry.EndChunk = delta
		}

		buffer.Reset()
		err := encoder.Encode(entry)
		if err != nil {
			LOG_ERROR("SNAPSHOT_UPLOAD", "Metadata for %s can't be encoded: %v", entry.Path, err)
			return err
		}

		metadataChunkMaker.AddData(buffer, uploadChunkFunc)
		return nil
	}

	err := entryList.ReadEntries(uploadEntryInfoFunc)
	if err != nil {
		LOG_ERROR("SNAPSHOT_UPLOAD", "The file list contains an error: %v", err)
		return 0, 0, 0, 0
	}

	snapshot.ChunkHashes = chunkHashes
	snapshot.ChunkLengths = chunkLengths

	metadataChunkMaker.AddData(nil, uploadChunkFunc)
	snapshot.SetSequence("files", chunkSequence)

	// Chunk and length sequences can be encoded and loaded into memory directly
	for _, sequenceType := range []string{"chunks", "lengths"} {

		chunkSequence = nil
		contents, err := snapshot.MarshalSequence(sequenceType)

		if err != nil {
			LOG_ERROR("SNAPSHOT_MARSHAL", "Failed to encode the %s in the snapshot %s: %v",
				sequenceType, manager.snapshotID, err)
			return int64(0), 0, int64(0), int64(0)
		}

		metadataChunkMaker = CreateMetaDataChunkMaker(manager.config, metadataChunkSize)
		metadataChunkMaker.AddData(bytes.NewBuffer(contents), uploadChunkFunc)
		metadataChunkMaker.AddData(nil, uploadChunkFunc)

		snapshot.SetSequence(sequenceType, chunkSequence)

	}

	chunkOperator.WaitForCompletion()

	if _, found := os.LookupEnv("DUPLICACY_FAIL_SNAPSHOT"); found {
		LOG_ERROR("SNAPSHOT_FAIL", "Artificially fail the backup for testing purposes")
		return 0, 0, 0, 0
	}

	description, err := snapshot.MarshalJSON()
	if err != nil {
		LOG_ERROR("SNAPSHOT_MARSHAL", "Failed to encode the snapshot %s: %v", manager.snapshotID, err)
		return int64(0), 0, int64(0), int64(0)
	}

	path := fmt.Sprintf("snapshots/%s/%d", manager.snapshotID, snapshot.Revision)
	if !manager.config.dryRun {
		manager.SnapshotManager.UploadFile(path, path, description)
	}
	return totalMetadataChunkSize, numberOfNewMetadataChunks, totalUploadedMetadataChunkSize, totalUploadedMetadataChunkBytes
}

// Restore downloads a file from the storage.  If 'inPlace' is false, the download file is saved first to a temporary
// file under the .duplicacy directory and then replaces the existing one.  Otherwise, the existing file will be
// overwritten directly.
// Return: true, nil:    Restored file; 
//         false, nil:   Skipped file; 
//         false, error: Failure to restore file (only if allowFailures == true)
func (manager *BackupManager) RestoreFile(chunkDownloader *ChunkDownloader, chunkMaker *ChunkMaker, entry *Entry, top string, inPlace bool, overwrite bool,
	showStatistics bool, totalFileSize int64, downloadedFileSize int64, startTime int64, allowFailures bool) (bool, error) {

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
					return false, nil
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
					return false, nil
				}
				_, err = existingFile.Write([]byte("\x00\x00")[:n])
				if err != nil {
					LOG_ERROR("DOWNLOAD_CREATE", "Failed to initialize the sparse file %s for in-place writing: %v", fullPath, err)
					return false, nil
				}
				existingFile.Close()
				existingFile, err = os.Open(fullPath)
				if err != nil {
					LOG_ERROR("DOWNLOAD_OPEN", "Can't reopen the initial file just created: %v", err)
					return false, nil
				}
				isNewFile = true
			}
		} else {
			LOG_TRACE("DOWNLOAD_OPEN", "Can't open the existing file: %v", err)
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
						return false, nil
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

			if fileHash == entry.Hash && fileHash != "" {
				LOG_TRACE("DOWNLOAD_SKIP", "File %s unchanged (by hash)", entry.Path)
				return false, nil
			}

			// fileHash != entry.Hash, warn/error depending on -overwrite option
			if !overwrite && !isNewFile {
				LOG_WERROR(allowFailures, "DOWNLOAD_OVERWRITE",
							"File %s already exists.  Please specify the -overwrite option to overwrite", entry.Path)
				return false, fmt.Errorf("file exists")
			}

		} else {
			// If it is not inplace, we want to reuse any chunks in the existing file regardless their offets, so
			// we run the chunk maker to split the original file.

			offset := int64(0)
			chunkFunc := func(chunk *Chunk) {
				hash := chunk.GetHash()
				chunkSize := chunk.GetLength()
				existingChunks = append(existingChunks, hash)
				existingLengths = append(existingLengths, chunkSize)
				offsetMap[hash] = offset
				lengthMap[hash] = chunkSize
				offset += int64(chunkSize)
			}

			chunkMaker.AddData(existingFile, chunkFunc)
			chunkMaker.AddData(nil, chunkFunc)
		}

		// This is an additional check comparing fileHash to entry.Hash above, so this should no longer occur
		if fileHash == entry.Hash && fileHash != "" {
			LOG_TRACE("DOWNLOAD_SKIP", "File %s unchanged (by hash)", entry.Path)
			return false, nil
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
				return false, nil
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
				return false, nil
			}

			// Check if the chunk is available in the existing file
			if existingOffset == offset && start == 0 && j < len(existingChunks) &&
				end == existingLengths[j] && existingChunks[j] == hash {
				// Identical chunk found.  Run it through the hasher in order to compute the file hash.
				_, err := io.CopyN(hasher, existingFile, int64(existingLengths[j]))
				if err != nil {
					LOG_ERROR("DOWNLOAD_READ", "Failed to read the existing chunk %s: %v", hash, err)
					return false, nil
				}
				if IsDebugging() {
					LOG_DEBUG("DOWNLOAD_UNCHANGED", "Chunk %s is unchanged", manager.config.GetChunkIDFromHash(hash))
				}
			} else {
				chunk := chunkDownloader.WaitForChunk(i)
				if chunk.isBroken {
					return false, fmt.Errorf("chunk %s is corrupted", manager.config.GetChunkIDFromHash(hash))
				}
				_, err = existingFile.Write(chunk.GetBytes()[start:end])
				if err != nil {
					LOG_ERROR("DOWNLOAD_WRITE", "Failed to write to the file: %v", err)
					return false, nil
				}
				hasher.Write(chunk.GetBytes()[start:end])
			}

			offset += int64(end - start)
		}

		// Must truncate the file if the new size is smaller
		if err = existingFile.Truncate(offset); err != nil {
			LOG_ERROR("DOWNLOAD_TRUNCATE", "Failed to truncate the file at %d: %v", offset, err)
			return false, nil
		}

		// Verify the download by hash
		hash := hex.EncodeToString(hasher.Sum(nil))
		if hash != entry.Hash && hash != "" && entry.Hash != "" && !strings.HasPrefix(entry.Hash, "#") {
			LOG_WERROR(allowFailures, "DOWNLOAD_HASH", "File %s has a mismatched hash: %s instead of %s (in-place)",
				fullPath, hash, entry.Hash)
			return false, fmt.Errorf("file corrupt (hash mismatch)")
		}

	} else {

		// Create the temporary file.
		newFile, err = os.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			LOG_ERROR("DOWNLOAD_OPEN", "Failed to open file for writing: %v", err)
			return false, nil
		}

		hasher := manager.config.NewFileHasher()

		var localChunk *Chunk
		defer chunkDownloader.operator.config.PutChunk(localChunk)

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
						localChunk = chunkDownloader.operator.config.GetChunk()
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
				if chunk.isBroken {
					return false, fmt.Errorf("chunk %s is corrupted", manager.config.GetChunkIDFromHash(hash))
				}
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
				return false, nil
			}

			hasher.Write(data)
			offset += int64(len(data))
		}

		hash := hex.EncodeToString(hasher.Sum(nil))
		if hash != entry.Hash && hash != "" && entry.Hash != "" && !strings.HasPrefix(entry.Hash, "#") {
			LOG_WERROR(allowFailures, "DOWNLOAD_HASH", "File %s has a mismatched hash: %s instead of %s",
				entry.Path, hash, entry.Hash)
			return false, fmt.Errorf("file corrupt (hash mismatch)")
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
			return false, nil
		}

		err = os.Rename(temporaryPath, fullPath)
		if err != nil {
			LOG_ERROR("DOWNLOAD_RENAME", "Failed to rename the file %s to %s: %v", temporaryPath, fullPath, err)
			return false, nil
		}
	}

	if !showStatistics {
		LOG_INFO("DOWNLOAD_DONE", "Downloaded %s (%d)", entry.Path, entry.Size)
	}
	return true, nil
}

// CopySnapshots copies the specified snapshots from one storage to the other.
func (manager *BackupManager) CopySnapshots(otherManager *BackupManager, snapshotID string,
	revisionsToBeCopied []int, uploadingThreads int, downloadingThreads int) bool {

	if !manager.config.IsCompatiableWith(otherManager.config) {
		LOG_ERROR("CONFIG_INCOMPATIBLE", "Two storages are not compatible for the copy operation")
		return false
	}

	if otherManager.config.DataShards != 0 && otherManager.config.ParityShards != 0 {
		LOG_INFO("BACKUP_ERASURECODING", "Erasure coding is enabled for the destination storage with %d data shards and %d parity shards",
		         otherManager.config.DataShards, otherManager.config.ParityShards)
	}

	if otherManager.config.rsaPublicKey != nil && len(otherManager.config.FileKey) > 0 {
		LOG_INFO("BACKUP_KEY", "RSA encryption is enabled for the destination")
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

	// These two maps store hashes of chunks in the source and destination storages, respectively.  Note that
	// the value of 'chunks' is used to indicate if the chunk is a snapshot chunk, while the value of 'otherChunks'
	// is not used.
	chunks := make(map[string]bool)
	otherChunks := make(map[string]bool)

	for _, snapshot := range snapshots {

		if revisionMap[snapshot.ID][snapshot.Revision] == false {
			continue
		}

		LOG_TRACE("SNAPSHOT_COPY", "Copying snapshot %s at revision %d", snapshot.ID, snapshot.Revision)

		for _, chunkHash := range snapshot.FileSequence {
			chunks[chunkHash] = true  // The chunk is a snapshot chunk
		}

		for _, chunkHash := range snapshot.ChunkSequence {
			chunks[chunkHash] = true  // The chunk is a snapshot chunk
		}

		for _, chunkHash := range snapshot.LengthSequence {
			chunks[chunkHash] = true  // The chunk is a snapshot chunk
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
				chunks[chunkHash] = false  // The chunk is a file chunk
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

	var chunksToCopy []string

	for chunkHash := range chunks {
		otherChunkID := otherManager.config.GetChunkIDFromHash(chunkHash)
		if _, found := otherChunks[otherChunkID]; !found {
			chunksToCopy = append(chunksToCopy, chunkHash)
		}
	}

	LOG_INFO("SNAPSHOT_COPY", "Chunks to copy: %d, to skip: %d, total: %d", len(chunksToCopy), len(chunks) - len(chunksToCopy), len(chunks))

	chunkDownloader := CreateChunkOperator(manager.config, manager.storage, nil, false, downloadingThreads, false)

	var uploadedBytes int64
	startTime := time.Now()

	copiedChunks := 0
	chunkUploader := CreateChunkOperator(otherManager.config, otherManager.storage, nil, false, uploadingThreads, false)
	chunkUploader.UploadCompletionFunc =  func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
		action := "Skipped"
		if !skipped {
			copiedChunks++
			action = "Copied"
		}

		atomic.AddInt64(&uploadedBytes, int64(chunkSize))

		elapsedTime := time.Now().Sub(startTime).Seconds()
		speed := int64(float64(atomic.LoadInt64(&uploadedBytes)) / elapsedTime)
		remainingTime := int64(float64(len(chunksToCopy) - chunkIndex - 1) / float64(chunkIndex + 1) * elapsedTime)
		percentage := float64(chunkIndex + 1) / float64(len(chunksToCopy)) * 100.0
		LOG_INFO("COPY_PROGRESS", "%s chunk %s (%d/%d) %sB/s %s %.1f%%",
				action, chunk.GetID(), chunkIndex + 1, len(chunksToCopy),
				PrettySize(speed), PrettyTime(remainingTime), percentage)
		otherManager.config.PutChunk(chunk)
	}

	for i, chunkHash := range chunksToCopy {
		chunkID := manager.config.GetChunkIDFromHash(chunkHash)
		newChunkID := otherManager.config.GetChunkIDFromHash(chunkHash)

		chunkDownloader.DownloadAsync(chunkHash, i, chunks[chunkHash], func(chunk *Chunk, chunkIndex int) {
			newChunk := otherManager.config.GetChunk()
			newChunk.Reset(true)
			newChunk.Write(chunk.GetBytes())
			newChunk.isMetadata = chunks[chunk.GetHash()]
			chunkUploader.Upload(newChunk, chunkIndex, newChunk.isMetadata)
			manager.config.PutChunk(chunk)
		})

		LOG_DEBUG("SNAPSHOT_COPY", "Copying chunk %s to %s", chunkID, newChunkID)
	}

	chunkDownloader.Stop()
	chunkUploader.Stop()

	LOG_INFO("SNAPSHOT_COPY", "Copied %d new chunks and skipped %d existing chunks", copiedChunks, len(chunks) - copiedChunks)

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
