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
	"io/ioutil"
	"math"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aryann/difflib"
)

const (
	secondsInDay = 86400
	chunkDir     = "chunks/"
)

// FossilCollection contains fossils and temporary files found during a snapshot deletions.
type FossilCollection struct {

	// At what time the fossil collection was finished
	EndTime int64 `json:"end_time"`

	// The lastest revision for each snapshot id when the fossil collection was created.
	LastRevisions map[string]int `json:"last_revisions"`

	// Record the set of snapshots that have been removed by the prune command that created this fossil collection
	DeletedRevisions map[string][]int `json:"deleted_revisions"`

	// Fossils (i.e., chunks not referenced by any snapshots)
	Fossils []string `json:"fossils"`

	// Temporary files.
	Temporaries []string `json:"temporaries"`
}

// CreateFossilCollection creates an empty fossil collection
func CreateFossilCollection(allSnapshots map[string][]*Snapshot) *FossilCollection {

	lastRevisions := make(map[string]int)
	for id, snapshots := range allSnapshots {
		lastRevisions[id] = snapshots[len(snapshots)-1].Revision
	}

	return &FossilCollection{
		LastRevisions:    lastRevisions,
		DeletedRevisions: make(map[string][]int),
	}
}

// IsDeletable determines if the previously collected fossils are safe to be permanently removed.  If so, it will
// also returns a number of snapshots that were created during or after these fossils were being collected.
// Therefore, some fossils may be referenced by these new snapshots and they must be resurrected.
func (collection *FossilCollection) IsDeletable(isStrongConsistent bool, ignoredIDs []string,
	allSnapshots map[string][]*Snapshot) (isDeletable bool, newSnapshots []*Snapshot) {

	hasNewSnapshot := make(map[string]bool)
	lastSnapshotTime := make(map[string]int64)
	for snapshotID, snapshotList := range allSnapshots {

		if len(snapshotList) == 0 {
			continue
		}

		ignored := false
		for _, ignoredID := range ignoredIDs {
			if snapshotID == ignoredID {
				ignored = true
			}
		}

		if ignored {
			LOG_INFO("SNAPSHOT_NOT_CONSIDERED", "Ignored snapshot %s", snapshotID)
			continue
		}

		lastRevision := collection.LastRevisions[snapshotID]

		// We want to handle snapshot ids such as 'repo@host' so that one new snapshot from that host means other
		// repositories on the same host are safe -- because presumably one host can do one backup at a time.
		hostID := snapshotID
		if strings.Contains(hostID, "@") {
			hostID = strings.SplitN(hostID, "@", 2)[1]
		}

		if _, found := hasNewSnapshot[hostID]; !found {
			hasNewSnapshot[hostID] = false
			lastSnapshotTime[hostID] = 0
		}

		for _, snapshot := range snapshotList {

			if snapshot.Revision <= lastRevision {
				// This is an old snapshot known by this fossil collection
				continue
			}

			newSnapshots = append(newSnapshots, snapshot)

			extraTime := 0
			if !isStrongConsistent {
				extraTime = secondsInDay / 2
			}

			// If this snapshot ends before this fossil collection, then it is still possible that another snapshot
			// might be in progress (although very unlikely).  So we only deem it deletable if that is not the case.
			if snapshot.EndTime > collection.EndTime+int64(extraTime) {
				hasNewSnapshot[hostID] = true
				break
			} else {
				LOG_TRACE("SNAPSHOT_UNDELETABLE",
					"New snapshot %s revision %d doesn't meet the fossil deletion criteria",
					snapshot.ID, snapshot.Revision)
			}
		}

		if !hasNewSnapshot[hostID] {
			LOG_TRACE("SNAPSHOT_NO_NEW", "No new snapshot from %s since the fossil collection step", snapshotID)
		}

		lastSnapshot := allSnapshots[snapshotID][len(allSnapshots[snapshotID])-1]
		if lastSnapshot.EndTime > lastSnapshotTime[hostID] {
			lastSnapshotTime[hostID] = lastSnapshot.EndTime
		}
	}

	maxSnapshotRunningTime := int64(7)

	for hostID, value := range hasNewSnapshot {
		if value == false {
			// In case of a dormant repository, a fossil collection is safe if no new snapshot has been seen for a
			// snapshot id during the last 7 days.  A snapshot created at the roughly same time as this fossil
			// collection would have finsihed already, while a snapshot currently being created does not affect
			// this fossil collection.
			if lastSnapshotTime[hostID] > 0 && lastSnapshotTime[hostID] < time.Now().Unix()-maxSnapshotRunningTime*secondsInDay {
				LOG_INFO("SNAPSHOT_INACTIVE", "Ignore snapshot %s whose last revision was created %d days ago",
					hostID, maxSnapshotRunningTime)
				continue
			}
			return false, nil
		}
	}

	return true, newSnapshots
}

func (collection *FossilCollection) AddFossil(hash string) {
	collection.Fossils = append(collection.Fossils, hash)
}

func (collection *FossilCollection) AddTemporary(temporary string) {
	collection.Temporaries = append(collection.Temporaries, temporary)
}

func (collection *FossilCollection) IsEmpty() bool {
	return len(collection.Fossils) == 0 && len(collection.Temporaries) == 0
}

// Calculates the number of days between two times ignoring the hours, minutes and seconds.
func getDaysBetween(start int64, end int64) int {
	startTime := time.Unix(start, 0).In(time.Now().Location())
	endTime := time.Unix(end, 0).In(time.Now().Location())
	startDate := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())
	endDate := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, endTime.Location())
	hours := int(endDate.Sub(startDate).Hours())
	return (hours + 1) / 24
}

// SnapshotManager is mainly responsible for downloading, and deleting snapshots.
type SnapshotManager struct {

	// These are variables shared with the backup manager
	config        *Config
	storage       Storage
	fileChunk     *Chunk
	snapshotCache *FileStorage

	chunkDownloader *ChunkDownloader
	chunkOperator   *ChunkOperator
}

// CreateSnapshotManager creates a snapshot manager
func CreateSnapshotManager(config *Config, storage Storage) *SnapshotManager {

	manager := &SnapshotManager{
		config:    config,
		storage:   storage,
		fileChunk: CreateChunk(config, true),
	}

	return manager
}

// DownloadSnapshot downloads the specified snapshot.
func (manager *SnapshotManager) DownloadSnapshot(snapshotID string, revision int) *Snapshot {

	snapshotDir := fmt.Sprintf("snapshots/%s", snapshotID)
	manager.storage.CreateDirectory(0, snapshotDir)
	manager.snapshotCache.CreateDirectory(0, snapshotDir)

	snapshotPath := fmt.Sprintf("snapshots/%s/%d", snapshotID, revision)

	// We must check if the snapshot file exists in the storage, because the snapshot cache may store a copy of the
	// file even if the snapshot has been deleted in the storage (possibly by a different client)
	exist, _, _, err := manager.storage.GetFileInfo(0, snapshotPath)
	if err != nil {
		LOG_ERROR("SNAPSHOT_INFO", "Failed to get the information on the snapshot %s at revision %d: %v",
			snapshotID, revision, err)
		return nil
	}

	if !exist {
		LOG_ERROR("SNAPSHOT_NOT_EXIST", "Snapshot %s at revision %d does not exist", snapshotID, revision)
		return nil

	}

	description := manager.DownloadFile(snapshotPath, snapshotPath)

	snapshot, err := CreateSnapshotFromDescription(description)

	if err != nil {
		LOG_ERROR("SNAPSHOT_PARSE", "Failed to parse the snapshot %s at revision %d: %v", snapshotID, revision, err)
		return nil
	}

	// Overwrite the snapshot ID; this allows snapshot dirs to be renamed freely
	snapshot.ID = snapshotID

	return snapshot
}

// sequenceReader loads the chunks pointed to by 'sequence' one by one as needed.  This avoid loading all chunks into
// the memory before passing them to the json unmarshaller.
type sequenceReader struct {
	sequence   []string
	buffer     *bytes.Buffer
	index      int
	refillFunc func(hash string) []byte
}

// Read reads a new chunk using the refill function when there is no more data in the buffer
func (reader *sequenceReader) Read(data []byte) (n int, err error) {
	if len(reader.buffer.Bytes()) == 0 {
		if reader.index < len(reader.sequence) {
			newData := reader.refillFunc(reader.sequence[reader.index])
			reader.buffer.Write(newData)
			reader.index++
		} else {
			return 0, io.EOF
		}
	}

	return reader.buffer.Read(data)
}

func (manager *SnapshotManager) CreateChunkDownloader() {
	if manager.chunkDownloader == nil {
		manager.chunkDownloader = CreateChunkDownloader(manager.config, manager.storage, manager.snapshotCache, false, 1)
	}
}

// DownloadSequence returns the content represented by a sequence of chunks.
func (manager *SnapshotManager) DownloadSequence(sequence []string) (content []byte) {
	manager.CreateChunkDownloader()
	for _, chunkHash := range sequence {
		i := manager.chunkDownloader.AddChunk(chunkHash)
		chunk := manager.chunkDownloader.WaitForChunk(i)
		content = append(content, chunk.GetBytes()...)
	}

	return content
}

func (manager *SnapshotManager) DownloadSnapshotFileSequence(snapshot *Snapshot, patterns []string, attributesNeeded bool) bool {

	manager.CreateChunkDownloader()

	reader := sequenceReader{
		sequence: snapshot.FileSequence,
		buffer:   new(bytes.Buffer),
		refillFunc: func(chunkHash string) []byte {
			i := manager.chunkDownloader.AddChunk(chunkHash)
			chunk := manager.chunkDownloader.WaitForChunk(i)
			return chunk.GetBytes()
		},
	}

	files := make([]*Entry, 0)
	decoder := json.NewDecoder(&reader)

	// read open bracket
	_, err := decoder.Token()
	if err != nil {
		LOG_ERROR("SNAPSHOT_PARSE", "Failed to load files specified in the snapshot %s at revision %d: not a list of entries",
			snapshot.ID, snapshot.Revision)
		return false
	}

	// while the array contains values
	for decoder.More() {
		var entry Entry
		err = decoder.Decode(&entry)
		if err != nil {
			LOG_ERROR("SNAPSHOT_PARSE", "Failed to load files specified in the snapshot %s at revision %d: %v",
				snapshot.ID, snapshot.Revision, err)
			return false
		}

		// If we don't need the attributes or the file isn't included we clear the attributes to save memory
		if !attributesNeeded || (len(patterns) != 0 && !MatchPath(entry.Path, patterns)) {
			entry.Attributes = nil
		}

		files = append(files, &entry)
	}
	snapshot.Files = files
	return true
}

// DownloadSnapshotSequence downloads the content represented by a sequence of chunks, and then unmarshal the content
// using the specified 'loadFunction'.  It purpose is to decode the chunk sequences representing chunk hashes or chunk lengths
// in a snapshot.
func (manager *SnapshotManager) DownloadSnapshotSequence(snapshot *Snapshot, sequenceType string) bool {

	sequence := snapshot.ChunkSequence
	loadFunc := snapshot.LoadChunks

	if sequenceType == "lengths" {
		sequence = snapshot.LengthSequence
		loadFunc = snapshot.LoadLengths
	}

	content := manager.DownloadSequence(sequence)

	if len(content) == 0 {
		LOG_ERROR("SNAPSHOT_PARSE", "Failed to load %s specified in the snapshot %s at revision %d",
			sequenceType, snapshot.ID, snapshot.Revision)
		return false
	}

	err := loadFunc(content)
	if err != nil {
		LOG_ERROR("SNAPSHOT_PARSE", "Failed to load %s specified in the snapshot %s at revision %d: %v",
			sequenceType, snapshot.ID, snapshot.Revision, err)
		return false
	}
	return true
}

// DownloadSnapshotContents loads all chunk sequences in a snapshot.  A snapshot, when just created, only contains
// some metadata and theree sequence representing files, chunk hashes, and chunk lengths.  This function must be called
// for the actual content of the snapshot to be usable.
func (manager *SnapshotManager) DownloadSnapshotContents(snapshot *Snapshot, patterns []string, attributesNeeded bool) bool {

	manager.DownloadSnapshotFileSequence(snapshot, patterns, attributesNeeded)
	manager.DownloadSnapshotSequence(snapshot, "chunks")
	manager.DownloadSnapshotSequence(snapshot, "lengths")

	err := manager.CheckSnapshot(snapshot)
	if err != nil {
		LOG_ERROR("SNAPSHOT_CHECK", "The snapshot %s at revision %d contains an error: %v",
			snapshot.ID, snapshot.Revision, err)
		return false
	}

	return true
}

// CleanSnapshotCache removes all files not referenced by the specified 'snapshot' in the snapshot cache.
func (manager *SnapshotManager) CleanSnapshotCache(latestSnapshot *Snapshot, allSnapshots map[string][]*Snapshot) bool {

	if allSnapshots == nil {
		// If the 'fossils' directory exists then don't clean the cache as all snapshots will be needed later
		// during the fossil collection phase.  The deletion procedure creates this directory.
		// We only check this condition when allSnapshots is nil because
		// in thise case it is the deletion procedure that is trying to clean the snapshot cache.
		exist, _, _, err := manager.snapshotCache.GetFileInfo(0, "fossils")

		if err != nil {
			LOG_ERROR("SNAPSHOT_CLEAN", "Failed to list the snapshot cache: %v", err)
			return false
		}

		if exist {
			return true
		}
	}

	// This stores all chunks we want to keep
	chunks := make(map[string]bool)

	if latestSnapshot != nil {
		for _, chunkID := range manager.GetSnapshotChunks(latestSnapshot, true) {
			chunks[chunkID] = true
		}
	}

	allSnapshotFiles := make(map[string]bool)

	for snapshotID, snapshotList := range allSnapshots {
		for _, snapshot := range snapshotList {
			allSnapshotFiles[fmt.Sprintf("%s/%d", snapshotID, snapshot.Revision)] = false
		}
	}

	if latestSnapshot != nil {
		allSnapshotFiles[fmt.Sprintf("%s/%d", latestSnapshot.ID, latestSnapshot.Revision)] = false
	}

	allCachedSnapshots, _ := manager.ListAllFiles(manager.snapshotCache, "snapshots/")
	for _, snapshotFile := range allCachedSnapshots {
		if snapshotFile[len(snapshotFile)-1] == '/' {
			continue
		}

		if _, found := allSnapshotFiles[snapshotFile]; !found {
			LOG_DEBUG("SNAPSHOT_CLEAN", "Delete cached snapshot file %s not found in the storage", snapshotFile)
			manager.snapshotCache.DeleteFile(0, path.Join("snapshots", snapshotFile))
			continue
		}

		description, err := ioutil.ReadFile(path.Join(manager.snapshotCache.storageDir,
			"snapshots", snapshotFile))
		if err != nil {
			LOG_WARN("SNAPSHOT_CACHE", "Failed to read the cached snapshot file: %v", err)
			continue
		}

		cachedSnapshot, err := CreateSnapshotFromDescription(description)

		if err != nil {
			LOG_ERROR("SNAPSHOT_CACHE", "Failed to parse the cached snapshot file %s: %v", snapshotFile, err)
			continue
		}

		isComplete := true
		for _, chunkHash := range cachedSnapshot.ChunkSequence {
			chunkID := manager.config.GetChunkIDFromHash(chunkHash)

			if _, exist, _, _ := manager.snapshotCache.FindChunk(0, chunkID, false); !exist {
				if _, exist, _, _ = manager.storage.FindChunk(0, chunkID, false); !exist {
					isComplete = false
					break
				}
			}
		}

		if !isComplete {
			LOG_DEBUG("SNAPSHOT_CLEAN", "Delete cached snapshot file %s with nonexistent chunks", snapshotFile)
			manager.snapshotCache.DeleteFile(0, path.Join("snapshots", snapshotFile))
			continue
		}

		for _, chunkHash := range cachedSnapshot.ChunkSequence {
			chunkID := manager.config.GetChunkIDFromHash(chunkHash)
			LOG_DEBUG("SNAPSHOT_CLEAN", "Snapshot %s revision %d needs chunk %s", cachedSnapshot.ID, cachedSnapshot.Revision, chunkID)
			chunks[chunkID] = true
		}
	}

	allFiles, _ := manager.ListAllFiles(manager.snapshotCache, chunkDir)
	for _, file := range allFiles {
		if file[len(file)-1] != '/' {
			chunkID := strings.Replace(file, "/", "", -1)
			if _, found := chunks[chunkID]; !found {
				LOG_DEBUG("SNAPSHOT_CLEAN", "Delete chunk %s from the snapshot cache", chunkID)
				err := manager.snapshotCache.DeleteFile(0, path.Join("chunks", file))
				if err != nil {
					LOG_WARN("SNAPSHOT_CLEAN", "Failed to remove the chunk %s from the snapshot cache: %v",
						file, err)
				}
			}
		}
	}

	return true

}

// ListSnapshotIDs returns all snapshot ids.
func (manager *SnapshotManager) ListSnapshotIDs() (snapshotIDs []string, err error) {

	LOG_TRACE("SNAPSHOT_LIST_IDS", "Listing all snapshot ids")

	dirs, _, err := manager.storage.ListFiles(0, "snapshots/")
	if err != nil {
		return nil, err
	}

	for _, dir := range dirs {
		if len(dir) > 0 && dir[len(dir)-1] == '/' {
			snapshotIDs = append(snapshotIDs, dir[:len(dir)-1])
		}
	}

	return snapshotIDs, nil
}

// ListSnapshotRevisions returns the list of all revisions given a snapshot id.
func (manager *SnapshotManager) ListSnapshotRevisions(snapshotID string) (revisions []int, err error) {

	LOG_TRACE("SNAPSHOT_LIST_REVISIONS", "Listing revisions for snapshot %s", snapshotID)

	snapshotDir := fmt.Sprintf("snapshots/%s/", snapshotID)

	err = manager.storage.CreateDirectory(0, snapshotDir)
	if err != nil {
		return nil, err
	}

	err = manager.snapshotCache.CreateDirectory(0, snapshotDir)
	if err != nil {
		LOG_WARN("SNAPSHOT_CACHE_DIR", "Failed to create the snapshot cache directory %s: %v", snapshotDir, err)
	}

	files, _, err := manager.storage.ListFiles(0, snapshotDir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if len(file) > 0 && file[len(file)-1] != '/' {
			revision, err := strconv.Atoi(file)
			if err == nil {
				revisions = append(revisions, revision)
			}
		}
	}

	sort.Ints(revisions)

	return revisions, nil
}

// DownloadLatestSnapshot downloads the snapshot with the largest revision number.
func (manager *SnapshotManager) downloadLatestSnapshot(snapshotID string) (remote *Snapshot) {

	LOG_TRACE("SNAPSHOT_DOWNLOAD_LATEST", "Downloading latest revision for snapshot %s", snapshotID)

	revisions, err := manager.ListSnapshotRevisions(snapshotID)

	if err != nil {
		LOG_ERROR("SNAPSHOT_LIST", "Failed to list the revisions of the snapshot %s: %v", snapshotID, err)
		return nil
	}

	latest := 0
	for _, revision := range revisions {
		if revision > latest {
			latest = revision
		}
	}

	if latest > 0 {
		remote = manager.DownloadSnapshot(snapshotID, latest)
	}

	if remote != nil {
		manager.DownloadSnapshotContents(remote, nil, false)
	}

	return remote
}

// ListAllFiles return all files and subdirectories in the subtree of the 'top' directory in the specified 'storage'.
func (manager *SnapshotManager) ListAllFiles(storage Storage, top string) (allFiles []string, allSizes []int64) {

	directories := make([]string, 0, 1024)

	directories = append(directories, top)

	for len(directories) > 0 {

		dir := directories[len(directories)-1]
		directories = directories[:len(directories)-1]

		LOG_TRACE("LIST_FILES", "Listing %s", dir)

		files, sizes, err := storage.ListFiles(0, dir)
		if err != nil {
			LOG_ERROR("LIST_FILES", "Failed to list the directory %s: %v", dir, err)
			return nil, nil
		}

		if len(dir) > len(top) {
			allFiles = append(allFiles, dir[len(top):])
			allSizes = append(allSizes, 0)
		}

		for i, file := range files {
			if len(file) > 0 && file[len(file)-1] == '/' {
				directories = append(directories, dir+file)
			} else {
				allFiles = append(allFiles, (dir + file)[len(top):])
				allSizes = append(allSizes, sizes[i])
			}
		}
	}

	return allFiles, allSizes
}

// GetSnapshotChunks returns all chunks referenced by a given snapshot. If
// keepChunkHashes is true, snapshot.ChunkHashes will be populated.
func (manager *SnapshotManager) GetSnapshotChunks(snapshot *Snapshot, keepChunkHashes bool) (chunks []string) {

	for _, chunkHash := range snapshot.FileSequence {
		chunks = append(chunks, manager.config.GetChunkIDFromHash(chunkHash))
	}

	for _, chunkHash := range snapshot.ChunkSequence {
		chunks = append(chunks, manager.config.GetChunkIDFromHash(chunkHash))
	}

	for _, chunkHash := range snapshot.LengthSequence {
		chunks = append(chunks, manager.config.GetChunkIDFromHash(chunkHash))
	}

	if len(snapshot.ChunkHashes) == 0 {

		description := manager.DownloadSequence(snapshot.ChunkSequence)
		err := snapshot.LoadChunks(description)
		if err != nil {
			LOG_ERROR("SNAPSHOT_CHUNK", "Failed to load chunks for snapshot %s at revision %d: %v",
				snapshot.ID, snapshot.Revision, err)
			return nil
		}
	}

	for _, chunkHash := range snapshot.ChunkHashes {
		chunks = append(chunks, manager.config.GetChunkIDFromHash(chunkHash))
	}

	if !keepChunkHashes {
		snapshot.ClearChunks()
	}

	return chunks
}

// ListSnapshots shows the information about a snapshot.
func (manager *SnapshotManager) ListSnapshots(snapshotID string, revisionsToList []int, tag string,
	showFiles bool, showChunks bool) int {

	LOG_DEBUG("LIST_PARAMETERS", "id: %s, revisions: %v, tag: %s, showFiles: %t, showChunks: %t",
		snapshotID, revisionsToList, tag, showFiles, showChunks)

	var snapshotIDs []string
	var err error

	if snapshotID == "" {
		snapshotIDs, err = manager.ListSnapshotIDs()
		if err != nil {
			LOG_ERROR("SNAPSHOT_LIST", "Failed to list all snapshots: %v", err)
			return 0
		}
	} else {
		snapshotIDs = []string{snapshotID}
	}

	numberOfSnapshots := 0

	for _, snapshotID = range snapshotIDs {

		revisions := revisionsToList
		if len(revisions) == 0 {
			revisions, err = manager.ListSnapshotRevisions(snapshotID)
			if err != nil {
				LOG_ERROR("SNAPSHOT_LIST", "Failed to list all revisions for snapshot %s: %v", snapshotID, err)
				return 0
			}
		}

		for _, revision := range revisions {

			snapshot := manager.DownloadSnapshot(snapshotID, revision)
			if tag != "" && snapshot.Tag != tag {
				continue
			}
			creationTime := time.Unix(snapshot.StartTime, 0).Format("2006-01-02 15:04")
			tagWithSpace := ""
			if len(snapshot.Tag) > 0 {
				tagWithSpace = snapshot.Tag + " "
			}
			LOG_INFO("SNAPSHOT_INFO", "Snapshot %s revision %d created at %s %s%s",
				snapshotID, revision, creationTime, tagWithSpace, snapshot.Options)

			if showFiles {
				manager.DownloadSnapshotFileSequence(snapshot, nil, false)
			}

			if showFiles {

				if snapshot.NumberOfFiles > 0 {
					LOG_INFO("SNAPSHOT_STATS", "Files: %d", snapshot.NumberOfFiles)
				}

				maxSize := int64(9)
				maxSizeDigits := 1
				totalFiles := 0
				totalFileSize := int64(0)
				lastChunk := 0

				for _, file := range snapshot.Files {
					if file.IsFile() {
						totalFiles++
						totalFileSize += file.Size
						if file.Size > maxSize {
							maxSize = maxSize*10 + 9
							maxSizeDigits += 1
						}
						if file.EndChunk > lastChunk {
							lastChunk = file.EndChunk
						}
					}
				}

				for _, file := range snapshot.Files {
					if file.IsFile() {
						LOG_INFO("SNAPSHOT_FILE", "%s", file.String(maxSizeDigits))
					}
				}

				metaChunks := len(snapshot.FileSequence) + len(snapshot.ChunkSequence) + len(snapshot.LengthSequence)
				LOG_INFO("SNAPSHOT_STATS", "Files: %d, total size: %d, file chunks: %d, metadata chunks: %d",
					totalFiles, totalFileSize, lastChunk+1, metaChunks)
			}

			if showChunks {
				for _, chunkID := range manager.GetSnapshotChunks(snapshot, false) {
					LOG_INFO("SNAPSHOT_CHUNKS", "chunk: %s", chunkID)
				}
			}

			numberOfSnapshots++
		}
	}

	return numberOfSnapshots

}

// ListSnapshots shows the information about a snapshot.
func (manager *SnapshotManager) CheckSnapshots(snapshotID string, revisionsToCheck []int, tag string, showStatistics bool, showTabular bool,
	checkFiles bool, searchFossils bool, resurrect bool) bool {

	LOG_DEBUG("LIST_PARAMETERS", "id: %s, revisions: %v, tag: %s, showStatistics: %t, checkFiles: %t, searchFossils: %t, resurrect: %t",
		snapshotID, revisionsToCheck, tag, showStatistics, checkFiles, searchFossils, resurrect)

	snapshotMap := make(map[string][]*Snapshot)
	var err error

	// Stores the chunk file size for each chunk
	chunkSizeMap := make(map[string]int64)

	// Indicate whether or not a chunk is shared by multiple snapshots
	chunkUniqueMap := make(map[string]bool)

	// Store the index of the snapshot that references each chunk; if the chunk is shared by multiple chunks, the index is -1
	chunkSnapshotMap := make(map[string]int)

	LOG_INFO("SNAPSHOT_CHECK", "Listing all chunks")
	allChunks, allSizes := manager.ListAllFiles(manager.storage, chunkDir)

	for i, chunk := range allChunks {
		if len(chunk) == 0 || chunk[len(chunk)-1] == '/' {
			continue
		}

		if strings.HasSuffix(chunk, ".fsl") {
			continue
		}

		chunk = strings.Replace(chunk, "/", "", -1)
		chunkSizeMap[chunk] = allSizes[i]
	}

	if snapshotID == "" || showStatistics {
		snapshotIDs, err := manager.ListSnapshotIDs()
		if err != nil {
			LOG_ERROR("SNAPSHOT_LIST", "Failed to list all snapshots: %v", err)
			return false
		}

		for _, snapshotID := range snapshotIDs {
			snapshotMap[snapshotID] = nil
		}

	} else {
		snapshotMap[snapshotID] = nil
	}

	snapshotIDIndex := 0
	totalMissingChunks := 0
	for snapshotID = range snapshotMap {

		revisions := revisionsToCheck
		if len(revisions) == 0 || showStatistics {
			revisions, err = manager.ListSnapshotRevisions(snapshotID)
			if err != nil {
				LOG_ERROR("SNAPSHOT_LIST", "Failed to list all revisions for snapshot %s: %v", snapshotID, err)
				return false
			}
		}

		for _, revision := range revisions {
			snapshot := manager.DownloadSnapshot(snapshotID, revision)
			if tag != "" && snapshot.Tag != tag {
				continue
			}
			snapshotMap[snapshotID] = append(snapshotMap[snapshotID], snapshot)
		}
	}

	totalRevisions := 0
	for _, snapshotList := range snapshotMap {
		totalRevisions += len(snapshotList)
	}
	LOG_INFO("SNAPSHOT_CHECK", "%d snapshots and %d revisions", len(snapshotMap), totalRevisions)

	var totalChunkSize int64
	for _, size := range chunkSizeMap {
		totalChunkSize += size
	}
	LOG_INFO("SNAPSHOT_CHECK", "Total chunk size is %s in %d chunks", PrettyNumber(totalChunkSize), len(chunkSizeMap))

	for snapshotID = range snapshotMap {

		for _, snapshot := range snapshotMap[snapshotID] {

			if checkFiles {
				manager.DownloadSnapshotContents(snapshot, nil, false)
				manager.VerifySnapshot(snapshot)
				continue
			}

			chunks := make(map[string]bool)
			for _, chunkID := range manager.GetSnapshotChunks(snapshot, false) {
				chunks[chunkID] = true
			}

			missingChunks := 0
			for chunkID := range chunks {

				_, found := chunkSizeMap[chunkID]

				if !found {

					// Look up the chunk again in case it actually exists, but only if there aren't
					// too many missing chunks.
					if missingChunks < 100 {
						_, exist, _, err := manager.storage.FindChunk(0, chunkID, false)
						if err != nil {
							LOG_WARN("SNAPSHOT_VALIDATE", "Failed to check the existence of chunk %s: %v",
							         chunkID, err)
						} else if exist {
							LOG_INFO("SNAPSHOT_VALIDATE", "Chunk %s is confirmed to exist", chunkID)
							continue
						}
					}

					if !searchFossils {
						missingChunks += 1
						LOG_WARN("SNAPSHOT_VALIDATE",
							"Chunk %s referenced by snapshot %s at revision %d does not exist",
							chunkID, snapshotID, snapshot.Revision)
						continue
					}

					chunkPath, exist, size, err := manager.storage.FindChunk(0, chunkID, true)
					if err != nil {
						LOG_ERROR("SNAPSHOT_VALIDATE", "Failed to check the existence of fossil %s: %v",
							chunkID, err)
						return false
					}

					if !exist {
						missingChunks += 1
						LOG_WARN("SNAPSHOT_VALIDATE",
							"Chunk %s referenced by snapshot %s at revision %d does not exist",
							chunkID, snapshotID, snapshot.Revision)
						continue
					}

					if resurrect {
						manager.resurrectChunk(chunkPath, chunkID)
					} else {
						LOG_WARN("SNAPSHOT_FOSSIL", "Chunk %s referenced by snapshot %s at revision %d "+
							"has been marked as a fossil", chunkID, snapshotID, snapshot.Revision)
					}

					chunkSizeMap[chunkID] = size
				}

				if unique, found := chunkUniqueMap[chunkID]; !found {
					chunkUniqueMap[chunkID] = true
				} else {
					if unique {
						chunkUniqueMap[chunkID] = false
					}
				}

				if previousSnapshotIDIndex, found := chunkSnapshotMap[chunkID]; !found {
					chunkSnapshotMap[chunkID] = snapshotIDIndex
				} else if previousSnapshotIDIndex != snapshotIDIndex && previousSnapshotIDIndex != -1 {
					chunkSnapshotMap[chunkID] = -1
				}
			}

			if missingChunks > 0 {
				LOG_WARN("SNAPSHOT_CHECK", "Some chunks referenced by snapshot %s at revision %d are missing",
					snapshotID, snapshot.Revision)
				totalMissingChunks += missingChunks
			} else {
				LOG_INFO("SNAPSHOT_CHECK", "All chunks referenced by snapshot %s at revision %d exist",
					snapshotID, snapshot.Revision)
			}
		}

		snapshotIDIndex += 1
	}

	if totalMissingChunks > 0 {
		LOG_ERROR("SNAPSHOT_CHECK", "Some chunks referenced by some snapshots do not exist in the storage")
		return false
	}

	if showTabular {
		manager.ShowStatisticsTabular(snapshotMap, chunkSizeMap, chunkUniqueMap, chunkSnapshotMap)
	} else if showStatistics {
		manager.ShowStatistics(snapshotMap, chunkSizeMap, chunkUniqueMap, chunkSnapshotMap)
	}

	return true
}

// Print snapshot and revision statistics
func (manager *SnapshotManager) ShowStatistics(snapshotMap map[string][]*Snapshot, chunkSizeMap map[string]int64, chunkUniqueMap map[string]bool,
	chunkSnapshotMap map[string]int) {
	for snapshotID, snapshotList := range snapshotMap {

		snapshotChunks := make(map[string]bool)

		for _, snapshot := range snapshotList {

			chunks := make(map[string]bool)
			for _, chunkID := range manager.GetSnapshotChunks(snapshot, false) {
				chunks[chunkID] = true
				snapshotChunks[chunkID] = true
			}

			var totalChunkSize int64
			var uniqueChunkSize int64

			for chunkID := range chunks {
				chunkSize := chunkSizeMap[chunkID]
				totalChunkSize += chunkSize
				if chunkUniqueMap[chunkID] {
					uniqueChunkSize += chunkSize
				}
			}

			files := ""
			if snapshot.FileSize != 0 && snapshot.NumberOfFiles != 0 {
				files = fmt.Sprintf("%d files (%s bytes), ", snapshot.NumberOfFiles, PrettyNumber(snapshot.FileSize))
			}
			LOG_INFO("SNAPSHOT_CHECK", "Snapshot %s at revision %d: %s%s total chunk bytes, %s unique chunk bytes",
				snapshot.ID, snapshot.Revision, files, PrettyNumber(totalChunkSize), PrettyNumber(uniqueChunkSize))
		}

		var totalChunkSize int64
		var uniqueChunkSize int64
		for chunkID := range snapshotChunks {
			chunkSize := chunkSizeMap[chunkID]
			totalChunkSize += chunkSize

			if chunkSnapshotMap[chunkID] != -1 {
				uniqueChunkSize += chunkSize
			}
		}
		LOG_INFO("SNAPSHOT_CHECK", "Snapshot %s all revisions: %s total chunk bytes, %s unique chunk bytes",
			snapshotID, PrettyNumber(totalChunkSize), PrettyNumber(uniqueChunkSize))
	}
}

// Print snapshot and revision statistics in tabular format
func (manager *SnapshotManager) ShowStatisticsTabular(snapshotMap map[string][]*Snapshot, chunkSizeMap map[string]int64, chunkUniqueMap map[string]bool,
	chunkSnapshotMap map[string]int) {
	tableBuffer := new(bytes.Buffer)
	tableWriter := tabwriter.NewWriter(tableBuffer, 0, 0, 1, ' ', tabwriter.AlignRight|tabwriter.Debug)

	for snapshotID, snapshotList := range snapshotMap {
		fmt.Fprintln(tableWriter, "")
		fmt.Fprintln(tableWriter, " snap \trev \t \tfiles \tbytes \tchunks \tbytes \tuniq \tbytes \tnew \tbytes \t")
		snapshotChunks := make(map[string]bool)

		earliestSeenChunks := make(map[string]int)

		for _, snapshot := range snapshotList {
			for _, chunkID := range manager.GetSnapshotChunks(snapshot, false) {
				if earliestSeenChunks[chunkID] == 0 {
					earliestSeenChunks[chunkID] = math.MaxInt32
				}
				if earliestSeenChunks[chunkID] > snapshot.Revision {
					earliestSeenChunks[chunkID] = snapshot.Revision
				}
			}
		}

		for _, snapshot := range snapshotList {

			chunks := make(map[string]bool)
			for _, chunkID := range manager.GetSnapshotChunks(snapshot, false) {
				chunks[chunkID] = true
				snapshotChunks[chunkID] = true
			}

			var totalChunkSize int64
			var uniqueChunkSize int64
			var totalChunkCount int64
			var uniqueChunkCount int64
			var newChunkCount int64
			var newChunkSize int64

			for chunkID := range chunks {
				chunkSize := chunkSizeMap[chunkID]
				totalChunkSize += chunkSize
				totalChunkCount += 1
				if earliestSeenChunks[chunkID] == snapshot.Revision {
					newChunkCount += 1
					newChunkSize += chunkSize
				}
				if chunkUniqueMap[chunkID] {
					uniqueChunkSize += chunkSize
					uniqueChunkCount += 1
				}
			}

			files := " \t "
			if snapshot.FileSize != 0 && snapshot.NumberOfFiles != 0 {
				files = fmt.Sprintf("%d \t%s", snapshot.NumberOfFiles, PrettyNumber(snapshot.FileSize))
			}
			creationTime := time.Unix(snapshot.StartTime, 0).Format("2006-01-02 15:04")
			fmt.Fprintln(tableWriter, fmt.Sprintf(
				"%s \t%d \t@ %s %5s \t%s \t%d \t%s \t%d \t%s \t%d \t%s \t",
				snapshotID, snapshot.Revision, creationTime, snapshot.Options, files, totalChunkCount, PrettyNumber(totalChunkSize), uniqueChunkCount, PrettyNumber(uniqueChunkSize), newChunkCount, PrettyNumber(newChunkSize)))
		}

		var totalChunkSize int64
		var uniqueChunkSize int64
		var totalChunkCount int64
		var uniqueChunkCount int64
		for chunkID := range snapshotChunks {
			chunkSize := chunkSizeMap[chunkID]
			totalChunkSize += chunkSize
			totalChunkCount += 1

			if chunkSnapshotMap[chunkID] != -1 {
				uniqueChunkSize += chunkSize
				uniqueChunkCount += 1
			}
		}
		fmt.Fprintln(tableWriter, fmt.Sprintf(
			"%s \tall \t \t \t \t%d \t%s \t%d \t%s \t \t \t",
			snapshotID, totalChunkCount, PrettyNumber(totalChunkSize), uniqueChunkCount, PrettyNumber(uniqueChunkSize)))
	}
	tableWriter.Flush()
	LOG_INFO("SNAPSHOT_CHECK", tableBuffer.String())
}

// ConvertSequence converts a sequence of chunk hashes into a sequence of chunk ids.
func (manager *SnapshotManager) ConvertSequence(sequence []string) (result []string) {
	result = make([]string, len(sequence))
	for i, hash := range sequence {
		result[i] = manager.config.GetChunkIDFromHash(hash)
	}
	return result
}

// PrintSnapshot prints the snapshot in the json format (with chunk hasher converted into chunk ids)
func (manager *SnapshotManager) PrintSnapshot(snapshot *Snapshot) bool {

	object := make(map[string]interface{})

	object["id"] = snapshot.ID
	object["revision"] = snapshot.Revision
	object["tag"] = snapshot.Tag
	object["start_time"] = snapshot.StartTime
	object["end_time"] = snapshot.EndTime

	object["file_sequence"] = manager.ConvertSequence(snapshot.FileSequence)
	object["chunk_sequence"] = manager.ConvertSequence(snapshot.ChunkSequence)
	object["length_sequence"] = manager.ConvertSequence(snapshot.LengthSequence)

	object["chunks"] = manager.ConvertSequence(snapshot.ChunkHashes)
	object["lengths"] = snapshot.ChunkLengths

	// By default the json serialization of a file entry contains the path in base64 format.  This is
	// to convert every file entry into an object which include the path in a more readable format.
	var files []map[string]interface{}
	for _, file := range snapshot.Files {
		files = append(files, file.convertToObject(false))
	}
	object["files"] = files

	description, err := json.MarshalIndent(object, "", "  ")

	if err != nil {
		LOG_ERROR("SNAPSHOT_PRINT", "Failed to marshal the snapshot %s at revision %d: %v",
			snapshot.ID, snapshot.Revision, err)
		return false
	}

	fmt.Printf("%s\n", string(description))

	return true
}

// VerifySnapshot verifies that every file in the snapshot has the correct hash.  It does this by downloading chunks
// and computing the whole file hash for each file.
func (manager *SnapshotManager) VerifySnapshot(snapshot *Snapshot) bool {

	err := manager.CheckSnapshot(snapshot)

	if err != nil {
		LOG_ERROR("SNAPSHOT_CHECK", "Snapshot %s at revision %d has an error: %v",
			snapshot.ID, snapshot.Revision, err)
		return false
	}

	files := make([]*Entry, 0, len(snapshot.Files)/2)
	for _, file := range snapshot.Files {
		if file.IsFile() && file.Size != 0 {
			files = append(files, file)
		}
	}

	sort.Sort(ByChunk(files))
	corruptedFiles := 0
	for _, file := range files {
		if !manager.RetrieveFile(snapshot, file, func([]byte) {}) {
			corruptedFiles++
		}
		LOG_TRACE("SNAPSHOT_VERIFY", "%s", file.Path)
	}

	if corruptedFiles > 0 {
		LOG_WARN("SNAPSHOT_VERIFY", "Snapshot %s at revision %d contains %d corrupted files",
			snapshot.ID, snapshot.Revision, corruptedFiles)
		return false
	} else {
		LOG_INFO("SNAPSHOT_VERIFY", "All files in snapshot %s at revision %d have been successfully verified",
			snapshot.ID, snapshot.Revision)
		return true
	}
}

// RetrieveFile retrieves the file in the specified snapshot.
func (manager *SnapshotManager) RetrieveFile(snapshot *Snapshot, file *Entry, output func([]byte)) bool {

	if file.Size == 0 {
		return true
	}

	manager.CreateChunkDownloader()

	// Temporarily disable the snapshot cache of the download so that downloaded file chunks won't be saved
	// to the cache.
	snapshotCache := manager.chunkDownloader.snapshotCache
	manager.chunkDownloader.snapshotCache = nil
	defer func() {
		manager.chunkDownloader.snapshotCache = snapshotCache
	}()

	fileHasher := manager.config.NewFileHasher()
	alternateHash := false
	if strings.HasPrefix(file.Hash, "#") {
		alternateHash = true
	}

	var chunk *Chunk
	currentHash := ""

	for i := file.StartChunk; i <= file.EndChunk; i++ {
		start := 0
		if i == file.StartChunk {
			start = file.StartOffset
		}
		end := snapshot.ChunkLengths[i]
		if i == file.EndChunk {
			end = file.EndOffset
		}

		hash := snapshot.ChunkHashes[i]
		if currentHash != hash {
			i := manager.chunkDownloader.AddChunk(hash)
			chunk = manager.chunkDownloader.WaitForChunk(i)
			currentHash = hash
		}

		output(chunk.GetBytes()[start:end])
		if alternateHash {
			fileHasher.Write([]byte(hex.EncodeToString([]byte(hash))))
		} else {
			fileHasher.Write(chunk.GetBytes()[start:end])
		}
	}

	fileHash := hex.EncodeToString(fileHasher.Sum(nil))
	if alternateHash {
		fileHash = "#" + fileHash
	}
	if strings.ToLower(fileHash) != strings.ToLower(file.Hash) && !SkipFileHash {
		LOG_WARN("SNAPSHOT_HASH", "File %s has mismatched hashes: %s vs %s", file.Path, file.Hash, fileHash)
		return false
	}
	return true
}

// FindFile returns the file entry that has the given file name.
func (manager *SnapshotManager) FindFile(snapshot *Snapshot, filePath string, suppressError bool) *Entry {
	for _, entry := range snapshot.Files {
		if entry.Path == filePath {
			return entry
		}
	}

	if !suppressError {
		LOG_ERROR("SNAPSHOT_FIND", "No file %s found in snapshot %s at revision %d",
			filePath, snapshot.ID, snapshot.Revision)
	}

	return nil
}

// PrintFile prints the specified file or the snapshot to stdout.
func (manager *SnapshotManager) PrintFile(snapshotID string, revision int, path string) bool {

	LOG_DEBUG("PRINT_PARAMETERS", "id: %s, revision: %d, path: %s", snapshotID, revision, path)

	var snapshot *Snapshot

	if revision <= 0 {
		snapshot = manager.downloadLatestSnapshot(snapshotID)
		if snapshot == nil {
			LOG_ERROR("SNAPSHOT_PRINT", "No previous snapshot %s is not found", snapshotID)
			return false
		}
	} else {
		snapshot = manager.DownloadSnapshot(snapshotID, revision)
	}

	if snapshot == nil {
		return false
	}

	patterns := []string{}
	if path != "" {
		patterns = []string{path}
	}

	// If no path is specified, we're printing the snapshot so we need all attributes
	if !manager.DownloadSnapshotContents(snapshot, patterns, path == "") {
		return false
	}

	if path == "" {
		manager.PrintSnapshot(snapshot)
		return true
	}

	file := manager.FindFile(snapshot, path, false)
	if !manager.RetrieveFile(snapshot, file, func(chunk []byte) {
			fmt.Printf("%s", chunk)
		}) {
		LOG_ERROR("SNAPSHOT_RETRIEVE", "File %s is corrupted in snapshot %s at revision %d",
			path, snapshot.ID, snapshot.Revision)
		return false
	}

	return true
}

// Diff compares two snapshots, or two revision of a file if the file argument is given.
func (manager *SnapshotManager) Diff(top string, snapshotID string, revisions []int,
	filePath string, compareByHash bool, nobackupFile string, nofollowlinks bool) bool {

	LOG_DEBUG("DIFF_PARAMETERS", "top: %s, id: %s, revision: %v, path: %s, compareByHash: %t",
		top, snapshotID, revisions, filePath, compareByHash)

	var leftSnapshot *Snapshot
	var rightSnapshot *Snapshot
	var err error

	// If no or only one revision is specified, use the on-disk version for the right-hand side.
	if len(revisions) <= 1 {
		// Only scan the repository if filePath is not provided
		if len(filePath) == 0 {
			rightSnapshot, _, _, err = CreateSnapshotFromDirectory(snapshotID, top, nobackupFile, nofollowlinks)
			if err != nil {
				LOG_ERROR("SNAPSHOT_LIST", "Failed to list the directory %s: %v", top, err)
				return false
			}
		}
	} else {
		rightSnapshot = manager.DownloadSnapshot(snapshotID, revisions[1])
	}

	// If no revision is specified, use the latest revision as the left-hand side.
	if len(revisions) < 1 {
		leftSnapshot = manager.downloadLatestSnapshot(snapshotID)
		if leftSnapshot == nil {
			LOG_ERROR("SNAPSHOT_DIFF", "No previous snapshot %s is not found", snapshotID)
			return false
		}
	} else {
		leftSnapshot = manager.DownloadSnapshot(snapshotID, revisions[0])
	}

	if len(filePath) > 0 {

		manager.DownloadSnapshotContents(leftSnapshot, nil, false)
		if rightSnapshot != nil && rightSnapshot.Revision != 0 {
			manager.DownloadSnapshotContents(rightSnapshot, nil, false)
		}

		var leftFile []byte
		if !manager.RetrieveFile(leftSnapshot, manager.FindFile(leftSnapshot, filePath, false), func(content []byte) {
			leftFile = append(leftFile, content...)
		}) {
			LOG_ERROR("SNAPSHOT_DIFF", "File %s is corrupted in snapshot %s at revision %d",
				filePath, leftSnapshot.ID, leftSnapshot.Revision)
			return false
		}

		var rightFile []byte
		if rightSnapshot != nil {
			if !manager.RetrieveFile(rightSnapshot, manager.FindFile(rightSnapshot, filePath, false), func(content []byte) {
				rightFile = append(rightFile, content...)
			}) {
				LOG_ERROR("SNAPSHOT_DIFF", "File %s is corrupted in snapshot %s at revision %d",
					filePath, rightSnapshot.ID, rightSnapshot.Revision)
				return false
			}
		} else {
			var err error
			rightFile, err = ioutil.ReadFile(joinPath(top, filePath))
			if err != nil {
				LOG_ERROR("SNAPSHOT_DIFF", "Failed to read %s from the repository: %v", filePath, err)
				return false
			}
		}

		leftLines := strings.Split(string(leftFile), "\n")
		rightLines := strings.Split(string(rightFile), "\n")

		after := 10
		before := 10
		var buffer []string
		on := false
		distance := 0

		for _, diff := range difflib.Diff(leftLines, rightLines) {
			if diff.Delta == difflib.Common {
				line := fmt.Sprintf("  %s", diff.Payload)
				if on {
					fmt.Printf("%s\n", line)
					distance++
					if distance > after {
						on = false
					}
				} else {
					buffer = append(buffer, line)
					if len(buffer) > before {
						buffer = buffer[1:]
					}
				}
			} else {
				if !on {
					fmt.Printf("\n%s\n\n", strings.Repeat(" -", 40))
					for _, line := range buffer {
						fmt.Printf("%s\n", line)
					}
					buffer = nil
					on = true
				}
				if diff.Delta == difflib.LeftOnly {
					fmt.Printf("- %s\n", diff.Payload)
				} else {
					fmt.Printf("+ %s\n", diff.Payload)
				}
				distance = 0
			}
		}

		return true
	}

	// We only need to decode the 'files' sequence, not 'chunkhashes' or 'chunklengthes'
	manager.DownloadSnapshotFileSequence(leftSnapshot, nil, false)
	if rightSnapshot != nil && rightSnapshot.Revision != 0 {
		manager.DownloadSnapshotFileSequence(rightSnapshot, nil, false)
	}

	maxSize := int64(9)
	maxSizeDigits := 1

	// Find the max Size value in order for pretty alignment.
	for _, file := range leftSnapshot.Files {
		for !file.IsDir() && file.Size > maxSize {
			maxSize = maxSize*10 + 9
			maxSizeDigits += 1
		}
	}

	for _, file := range rightSnapshot.Files {
		for !file.IsDir() && file.Size > maxSize {
			maxSize = maxSize*10 + 9
			maxSizeDigits += 1
		}
	}

	buffer := make([]byte, 32*1024)

	var i, j int
	for i < len(leftSnapshot.Files) || j < len(rightSnapshot.Files) {

		if i >= len(leftSnapshot.Files) {
			if rightSnapshot.Files[j].IsFile() {
				LOG_INFO("SNAPSHOT_DIFF", "+ %s", rightSnapshot.Files[j].String(maxSizeDigits))
			}
			j++
		} else if j >= len(rightSnapshot.Files) {
			if leftSnapshot.Files[i].IsFile() {
				LOG_INFO("SNAPSHOT_DIFF", "- %s", leftSnapshot.Files[i].String(maxSizeDigits))
			}
			i++
		} else {

			left := leftSnapshot.Files[i]
			right := rightSnapshot.Files[j]

			if !left.IsFile() {
				i++
				continue
			}

			if !right.IsFile() {
				j++
				continue
			}

			c := left.Compare(right)
			if c < 0 {
				LOG_INFO("SNAPSHOT_DIFF", "- %s", left.String(maxSizeDigits))
				i++
			} else if c > 0 {
				LOG_INFO("SNAPSHOT_DIFF", "+ %s", right.String(maxSizeDigits))
				j++
			} else {
				same := false
				if rightSnapshot.Revision == 0 {
					if compareByHash && right.Size > 0 {
						right.Hash = manager.config.ComputeFileHash(joinPath(top, right.Path), buffer)
						same = left.Hash == right.Hash
					} else {
						same = right.IsSameAs(left)
					}
				} else {
					same = left.Hash == right.Hash
				}

				if !same {
					LOG_INFO("SNAPSHOT_DIFF", "  %s", left.String(maxSizeDigits))
					LOG_INFO("SNAPSHOT_DIFF", "* %s", right.String(maxSizeDigits))
				}
				i++
				j++
			}
		}
	}
	return true
}

// ShowHistory shows how a file changes over different revisions.
func (manager *SnapshotManager) ShowHistory(top string, snapshotID string, revisions []int,
	filePath string, showLocalHash bool) bool {

	LOG_DEBUG("HISTORY_PARAMETERS", "top: %s, id: %s, revisions: %v, path: %s, showLocalHash: %t",
		top, snapshotID, revisions, filePath, showLocalHash)

	var err error

	if len(revisions) == 0 {
		revisions, err = manager.ListSnapshotRevisions(snapshotID)
		if err != nil {
			LOG_ERROR("SNAPSHOT_LIST", "Failed to list all revisions for snapshot %s: %v", snapshotID, err)
			return false
		}
	}

	var lastVersion *Entry
	sort.Ints(revisions)
	for _, revision := range revisions {
		snapshot := manager.DownloadSnapshot(snapshotID, revision)
		manager.DownloadSnapshotFileSequence(snapshot, nil, false)
		file := manager.FindFile(snapshot, filePath, true)

		if file != nil {

			if !file.IsFile() {
				continue
			}

			modifiedFlag := ""
			if lastVersion != nil && lastVersion.Hash != file.Hash {
				modifiedFlag = "*"
			}
			LOG_INFO("SNAPSHOT_HISTORY", "%7d: %s%s", revision, file.String(15), modifiedFlag)
			lastVersion = file
		} else {
			LOG_INFO("SNAPSHOT_HISTORY", "%7d:", revision)
		}

	}

	stat, err := os.Stat(joinPath(top, filePath))
	if stat != nil {
		localFile := CreateEntry(filePath, stat.Size(), stat.ModTime().Unix(), 0)
		modifiedFlag := ""
		if lastVersion != nil && !lastVersion.IsSameAs(localFile) {
			modifiedFlag = "*"
		}
		if showLocalHash {
			localFile.Hash = manager.config.ComputeFileHash(joinPath(top, filePath), make([]byte, 32*1024))
			if lastVersion == nil || lastVersion.Hash != localFile.Hash {
				modifiedFlag = "*"
			}
		}
		LOG_INFO("SNAPSHOT_HISTORY", "current: %s%s", localFile.String(15), modifiedFlag)
	} else {
		LOG_INFO("SNAPSHOT_HISTORY", "current:")
	}

	return true
}

// fossilizeChunk turns the chunk into a fossil.
func (manager *SnapshotManager) fossilizeChunk(chunkID string, filePath string, exclusive bool) bool {
	if exclusive {
		manager.chunkOperator.Delete(chunkID, filePath)
	} else {
		manager.chunkOperator.Fossilize(chunkID, filePath)
	}

	return true

}

// resurrectChunk turns the fossil back into a chunk
func (manager *SnapshotManager) resurrectChunk(fossilPath string, chunkID string) bool {
	chunkPath, exist, _, err := manager.storage.FindChunk(0, chunkID, false)
	if err != nil {
		LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", chunkID, err)
		return false
	}

	if exist {
		manager.storage.DeleteFile(0, fossilPath)
		LOG_INFO("FOSSIL_RECREATE", "The chunk %s already exists", chunkID)
	} else {
		err := manager.storage.MoveFile(0, fossilPath, chunkPath)
		if err != nil {
			LOG_ERROR("FOSSIL_RESURRECT", "Failed to resurrect the chunk %s from the fossil %s: %v",
				chunkID, fossilPath, err)
			return false
		} else {
			LOG_INFO("FOSSIL_RESURRECT", "The chunk %s has been resurrected", fossilPath)
		}
	}
	return true
}

// PruneSnapshots deletes snapshots by revisions, tags, or a retention policy.  The main idea is two-step
// fossil collection.
// 1. Delete snapshots specified by revision, retention policy, with a tag.  Find any resulting unreferenced
//    chunks, and mark them as fossils (by renaming).  After that, create a fossil collection file containing
//    fossils collected during current run, and temporary files encountered.  Also in the file is the latest
//    revision for each snapshot id.  Save this file to a local directory.
//
// 2. On next run, check if there is any new revision for each snapshot.  Or if the lastest revision is too
//    old, for instance, more than 7 days.  This step is to identify snapshots that were being created while
//    step 1 is in progress.  For each fossil reference by any of these snapshots, move them back to the
//    normal chunk directory.
//
// Note that a snapshot being created when step 2 is in progress may reference a fossil.  To avoid this
// problem, never remove the lastest revision (unless exclusive is true), and only cache chunks referenced
// by the lastest revision.
func (manager *SnapshotManager) PruneSnapshots(selfID string, snapshotID string, revisionsToBeDeleted []int,
	tags []string, retentions []string,
	exhaustive bool, exclusive bool, ignoredIDs []string,
	dryRun bool, deleteOnly bool, collectOnly bool, threads int) bool {

	LOG_DEBUG("DELETE_PARAMETERS",
		"id: %s, revisions: %v, tags: %v, retentions: %v, exhaustive: %t, exclusive: %t, "+
			"dryrun: %t, deleteOnly: %t, collectOnly: %t",
		snapshotID, revisionsToBeDeleted, tags, retentions,
		exhaustive, exclusive, dryRun, deleteOnly, collectOnly)

	if len(revisionsToBeDeleted) > 0 && (len(tags) > 0 || len(retentions) > 0) {
		LOG_WARN("DELETE_OPTIONS", "Tags or retention policy will be ignored if at least one revision is specified")
	}

	manager.chunkOperator = CreateChunkOperator(manager.storage, threads)
	defer manager.chunkOperator.Stop()

	prefPath := GetDuplicacyPreferencePath()
	logDir := path.Join(prefPath, "logs")
	err := os.MkdirAll(logDir, 0700)
	if err != nil {
		LOG_ERROR("LOG_DIR", "Could not open log directory %s: %v", logDir, err)
	}
	logFileName := path.Join(logDir, time.Now().Format("prune-log-20060102-150405"))
	logFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		LOG_ERROR("LOG_FILE", "Could not open log file %s: %v", logFileName, err)
	}

	defer func() {
		if logFile != nil {
			cerr := logFile.Close()
			if cerr != nil {
				LOG_WARN("LOG_FILE", "Could not close log file %s: %v", logFileName, cerr)
			}
		}
	}()

	// A retention policy is specified in the form 'interval:age', where both 'interval' and 'age' are numbers of
	// days. A retention policy applies to a snapshot if the snapshot is older than the age.  For snapshots older
	// than the retention age, only one snapshot can be kept per interval.  if interval is 0, then no snapshot older
	// than the retention age will be kept.
	//
	// For example, ["30:365", "7:30", "1:1"] means to keep one snapshot per month for snapshots older than a year,
	// one snapshot per week for snapshots older than a month, and one snapshot per day for snapshot older than a day.
	//
	// Note that policies must be sorted by the ages in decreasing order.
	//
	type RetentionPolicy struct {
		Age      int
		Interval int
	}
	var retentionPolicies []RetentionPolicy

	// Parse the retention policy if needed.
	if len(revisionsToBeDeleted) == 0 && len(retentions) > 0 {

		retentionRegex := regexp.MustCompile(`^([0-9]+):([0-9]+)$`)

		for _, retention := range retentions {
			retention = strings.TrimSpace(retention)

			matched := retentionRegex.FindStringSubmatch(retention)

			if matched == nil {
				LOG_ERROR("RETENTION_INVALID", "Invalid retention policy: %s", retention)
				return false
			}

			age, _ := strconv.Atoi(matched[2])
			interval, _ := strconv.Atoi(matched[1])

			if age < 1 || interval < 0 {
				LOG_ERROR("RETENTION_INVALID", "Invalid retention policy: %s", retention)
				return false
			}

			policy := RetentionPolicy{
				Age:      age,
				Interval: interval,
			}

			retentionPolicies = append(retentionPolicies, policy)
		}

		if len(retentionPolicies) == 0 {
			LOG_ERROR("RETENTION_INVALID", "Invalid retention policy: %v", retentions)
			return false
		}

		for i, policy := range retentionPolicies {
			if i == 0 || policy.Age < retentionPolicies[i-1].Age {
				if policy.Interval == 0 {
					LOG_INFO("RETENTION_POLICY", "Keep no snapshots older than %d days", policy.Age)
				} else {
					LOG_INFO("RETENTION_POLICY", "Keep 1 snapshot every %d day(s) if older than %d day(s)",
						policy.Interval, policy.Age)
				}
			}
		}
	}

	allSnapshots := make(map[string][]*Snapshot)

	// We must find all snapshots for all ids even if only one snapshot is specified to be deleted,
	// because we need to find out which chunks are not referenced.
	snapshotIDs, err := manager.ListSnapshotIDs()
	if err != nil {
		LOG_ERROR("SNAPSHOT_LIST", "Failed to list all snapshots: %v", err)
		return false
	}

	for _, id := range snapshotIDs {
		var revisions []int
		revisions, err = manager.ListSnapshotRevisions(id)
		if err != nil {
			LOG_ERROR("SNAPSHOT_LIST", "Failed to list all revisions for snapshot %s: %v", id, err)
			return false
		}

		sort.Ints(revisions)
		var snapshots []*Snapshot
		for _, revision := range revisions {
			snapshot := manager.DownloadSnapshot(id, revision)
			if snapshot != nil {
				snapshots = append(snapshots, snapshot)
			}
		}

		if len(snapshots) > 0 {
			allSnapshots[id] = snapshots
		}
	}

	collectionRegex := regexp.MustCompile(`^([0-9]+)$`)

	collectionDir := "fossils"
	err = manager.snapshotCache.CreateDirectory(0, collectionDir)
	if err != nil {
		LOG_ERROR("FOSSIL_COLLECT", "Failed to create collection directory %s: %v", collectionDir, err)
		return false
	}

	collections, _, err := manager.snapshotCache.ListFiles(0, collectionDir)
	if err != nil {
		LOG_ERROR("FOSSIL_COLLECT", "Failed to list fossil collection files for dir %s: %v", collectionDir, err)
		return false
	}
	maxCollectionNumber := 0

	referencedFossils := make(map[string]bool)

	// Find fossil collections previously created, and delete fossils and temporary files in them if they are
	// deletable.
	for _, collectionName := range collections {

		if collectOnly {
			continue
		}

		matched := collectionRegex.FindStringSubmatch(collectionName)
		if matched == nil {
			continue
		}

		collectionNumber, _ := strconv.Atoi(matched[1])
		if collectionNumber > maxCollectionNumber {
			maxCollectionNumber = collectionNumber
		}

		collectionFile := path.Join(collectionDir, collectionName)
		manager.fileChunk.Reset(false)

		err = manager.snapshotCache.DownloadFile(0, collectionFile, manager.fileChunk)
		if err != nil {
			LOG_ERROR("FOSSIL_COLLECT", "Failed to read the fossil collection file %s: %v", collectionFile, err)
			return false
		}

		var collection FossilCollection
		err = json.Unmarshal(manager.fileChunk.GetBytes(), &collection)
		if err != nil {
			LOG_ERROR("FOSSIL_COLLECT", "Failed to load the fossil collection file %s: %v", collectionFile, err)
			return false
		}

		// Determine if any deleted revisions exist
		exist := false
		for snapshotID, revisionList := range collection.DeletedRevisions {
			for _, revision := range revisionList {
				for _, snapshot := range allSnapshots[snapshotID] {
					if revision == snapshot.Revision {
						LOG_INFO("FOSSIL_GHOSTSNAPSHOT", "Snapshot %s revision %d should have been deleted already", snapshotID, revision)
						exist = true
					}
				}
			}
		}

		if exist {
			err = manager.snapshotCache.DeleteFile(0, collectionFile)
			if err != nil {
				LOG_WARN("FOSSIL_FILE", "Failed to remove the fossil collection file %s: %v", collectionFile, err)
			} else {
				LOG_INFO("FOSSIL_IGNORE", "The fossil collection file %s has been ignored due to ghost snapshots", collectionFile)
			}
			continue
		}

		for _, fossil := range collection.Fossils {
			referencedFossils[fossil] = true
		}

		LOG_INFO("FOSSIL_COLLECT", "Fossil collection %s found", collectionName)

		isDeletable, newSnapshots := collection.IsDeletable(manager.storage.IsStrongConsistent(),
			ignoredIDs, allSnapshots)

		if isDeletable || exclusive {

			LOG_INFO("FOSSIL_DELETABLE", "Fossils from collection %s is eligible for deletion", collectionName)

			newChunks := make(map[string]bool)

			for _, newSnapshot := range newSnapshots {
				fmt.Fprintf(logFile, "Snapshot %s revision %d was created after collection %s\n", newSnapshot.ID, newSnapshot.Revision, collectionName)
				LOG_INFO("PRUNE_NEWSNAPSHOT", "Snapshot %s revision %d was created after collection %s", newSnapshot.ID, newSnapshot.Revision, collectionName)
				for _, chunk := range manager.GetSnapshotChunks(newSnapshot, false) {
					newChunks[chunk] = true
				}
			}

			for _, fossil := range collection.Fossils {

				chunk := fossil[len(chunkDir):]
				chunk = strings.Replace(chunk, "/", "", -1)
				chunk = strings.Replace(chunk, ".fsl", "", -1)

				if _, found := newChunks[chunk]; found {
					// The fossil is referenced so it can't be deleted.
					if dryRun {
						LOG_INFO("FOSSIL_RESURRECT", "Fossil %s would be resurrected: %v", chunk)
						continue
					}

					manager.chunkOperator.Resurrect(chunk, fossil)
					fmt.Fprintf(logFile, "Resurrected fossil %s (collection %s)\n", chunk, collectionName)

				} else {
					if dryRun {
						LOG_INFO("FOSSIL_DELETE", "The chunk %s would be permanently removed", chunk)
					} else {
						manager.chunkOperator.Delete(chunk, fossil)
						fmt.Fprintf(logFile, "Deleted fossil %s (collection %s)\n", chunk, collectionName)
					}
				}
			}

			// Delete all temporary files if they still exist.
			for _, temporary := range collection.Temporaries {
				if dryRun {
					LOG_INFO("TEMPORARY_DELETE", "The temporary file %s would be deleted", temporary)
				} else {
					// Fail silently, since temporary files are supposed to be renamed or deleted after upload is done
					manager.chunkOperator.Delete("", temporary)
					fmt.Fprintf(logFile, "Deleted temporary %s (collection %s)\n", temporary, collectionName)
				}
			}

			if !dryRun {
				err = manager.snapshotCache.DeleteFile(0, collectionFile)
				if err != nil {
					LOG_WARN("FOSSIL_FILE", "Failed to remove the fossil collection file %s: %v", collectionFile, err)
				}
			}
			LOG_TRACE("FOSSIL_END", "Finished processing fossil collection %s", collectionName)
		} else {
			LOG_INFO("FOSSIL_POSTPONE",
				"Fossils from collection %s can't be deleted because deletion criteria aren't met",
				collectionName)
		}
	}

	if deleteOnly {
		return true
	}

	toBeDeleted := 0

	revisionMap := make(map[int]bool)
	for _, revision := range revisionsToBeDeleted {
		revisionMap[revision] = true
	}

	tagMap := make(map[string]bool)
	for _, tag := range tags {
		tagMap[tag] = true
	}

	// Find the snapshots that need to be deleted
	for id, snapshots := range allSnapshots {

		if len(snapshotID) > 0 && id != snapshotID {
			continue
		}

		if len(revisionsToBeDeleted) > 0 {
			// If revisions are specified ignore tags and the retention policy.
			for _, snapshot := range snapshots {
				if _, found := revisionMap[snapshot.Revision]; found {
					LOG_DEBUG("SNAPSHOT_DELETE", "Snapshot %s at revision %d to be deleted - specified in command",
						snapshot.ID, snapshot.Revision)
					snapshot.Flag = true
					toBeDeleted++
				}
			}

			continue
		} else if len(retentionPolicies) > 0 {

			if len(snapshots) <= 1 {
				continue
			}

			lastSnapshotTime := int64(0)
			now := time.Now().Unix()
			i := 0
			for j, snapshot := range snapshots {

				if !exclusive && j == len(snapshots)-1 {
					continue
				}

				if len(tagMap) > 0 {
					if _, found := tagMap[snapshot.Tag]; !found {
						continue
					}
				}

				// Find out which retent policy applies based on the age.
				for i < len(retentionPolicies) &&
					getDaysBetween(snapshot.StartTime, now) < retentionPolicies[i].Age {
					i++
					lastSnapshotTime = 0
				}

				if i < len(retentionPolicies) {
					if retentionPolicies[i].Interval == 0 {
						// No snapshots to keep if interval is 0
						LOG_DEBUG("SNAPSHOT_DELETE", "Snapshot %s at revision %d to be deleted - older than %d days",
							snapshot.ID, snapshot.Revision, retentionPolicies[i].Age)
						snapshot.Flag = true
						toBeDeleted++
					} else if lastSnapshotTime != 0 &&
						getDaysBetween(lastSnapshotTime, snapshot.StartTime) < retentionPolicies[i].Interval {
						// Delete the snapshot if it is too close to the last kept one.
						LOG_DEBUG("SNAPSHOT_DELETE", "Snapshot %s at revision %d to be deleted - older than %d days, less than %d days from previous",
							snapshot.ID, snapshot.Revision, retentionPolicies[i].Age, retentionPolicies[i].Interval)
						snapshot.Flag = true
						toBeDeleted++
					} else {
						lastSnapshotTime = snapshot.StartTime
					}
				} else {
					// Ran out of retention policy; no need to check further
					break
				}
			}

		} else if len(tags) > 0 {
			for _, snapshot := range snapshots {
				if _, found := tagMap[snapshot.Tag]; found {
					snapshot.Flag = true
					toBeDeleted++
				}
			}

		}
	}

	if toBeDeleted == 0 && !exhaustive {
		LOG_INFO("SNAPSHOT_NONE", "No snapshot to delete")
		return false
	}

	collection := CreateFossilCollection(allSnapshots)

	var success bool
	if exhaustive {
		success = manager.pruneSnapshotsExhaustive(referencedFossils, allSnapshots, collection, logFile, dryRun, exclusive)
	} else {
		success = manager.pruneSnapshotsNonExhaustive(allSnapshots, collection, logFile, dryRun, exclusive)
	}
	if !success {
		return false
	}

	manager.chunkOperator.Stop()
	for _, fossil := range manager.chunkOperator.fossils {
		collection.AddFossil(fossil)
	}

	// Save the deleted revision in the fossil collection
	for _, snapshots := range allSnapshots {
		for _, snapshot := range snapshots {
			if snapshot.Flag {
				collection.DeletedRevisions[snapshot.ID] = append(collection.DeletedRevisions[snapshot.ID], snapshot.Revision)
			}
		}
	}

	// Save the fossil collection if it is not empty.
	if !collection.IsEmpty() && !dryRun && !exclusive {
		collection.EndTime = time.Now().Unix()

		collectionNumber := maxCollectionNumber + 1
		collectionFile := path.Join(collectionDir, fmt.Sprintf("%d", collectionNumber))

		var description []byte
		description, err = json.Marshal(collection)
		if err != nil {
			LOG_ERROR("FOSSIL_COLLECT", "Failed to create a json file for the fossil collection: %v", err)
			return false
		}

		err = manager.snapshotCache.UploadFile(0, collectionFile, description)
		if err != nil {
			LOG_ERROR("FOSSIL_COLLECT", "Failed to save the fossil collection file %s: %v", collectionFile, err)
			return false
		}

		LOG_INFO("FOSSIL_COLLECT", "Fossil collection %d saved", collectionNumber)
		fmt.Fprintf(logFile, "Fossil collection %d saved\n", collectionNumber)
	}

	// Now delete the snapshot files.
	for _, snapshots := range allSnapshots {
		for _, snapshot := range snapshots {
			if !snapshot.Flag || dryRun {
				continue
			}

			snapshotPath := fmt.Sprintf("snapshots/%s/%d", snapshot.ID, snapshot.Revision)
			err = manager.storage.DeleteFile(0, snapshotPath)
			if err != nil {
				LOG_ERROR("SNAPSHOT_DELETE", "Failed to delete the snapshot %s at revision %d: %v",
					snapshot.ID, snapshot.Revision, err)
				return false
			}
			LOG_INFO("SNAPSHOT_DELETE", "The snapshot %s at revision %d has been removed",
				snapshot.ID, snapshot.Revision)
			err = manager.snapshotCache.DeleteFile(0, snapshotPath)
			if err != nil {
				LOG_WARN("SNAPSHOT_DELETE", "The cached snapshot %s at revision %d could not be removed: %v",
					snapshot.ID, snapshot.Revision, err)
				fmt.Fprintf(logFile, "Cached snapshot %s at revision %d could not be removed: %v",
					snapshot.ID, snapshot.Revision, err)
			} else {
				fmt.Fprintf(logFile, "Deleted cached snapshot %s at revision %d\n", snapshot.ID, snapshot.Revision)
			}
		}
	}

	if collection.IsEmpty() && !dryRun && toBeDeleted != 0 && !exclusive {
		LOG_INFO("FOSSIL_NONE",
			"No fossil collection has been created since deleted snapshots did not reference any unique chunks")
	}

	var latestSnapshot *Snapshot
	if len(allSnapshots[selfID]) > 0 {
		latestSnapshot = allSnapshots[selfID][len(allSnapshots[selfID])-1]
	}

	if latestSnapshot != nil && !latestSnapshot.Flag {
		manager.CleanSnapshotCache(latestSnapshot, allSnapshots)
	} else {
		manager.CleanSnapshotCache(nil, allSnapshots)
	}

	return true
}

// pruneSnapshots in non-exhaustive mode, only chunks that exist in the
// snapshots to be deleted but not other are identified as unreferenced chunks.
func (manager *SnapshotManager) pruneSnapshotsNonExhaustive(allSnapshots map[string][]*Snapshot, collection *FossilCollection, logFile io.Writer, dryRun, exclusive bool) bool {
	targetChunks := make(map[string]bool)

	// Now build all chunks referened by snapshot not deleted
	for _, snapshots := range allSnapshots {

		if len(snapshots) > 0 {
			latest := snapshots[len(snapshots)-1]
			if latest.Flag && !exclusive {
				LOG_ERROR("SNAPSHOT_DELETE",
					"The latest snapshot %s at revision %d can't be deleted in non-exclusive mode",
					latest.ID, latest.Revision)
				return false
			}
		}

		for _, snapshot := range snapshots {
			if !snapshot.Flag {
				continue
			}

			LOG_INFO("SNAPSHOT_DELETE", "Deleting snapshot %s at revision %d", snapshot.ID, snapshot.Revision)
			chunks := manager.GetSnapshotChunks(snapshot, false)

			for _, chunk := range chunks {
				// The initial value is 'false'.  When a referenced chunk is found it will change the value to 'true'.
				targetChunks[chunk] = false
			}
		}
	}

	for _, snapshots := range allSnapshots {
		for _, snapshot := range snapshots {
			if snapshot.Flag {
				continue
			}

			chunks := manager.GetSnapshotChunks(snapshot, false)

			for _, chunk := range chunks {
				if _, found := targetChunks[chunk]; found {
					targetChunks[chunk] = true
				}
			}
		}
	}

	for chunk, value := range targetChunks {
		if value {
			continue
		}

		if dryRun {
			LOG_INFO("CHUNK_UNREFERENCED", "Found unreferenced chunk %s", chunk)
			continue
		}

		manager.fossilizeChunk(chunk, "", exclusive)
		if exclusive {
			fmt.Fprintf(logFile, "Deleted chunk %s (exclusive mode)\n", chunk)
		} else {
			fmt.Fprintf(logFile, "Marked fossil %s\n", chunk)
		}

		targetChunks[chunk] = true
	}

	return true
}

// pruneSnapshotsExhaustive in exhaustive, we scan the entire chunk tree to
// find dangling chunks and temporaries.
func (manager *SnapshotManager) pruneSnapshotsExhaustive(referencedFossils map[string]bool, allSnapshots map[string][]*Snapshot, collection *FossilCollection, logFile io.Writer, dryRun, exclusive bool) bool {
	chunkRegex := regexp.MustCompile(`^[0-9a-f]+$`)
	referencedChunks := make(map[string]bool)

	// Now build all chunks referened by snapshot not deleted
	for _, snapshots := range allSnapshots {
		if len(snapshots) > 0 {
			latest := snapshots[len(snapshots)-1]
			if latest.Flag && !exclusive {
				LOG_ERROR("SNAPSHOT_DELETE",
					"The latest snapshot %s at revision %d can't be deleted in non-exclusive mode",
					latest.ID, latest.Revision)
				return false
			}
		}

		for _, snapshot := range snapshots {
			if snapshot.Flag {
				LOG_INFO("SNAPSHOT_DELETE", "Deleting snapshot %s at revision %d", snapshot.ID, snapshot.Revision)
				continue
			}

			chunks := manager.GetSnapshotChunks(snapshot, false)

			for _, chunk := range chunks {
				// The initial value is 'false'.  When a referenced chunk is found it will change the value to 'true'.
				referencedChunks[chunk] = false
			}
		}
	}

	allFiles, _ := manager.ListAllFiles(manager.storage, chunkDir)
	for _, file := range allFiles {
		if file[len(file)-1] == '/' {
			continue
		}

		if strings.HasSuffix(file, ".tmp") {
			// This is a temporary chunk file.  It can be a result of a restore operation still in progress, or
			// a left-over from a restore operation that was terminated abruptly.
			if dryRun {
				LOG_INFO("CHUNK_TEMPORARY", "Found temporary file %s", file)
				continue
			}

			if exclusive {
				// In exclusive mode, we assume no other restore operation is running concurrently.
				manager.chunkOperator.Delete("", chunkDir+file)
				fmt.Fprintf(logFile, "Deleted temporary %s\n", file)
			} else {
				collection.AddTemporary(file)
			}
			continue
		} else if strings.HasSuffix(file, ".fsl") {
			// This is a fossil.  If it is unreferenced, it can be a result of failing to save the fossil
			// collection file after making it a fossil.
			if _, found := referencedFossils[file]; !found {

				chunk := strings.Replace(file, "/", "", -1)
				chunk = strings.Replace(chunk, ".fsl", "", -1)

				if _, found := referencedChunks[chunk]; found {

					if dryRun {
						LOG_INFO("FOSSIL_REFERENCED", "Found referenced fossil %s", file)
						continue
					}

					manager.chunkOperator.Resurrect(chunk, chunkDir+file)
					fmt.Fprintf(logFile, "Found referenced fossil %s\n", file)

				} else {

					if dryRun {
						LOG_INFO("FOSSIL_UNREFERENCED", "Found unreferenced fossil %s", file)
						continue
					}

					if exclusive {
						manager.chunkOperator.Delete(chunk, chunkDir+file)
					} else {
						collection.AddFossil(chunkDir + file)
						LOG_DEBUG("FOSSIL_FIND", "Found unreferenced fossil %s", file)
					}
					fmt.Fprintf(logFile, "Found unreferenced fossil %s\n", file)
				}
			}

			continue
		}

		chunk := strings.Replace(file, "/", "", -1)

		if !chunkRegex.MatchString(chunk) {
			LOG_WARN("CHUNK_UNKNOWN_FILE", "File %s is not a chunk", file)
			continue
		}

		if value, found := referencedChunks[chunk]; !found {
			if dryRun {
				LOG_INFO("CHUNK_UNREFERENCED", "Found unreferenced chunk %s", chunk)
				continue
			}

			manager.fossilizeChunk(chunk, chunkDir+file, exclusive)
			if exclusive {
				fmt.Fprintf(logFile, "Deleted chunk %s (exclusive mode)\n", chunk)
			} else {
				fmt.Fprintf(logFile, "Marked fossil %s\n", chunk)
			}
		} else if value {
			// Note that the initial value is false.  So if the value is true it means another copy of the chunk
			// exists in a higher-level directory.

			if dryRun {
				LOG_INFO("CHUNK_REDUNDANT", "Found redundant chunk %s", chunk)
				continue
			}

			// This is a redundant chunk file (for instance D3/495A8D and D3/49/5A8D )
			manager.chunkOperator.Delete(chunk, chunkDir+file)
			fmt.Fprintf(logFile, "Deleted redundant chunk %s\n", file)
		} else {
			referencedChunks[chunk] = true
			LOG_DEBUG("CHUNK_KEEP", "Chunk %s is referenced", chunk)
		}
	}

	return true
}

// CheckSnapshot performs sanity checks on the given snapshot.
func (manager *SnapshotManager) CheckSnapshot(snapshot *Snapshot) (err error) {

	lastChunk := 0
	lastOffset := 0
	var lastEntry *Entry

	numberOfChunks := len(snapshot.ChunkHashes)

	if numberOfChunks != len(snapshot.ChunkLengths) {
		return fmt.Errorf("The number of chunk hashes (%d) is different from the number of chunk lengths (%d)",
			numberOfChunks, len(snapshot.ChunkLengths))
	}

	entries := make([]*Entry, len(snapshot.Files))
	copy(entries, snapshot.Files)
	sort.Sort(ByChunk(entries))

	for _, entry := range snapshot.Files {
		if lastEntry != nil && lastEntry.Compare(entry) >= 0 && !strings.Contains(lastEntry.Path, "\ufffd") {
			return fmt.Errorf("The entry %s appears before the entry %s", lastEntry.Path, entry.Path)
		}
		lastEntry = entry
	}

	for _, entry := range entries {

		if !entry.IsFile() || entry.Size == 0 {
			continue
		}

		if entry.StartChunk < 0 {
			return fmt.Errorf("The file %s starts at chunk %d", entry.Path, entry.StartChunk)
		}

		if entry.EndChunk >= numberOfChunks {
			return fmt.Errorf("The file %s ends at chunk %d while the number of chunks is %d",
				entry.Path, entry.EndChunk, numberOfChunks)
		}

		if entry.EndChunk < entry.StartChunk {
			return fmt.Errorf("The file %s starts at chunk %d and ends at chunk %d",
				entry.Path, entry.StartChunk, entry.EndChunk)
		}

		if entry.StartOffset > 0 {
			if entry.StartChunk < lastChunk {
				return fmt.Errorf("The file %s starts at chunk %d while the last chunk is %d",
					entry.Path, entry.StartChunk, lastChunk)
			}

			if entry.StartChunk > lastChunk+1 {
				return fmt.Errorf("The file %s starts at chunk %d while the last chunk is %d",
					entry.Path, entry.StartChunk, lastChunk)
			}

			if entry.StartChunk == lastChunk && entry.StartOffset < lastOffset {
				return fmt.Errorf("The file %s starts at offset %d of chunk %d while the last file ends at offset %d",
					entry.Path, entry.StartOffset, entry.StartChunk, lastOffset)
			}

			if entry.StartChunk == entry.EndChunk && entry.StartOffset > entry.EndOffset {
				return fmt.Errorf("The file %s starts at offset %d and ends at offset %d of the same chunk %d",
					entry.Path, entry.StartOffset, entry.EndOffset, entry.StartChunk)
			}
		}

		fileSize := int64(0)

		for i := entry.StartChunk; i <= entry.EndChunk; i++ {

			start := 0
			if i == entry.StartChunk {
				start = entry.StartOffset
			}
			end := snapshot.ChunkLengths[i]
			if i == entry.EndChunk {
				end = entry.EndOffset
			}

			fileSize += int64(end - start)
		}

		if entry.Size != fileSize {
			return fmt.Errorf("The file %s has a size of %d but the total size of chunks is %d",
				entry.Path, entry.Size, fileSize)
		}

		lastChunk = entry.EndChunk
		lastOffset = entry.EndOffset
	}

	if len(entries) > 0 && entries[0].StartChunk != 0 {
		return fmt.Errorf("The first file starts at chunk %d", entries[0].StartChunk)
	}

	// There may be a last chunk whose size is 0 so we allow this to happen
	if lastChunk < numberOfChunks-2 {
		return fmt.Errorf("The last file ends at chunk %d but the number of chunks is %d", lastChunk, numberOfChunks)
	}

	return nil
}

// DownloadFile downloads a non-chunk file from the storage.  The only non-chunk files in the current implementation
// are snapshot files.
func (manager *SnapshotManager) DownloadFile(path string, derivationKey string) (content []byte) {

	if manager.storage.IsCacheNeeded() {
		manager.fileChunk.Reset(false)
		err := manager.snapshotCache.DownloadFile(0, path, manager.fileChunk)
		if err == nil && len(manager.fileChunk.GetBytes()) > 0 {
			LOG_DEBUG("DOWNLOAD_FILE_CACHE", "Loaded file %s from the snapshot cache", path)
			return manager.fileChunk.GetBytes()
		}
	}

	manager.fileChunk.Reset(false)
	err := manager.storage.DownloadFile(0, path, manager.fileChunk)
	if err != nil {
		LOG_ERROR("DOWNLOAD_FILE", "Failed to download the file %s: %v", path, err)
		return nil
	}

	if len(derivationKey) > 64 {
		derivationKey = derivationKey[len(derivationKey)-64:]
	}

	err = manager.fileChunk.Decrypt(manager.config.FileKey, derivationKey)
	if err != nil {
		LOG_ERROR("DOWNLOAD_DECRYPT", "Failed to decrypt the file %s: %v", path, err)
		return nil
	}

	err = manager.snapshotCache.UploadFile(0, path, manager.fileChunk.GetBytes())
	if err != nil {
		LOG_WARN("DOWNLOAD_FILE_CACHE", "Failed to add the file %s to the snapshot cache: %v", path, err)
	}

	LOG_DEBUG("DOWNLOAD_FILE", "Downloaded file %s", path)

	return manager.fileChunk.GetBytes()
}

// UploadFile uploads a non-chunk file from the storage.
func (manager *SnapshotManager) UploadFile(path string, derivationKey string, content []byte) bool {
	manager.fileChunk.Reset(false)
	manager.fileChunk.Write(content)

	if manager.storage.IsCacheNeeded() {
		err := manager.snapshotCache.UploadFile(0, path, manager.fileChunk.GetBytes())
		if err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to cache the file %s: %v", path, err)
		} else {
			LOG_DEBUG("UPLOAD_FILE_CACHE", "Saved file %s to the snapshot cache", path)
		}
	}

	if len(derivationKey) > 64 {
		derivationKey = derivationKey[len(derivationKey)-64:]
	}

	err := manager.fileChunk.Encrypt(manager.config.FileKey, derivationKey)
	if err != nil {
		LOG_ERROR("UPLOAD_File", "Failed to encrypt the file %s: %v", path, err)
		return false
	}

	err = manager.storage.UploadFile(0, path, manager.fileChunk.GetBytes())
	if err != nil {
		LOG_ERROR("UPLOAD_File", "Failed to upload the file %s: %v", path, err)
		return false
	}

	LOG_DEBUG("UPLOAD_FILE", "Uploaded file %s", path)

	return true

}
