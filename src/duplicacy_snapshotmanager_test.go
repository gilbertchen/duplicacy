// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack"
)

func createDummySnapshot(snapshotID string, revision int, endTime int64) *Snapshot {
	return &Snapshot{
		ID:       snapshotID,
		Revision: revision,
		EndTime:  endTime,
	}
}

func TestIsDeletable(t *testing.T) {

	//SetLoggingLevel(DEBUG)

	now := time.Now().Unix()
	day := int64(3600 * 24)

	allSnapshots := make(map[string][]*Snapshot)
	allSnapshots["host1"] = append([]*Snapshot{}, createDummySnapshot("host1", 1, now-2*day))
	allSnapshots["host2"] = append([]*Snapshot{}, createDummySnapshot("host2", 1, now-2*day))
	allSnapshots["host1"] = append(allSnapshots["host1"], createDummySnapshot("host1", 2, now-1*day))
	allSnapshots["host2"] = append(allSnapshots["host2"], createDummySnapshot("host2", 2, now-1*day))

	collection := &FossilCollection{
		EndTime:       now - day - 3600,
		LastRevisions: make(map[string]int),
	}

	collection.LastRevisions["host1"] = 1
	collection.LastRevisions["host2"] = 1

	isDeletable, newSnapshots := collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 2 {
		t.Errorf("Scenario 1: should be deletable, 2 new snapshots")
	}

	collection.LastRevisions["host3"] = 1
	allSnapshots["host3"] = append([]*Snapshot{}, createDummySnapshot("host3", 1, now-2*day))

	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if isDeletable {
		t.Errorf("Scenario 2: should not be deletable")
	}

	allSnapshots["host3"] = append(allSnapshots["host3"], createDummySnapshot("host3", 2, now-day))
	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 3 {
		t.Errorf("Scenario 3: should be deletable, 3 new snapshots")
	}

	collection.LastRevisions["host4"] = 1
	allSnapshots["host4"] = append([]*Snapshot{}, createDummySnapshot("host4", 1, now-8*day))

	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 3 {
		t.Errorf("Scenario 4: should be deletable, 3 new snapshots")
	}

	collection.LastRevisions["repository1@host5"] = 1
	allSnapshots["repository1@host5"] = append([]*Snapshot{}, createDummySnapshot("repository1@host5", 1, now-3*day))

	collection.LastRevisions["repository2@host5"] = 1
	allSnapshots["repository2@host5"] = append([]*Snapshot{}, createDummySnapshot("repository2@host5", 1, now-2*day))

	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if isDeletable {
		t.Errorf("Scenario 5: should not be deletable")
	}

	allSnapshots["repository1@host5"] = append(allSnapshots["repository1@host5"], createDummySnapshot("repository1@host5", 2, now-day))
	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 4 {
		t.Errorf("Scenario 6: should be deletable, 4 new snapshots")
	}
}

// createTestSnapshotWithFiles uploads a snapshot that lists the given files rather than an empty one, so that the
// commands that walk the file list of a snapshot (list -files) have something to read.  Each file is stored in its own
// chunk, and the entries are encoded the same way BackupManager.UploadSnapshot writes them.  The file hashes that the
// entries carry are returned, so that a test can predict what the file list is printed as.
func createTestSnapshotWithFiles(manager *SnapshotManager, snapshotID string, revision int, startTime int64,
	endTime int64, fileNames []string, fileSizes []int64, tag string) (fileHashes []string) {

	// One chunk per file, with one byte of content per byte of file size.
	contents := make([][]byte, len(fileNames))
	for i, fileSize := range fileSizes {
		content := make([]byte, fileSize)
		if _, err := rand.Read(content); err != nil {
			LOG_ERROR("SNAPSHOT_UPLOAD", "Failed to generate the content of the file %s: %v", fileNames[i], err)
			return nil
		}
		contents[i] = content
	}

	return createTestSnapshotWithContent(manager, snapshotID, revision, startTime, endTime, fileNames, contents, tag)
}

// createTestSnapshotWithContent uploads a snapshot whose files carry the given content, so that a test that needs a
// file to diff can control exactly what its lines are.  Each file is stored in its own chunk, and the entries are
// encoded the same way BackupManager.UploadSnapshot writes them.  The file hashes the entries carry are returned.
func createTestSnapshotWithContent(manager *SnapshotManager, snapshotID string, revision int, startTime int64,
	endTime int64, fileNames []string, contents [][]byte, tag string) (fileHashes []string) {

	// One chunk per file, with the file's content as the chunk.
	chunkHashes := make([]string, len(fileNames))
	chunkLengths := make([]int, len(fileNames))
	fileHashes = make([]string, len(fileNames))

	for i, content := range contents {
		chunkHashes[i] = uploadTestChunk(manager, content)
		chunkLengths[i] = len(content)

		hasher := manager.config.NewFileHasher()
		hasher.Write(content)
		fileHashes[i] = hex.EncodeToString(hasher.Sum(nil))
	}

	buffer := new(bytes.Buffer)
	encoder := msgpack.NewEncoder(buffer)

	var snapshotChunkHashes []string
	var snapshotChunkLengths []int
	lastChunk := -1
	lastEndChunk := 0

	for i, fileName := range fileNames {

		entry := CreateEntry(fileName, int64(len(contents[i])), startTime, 0644)
		entry.Hash = fileHashes[i]
		entry.StartChunk = i
		entry.StartOffset = 0
		entry.EndChunk = i
		entry.EndOffset = len(contents[i])

		// This mirrors the way UploadSnapshot rewrites the chunk indexes: the chunk indexes of an entry are relative
		// to the previous entry, and each entry only refers to the chunks that weren't referenced before it.
		delta := entry.StartChunk - len(snapshotChunkHashes) + 1
		if entry.StartChunk != lastChunk {
			snapshotChunkHashes = append(snapshotChunkHashes, chunkHashes[entry.StartChunk])
			snapshotChunkLengths = append(snapshotChunkLengths, chunkLengths[entry.StartChunk])
			delta--
		}
		for chunk := entry.StartChunk + 1; chunk <= entry.EndChunk; chunk++ {
			snapshotChunkHashes = append(snapshotChunkHashes, chunkHashes[chunk])
			snapshotChunkLengths = append(snapshotChunkLengths, chunkLengths[chunk])
		}

		lastChunk = entry.EndChunk
		entry.StartChunk -= delta
		entry.EndChunk -= delta

		delta = entry.EndChunk - entry.StartChunk
		entry.StartChunk -= lastEndChunk
		lastEndChunk = entry.EndChunk
		entry.EndChunk = delta

		if err := encoder.Encode(entry); err != nil {
			LOG_ERROR("SNAPSHOT_UPLOAD", "Failed to encode the entry %s: %v", fileName, err)
			return nil
		}
	}

	var totalFileSize int64
	for _, content := range contents {
		totalFileSize += int64(len(content))
	}

	snapshot := &Snapshot{
		Version:       1,
		ID:            snapshotID,
		Revision:      revision,
		StartTime:     startTime,
		EndTime:       endTime,
		Tag:           tag,
		FileSize:      totalFileSize,
		NumberOfFiles: int64(len(fileNames)),
		ChunkHashes:   snapshotChunkHashes,
		ChunkLengths:  snapshotChunkLengths,
	}

	snapshot.FileSequence = []string{uploadTestMetadataChunk(manager, buffer.Bytes())}

	for _, sequenceType := range []string{"chunks", "lengths"} {
		content, err := snapshot.MarshalSequence(sequenceType)
		if err != nil {
			LOG_ERROR("SNAPSHOT_MARSHAL", "Failed to encode the %s in the snapshot: %v", sequenceType, err)
			return nil
		}
		snapshot.SetSequence(sequenceType, []string{uploadTestMetadataChunk(manager, content)})
	}

	description, _ := snapshot.MarshalJSON()
	path := snapshotPath(snapshotID, revision)
	manager.UploadFile(path, path, description)

	return fileHashes
}

func checkTestSnapshots(manager *SnapshotManager, expectedSnapshots int, expectedFossils int) {

	manager.CreateChunkOperator(false, false, 1, false)
	defer func() {
		manager.chunkOperator.Stop()
		manager.chunkOperator = nil
	}()

	var snapshotIDs []string
	var err error

	chunks := make(map[string]bool)
	files, _ := manager.ListAllFiles(manager.storage, "chunks/")
	for _, file := range files {
		if file[len(file)-1] == '/' {
			continue
		}
		chunk := chunkIDFromListedPath(file)
		chunks[chunk] = false
	}

	snapshotIDs, err = manager.ListSnapshotIDs()
	if err != nil {
		LOG_ERROR("SNAPSHOT_LIST", "Failed to list all snapshots: %v", err)
		return
	}

	numberOfSnapshots := 0

	for _, snapshotID := range snapshotIDs {

		revisions, err := manager.ListSnapshotRevisions(snapshotID)
		if err != nil {
			LOG_ERROR("SNAPSHOT_LIST", "Failed to list all revisions for snapshot %s: %v", snapshotID, err)
			return
		}

		for _, revision := range revisions {
			snapshot := manager.DownloadSnapshot(snapshotID, revision)
			numberOfSnapshots++

			for _, chunk := range manager.GetSnapshotChunks(snapshot, false) {
				chunks[chunk] = true
			}
		}
	}

	numberOfFossils := 0
	for chunk, referenced := range chunks {
		if !referenced {
			LOG_INFO("UNREFERENCED_CHUNK", "Unreferenced chunk %s", chunk)
			numberOfFossils++
		}
	}

	if numberOfSnapshots != expectedSnapshots {
		LOG_ERROR("SNAPSHOT_COUNT", "Expecting %d snapshots, got %d instead", expectedSnapshots, numberOfSnapshots)
	}

	if numberOfFossils != expectedFossils {
		LOG_ERROR("FOSSIL_COUNT", "Expecting %d unreferenced chunks, got %d instead", expectedFossils, numberOfFossils)
	}
}

// The snapshot cache is only useful for storages that need one; for a local file storage the cached snapshot files
// are never read back, so downloading a snapshot must not write them.
func TestDownloadSnapshotCache(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	storage := snapshotManager.storage.(*FileStorage)

	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3600, now, []string{chunkHash}, "tag")

	cachedSnapshotPath := path.Join(snapshotManager.snapshotCache.storageDir, "snapshots", "vm1@host1", "1")

	if _, err := os.Stat(cachedSnapshotPath); !os.IsNotExist(err) {
		t.Errorf("The snapshot file should not be added to the cache when the storage doesn't need a cache")
	}

	snapshot := snapshotManager.DownloadSnapshot("vm1@host1", 1)
	if snapshot == nil || snapshot.ID != "vm1@host1" || snapshot.Revision != 1 {
		t.Errorf("Failed to download the snapshot vm1@host1 at revision 1")
		return
	}

	if _, err := os.Stat(cachedSnapshotPath); !os.IsNotExist(err) {
		t.Errorf("Downloading a snapshot should not add it to the cache when the storage doesn't need a cache")
	}

	// A storage that needs a cache must still get a copy of every downloaded snapshot file.
	storage.isCacheNeeded = true

	snapshot = snapshotManager.DownloadSnapshot("vm1@host1", 1)
	if snapshot == nil || snapshot.ID != "vm1@host1" || snapshot.Revision != 1 {
		t.Errorf("Failed to download the snapshot vm1@host1 at revision 1")
		return
	}

	if _, err := os.Stat(cachedSnapshotPath); err != nil {
		t.Errorf("The snapshot file should be added to the cache when the storage needs a cache: %v", err)
	}
}

// capturedLog is a single message passed to one of the logging functions.
type capturedLog struct {
	level   int
	logID   string
	message string
}

// logCapture records the log messages produced while it is installed as LogFunction, so that a test can inspect what
// a command printed.
type logCapture struct {
	logs     []capturedLog
	logsLock sync.Mutex
}

func (capture *logCapture) log(level int, logID string, message string) {
	capture.logsLock.Lock()
	defer capture.logsLock.Unlock()
	capture.logs = append(capture.logs, capturedLog{level, logID, message})
}

// messages returns the messages that were logged under 'logID'.
func (capture *logCapture) messages(logID string) (messages []string) {
	capture.logsLock.Lock()
	defer capture.logsLock.Unlock()
	for _, log := range capture.logs {
		if log.logID == logID {
			messages = append(messages, log.message)
		}
	}
	return messages
}

// failures returns the messages that were logged as errors, since they are only recorded and never propagated while
// the capture is installed.
func (capture *logCapture) failures() (failures []string) {
	capture.logsLock.Lock()
	defer capture.logsLock.Unlock()
	for _, log := range capture.logs {
		if log.level >= ERROR {
			failures = append(failures, fmt.Sprintf("%s: %s", log.logID, log.message))
		}
	}
	return failures
}

// Listing snapshots obtains the revisions from ListSnapshotRevisions, which already enumerated the snapshot
// directory of the storage, so checking the existence of every revision again before downloading it only repeats an
// operation that was just performed (and costs a round trip per revision on cloud storages).  The existence check
// must still be performed for revisions that were specified by the user instead of being listed.
func TestDownloadSnapshotSkipsExistenceCheck(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	storage := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = storage

	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-7200, now-3600, []string{chunkHash}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-3600, now, []string{chunkHash}, "tag")

	// Listing the revisions of a snapshot id and downloading them must not check their existence individually.
	atomic.StoreInt64(&storage.getFileInfoCalls, 0)

	numberOfSnapshots := snapshotManager.ListSnapshots("vm1@host1", []int{}, "", false, false, 1)
	if numberOfSnapshots != 2 {
		t.Errorf("Expecting 2 snapshots, got %d instead", numberOfSnapshots)
	}
	if checks := atomic.LoadInt64(&storage.getFileInfoCalls); checks != 0 {
		t.Errorf("Listing the revisions should not check the existence of each snapshot, but %d checks were made",
			checks)
	}

	// A revision given by the user wasn't listed, so it is still checked against the storage before the download.
	atomic.StoreInt64(&storage.getFileInfoCalls, 0)

	numberOfSnapshots = snapshotManager.ListSnapshots("vm1@host1", []int{1}, "", false, false, 1)
	if numberOfSnapshots != 1 {
		t.Errorf("Expecting 1 snapshot, got %d instead", numberOfSnapshots)
	}
	if checks := atomic.LoadInt64(&storage.getFileInfoCalls); checks != 1 {
		t.Errorf("A revision specified by the user should be checked once, but %d checks were made", checks)
	}

	// The existence check must still be enforced for a revision that is no longer in the storage but whose file is
	// still in the snapshot cache, otherwise a deleted snapshot would remain readable.
	storage.FileStorage.isCacheNeeded = true

	createTestSnapshot(snapshotManager, "vm1@host1", 3, now, now+3600, []string{chunkHash}, "tag")

	cachedSnapshotPath := path.Join(snapshotManager.snapshotCache.storageDir, "snapshots", "vm1@host1", "3")
	if _, err := os.Stat(cachedSnapshotPath); err != nil {
		t.Errorf("Snapshot vm1@host1 at revision 3 should have been added to the cache: %v", err)
		return
	}

	// Delete it from the storage only; the cache keeps a stale copy.
	if err := os.Remove(path.Join(storage.storageDir, "snapshots", "vm1@host1", "3")); err != nil {
		t.Errorf("Failed to delete the snapshot from the storage: %v", err)
		return
	}

	if !downloadedSnapshotMissing(snapshotManager, "vm1@host1", 3) {
		t.Errorf("Downloading the snapshot vm1@host1 at revision 3 should report that it does not exist")
	}
}

// downloadedSnapshotMissing calls DownloadSnapshot and reports whether it reported that the snapshot does not exist,
// which is signaled by LOG_ERROR raising a SNAPSHOT_NOT_EXIST Exception.
func downloadedSnapshotMissing(manager *SnapshotManager, snapshotID string, revision int) (missing bool) {
	// A previous test may have left testingT set, which would turn the expected error into a test failure
	savedTestingT := testingT
	testingT = nil

	defer func() {
		testingT = savedTestingT
		if r := recover(); r != nil {
			if exception, ok := r.(Exception); ok && exception.LogID == "SNAPSHOT_NOT_EXIST" {
				missing = true
				return
			}
			panic(r)
		}
	}()

	manager.DownloadSnapshot(snapshotID, revision)
	return false
}

// Downloading the revisions concurrently must return the same snapshots as downloading them one at a time, in the
// same order, without the workers sharing the download buffer of the manager.
func TestDownloadSnapshotsConcurrently(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	for revision := 1; revision <= 8; revision++ {
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now, []string{chunkHash}, "tag")
	}

	revisions, err := snapshotManager.ListSnapshotRevisions("vm1@host1")
	if err != nil {
		t.Errorf("Failed to list the revisions: %v", err)
		return
	}
	if len(revisions) != 8 {
		t.Errorf("Expecting 8 revisions, got %d", len(revisions))
		return
	}

	// The revisions must come back in order, each carrying its own revision number, no matter how many workers
	// downloaded them and in which order they finished.
	for _, threads := range []int{1, 2, 4, 16} {
		counting.resetDownloadStats()

		snapshots := snapshotManager.downloadSnapshots("vm1@host1", revisions, true, threads)
		if len(snapshots) != len(revisions) {
			t.Errorf("With %d threads: expecting %d snapshots, got %d", threads, len(revisions), len(snapshots))
			continue
		}
		for i, revision := range revisions {
			snapshot := snapshots[i]
			if snapshot == nil {
				t.Errorf("With %d threads: the snapshot at revision %d was not downloaded", threads, revision)
				continue
			}
			if snapshot.Revision != revision {
				t.Errorf("With %d threads: expecting revision %d at position %d, got %d",
					threads, revision, i, snapshot.Revision)
			}
		}

		// Each snapshot is a separate download attributed to the worker that handled it, and the downloads must be
		// spread over the thread indexes the storage was told to expect -- but never beyond them, since some
		// backends index a per-thread client or nested directory with the thread index.
		usedThreads := counting.numberOfDownloadThreads()
		if usedThreads > threads {
			t.Errorf("With %d threads: the storage saw %d different thread indexes, more than the storage was created with",
				threads, usedThreads)
		}

		// The downloads must really overlap: a serial loop never has two of them in flight at the same time.
		peak := counting.peakConcurrentDownloads()
		if threads == 1 && peak != 1 {
			t.Errorf("With 1 thread: expecting at most one download at a time, got %d", peak)
		}
		if threads > 1 && peak < 2 {
			t.Errorf("With %d threads: the downloads did not overlap, at most %d was in flight at a time", threads, peak)
		}
	}

	// The number of snapshots reported by list must not depend on the number of threads either.
	if numberOfSnapshots := snapshotManager.ListSnapshots("vm1@host1", []int{}, "", false, false, 4); numberOfSnapshots != 8 {
		t.Errorf("Expecting 8 snapshots from a concurrent list, got %d", numberOfSnapshots)
	}
}

// Prune reads every snapshot file of every id before it can work out which chunks are unreferenced, and that loop used
// to download them one at a time however many threads were asked for.  The download reads must overlap when -threads is
// greater than 1, since on a storage where a read is a round trip this is the largest per-item cost of the command.
func TestPruneDownloadsRevisionsConcurrently(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	// Prune creates its storage with the number of threads it was given, so the storage under test is created the same
	// way, with four threads.
	threadedStorage, err := CreateFileStorage(testDir, false, 4)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	counting := &instrumentedStorage{FileStorage: threadedStorage, downloadDelay: time.Millisecond}
	snapshotManager.storage = counting

	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	for revision := 1; revision <= 8; revision++ {
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now, []string{chunkHash}, "tag")
	}

	// Nothing is being deleted, so prune stops after reading the snapshot files and the only downloads made are theirs.
	counting.resetDownloadStats()
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{},
		false, false, false, 4)

	// A serial loop never has two revisions in flight at the same time.
	if peak := counting.peakConcurrentDownloads(); peak < 2 {
		t.Errorf("Prune did not download the revisions concurrently, at most %d was in flight at a time", peak)
	}

	// The workers must stay within the thread indexes the storage was told to expect, since some backends index a
	// per-thread client or nested directory with the thread index.
	if usedThreads := counting.numberOfDownloadThreads(); usedThreads > 4 {
		t.Errorf("The storage saw %d different thread indexes, more than the 4 threads it was created with", usedThreads)
	}
}

// Check reads every snapshot file of every id before it can verify the chunks those revisions reference, and that loop
// used to download them one at a time however many threads were asked for -- the same defect prune had.  The download
// reads must overlap when -threads is greater than 1, since on a storage where a read is a round trip this is the
// largest per-item cost of the command's first phase.
func TestCheckDownloadsRevisionsConcurrently(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	// Check creates its storage with the number of threads it was given, so the storage under test is created the same
	// way, with four threads.
	threadedStorage, err := CreateFileStorage(testDir, false, 4)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	counting := &instrumentedStorage{FileStorage: threadedStorage, downloadDelay: time.Millisecond}
	snapshotManager.storage = counting

	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	for revision := 1; revision <= 8; revision++ {
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now, []string{chunkHash}, "tag")
	}

	// A plain check reads the snapshot files and then compares the referenced chunk ids against the chunk tree; the
	// per-revision reads are what must overlap, and the per-revision chunk-sequence expansions that follow are serial.
	counting.resetDownloadStats()
	if !snapshotManager.CheckSnapshots("vm1@host1", []int{}, "", false, false, false, false, false, false, false, 4,
		false) {
		t.Errorf("CheckSnapshots failed")
		return
	}

	// A serial loop never has two revisions in flight at the same time.
	if peak := counting.peakConcurrentDownloads(); peak < 2 {
		t.Errorf("Check did not download the revisions concurrently, at most %d was in flight at a time", peak)
	}

	// The workers must stay within the thread indexes the storage was told to expect, since some backends index a
	// per-thread client or nested directory with the thread index.
	if usedThreads := counting.numberOfDownloadThreads(); usedThreads > 4 {
		t.Errorf("The storage saw %d different thread indexes, more than the 4 threads it was created with", usedThreads)
	}
}

// checkSnapshotsProbeFixture builds a storage holding 'numberOfChunks' chunks, so that the chunk tree fills one
// directory per chunk and a test can put the referenced set on either side of the directory count the check weighs it
// against.  It returns the manager, the storage double and the hashes of the uploaded chunks, in the order uploaded.
func checkSnapshotsProbeFixture(t *testing.T, testDir string,
	numberOfChunks int) (*SnapshotManager, *instrumentedStorage, []string) {

	snapshotManager := createTestSnapshotManager(testDir)

	storage, err := CreateFileStorage(testDir, false, 4)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return snapshotManager, nil, nil
	}
	counting := &instrumentedStorage{FileStorage: storage}
	snapshotManager.storage = counting

	chunkHashes := make([]string, 0, numberOfChunks)
	for i := 0; i < numberOfChunks; i++ {
		chunkHash := uploadRandomChunk(snapshotManager, 1024)
		if chunkHash == "" {
			t.Errorf("Failed to upload a chunk")
			return snapshotManager, counting, nil
		}
		chunkHashes = append(chunkHashes, chunkHash)
	}

	return snapshotManager, counting, chunkHashes
}

// checkSnapshotsFailed runs a check and reports whether it failed.  A failed check raises the exception LOG_ERROR
// panics with, so the failure is caught here rather than reported by the test's own recovering handler; the logging is
// detached while it runs so that the expected LOG_ERROR is not also recorded against the test.
func checkSnapshotsFailed(snapshotManager *SnapshotManager, revisions []int) (failed bool) {
	recovered := recoverPanicFrom(func() {
		if !snapshotManager.CheckSnapshots("vm1@host1", revisions, "", false, false, false, false, false, false, false,
			1, false) {
			failed = true
		}
	})
	return failed || recovered != nil
}

// check -r N reads the named revisions and then answers "does this chunk exist" for the chunks those revisions
// reference.  Listing the whole chunk tree answers that for every chunk at the cost of one listing per chunk directory,
// which is the cost a restricted check should not have to pay when the tree is far larger than the referenced set.
func TestCheckListsTheChunkTreeOnlyWhenItIsCheaper(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager, counting, chunkHashes := checkSnapshotsProbeFixture(t, testDir, 16)
	if counting == nil {
		return
	}

	// Every revision references one of the uploaded chunks, so a check of one revision references that chunk and the
	// metadata chunk holding its sequence -- two chunks against a tree of one directory per uploaded chunk.
	now := time.Now().Unix()
	for revision := 1; revision <= 16; revision++ {
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{chunkHashes[revision-1]}, "tag")
	}

	counting.resetCounters()
	if checkSnapshotsFailed(snapshotManager, []int{1}) {
		t.Errorf("The restricted check failed")
		return
	}
	if listings := atomic.LoadInt64(&counting.chunkListings); listings != 0 {
		t.Errorf("The restricted check listed %d chunk directories; a tree larger than the referenced set is probed",
			listings)
	}

	// A check of every revision references every chunk, so the tree is never larger than the referenced set and
	// walking it is what the existence check and the total chunk size are then reported over.
	counting.resetCounters()
	if checkSnapshotsFailed(snapshotManager, []int{}) {
		t.Errorf("The unrestricted check failed")
		return
	}
	if listings := atomic.LoadInt64(&counting.chunkListings); listings == 0 {
		t.Errorf("The unrestricted check probed the chunks instead of listing the tree")
	}
}

// 'check -files' used to walk the file sequence twice per revision: once to sanity-check the entries, and once to
// collect the files it hashes.  Each walk re-reads and re-decodes every entry, fetching every metadata chunk of the
// sequence again -- from the storage the first time and, when the storage has a cache, from the cache the second.  The
// file list must instead be checked and collected in a single walk, which is what 'list -files' already does.
func TestCheckFilesWalksTheFileSequenceOnce(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	fileNames := []string{"file1", "file2", "file3", "file4"}
	fileSizes := []int64{9, 1234, 12345, 10}

	now := time.Now().Unix()
	createTestSnapshotWithFiles(snapshotManager, "vm1@host1", 1, now-3600, now, fileNames, fileSizes, "tag")

	// Capture what the check logs instead of letting it go to the test log.
	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	counting.resetCounters()
	counting.resetDownloadStats()

	// A LOG_ERROR would be recorded by the capture rather than raised, so the check runs directly.
	if !snapshotManager.CheckSnapshots("vm1@host1", []int{1}, "", false, false, true, false, false, false, false, 1,
		false) {
		t.Errorf("The file check failed")
		return
	}
	if failures := capture.failures(); len(failures) > 0 {
		t.Errorf("Checking the files of the snapshot failed: %v", failures)
		return
	}

	// The file, chunk and length sequences and the content of each file are fetched once each, and never a second
	// time.  A chunk that is fetched again is served from the snapshot cache rather than from the storage, so the
	// download counts alone would not show it; the cache hits below are what the second walk leaves behind.
	downloads := counting.chunkDownloadCounts()
	if len(downloads) != len(fileNames)+3 {
		t.Errorf("Expecting the 3 sequences and the content of the %d files to be downloaded once, got %v",
			len(fileNames), downloads)
	}
	for chunkPath, count := range downloads {
		if count != 1 {
			t.Errorf("The chunk %s was downloaded %d times instead of once", chunkPath, count)
		}
	}

	// The file sequence is the only sequence whose chunks would be fetched a second time, and the second fetch is the
	// cache hit.  Any of them means the sequence was walked twice.
	if hits := capture.messages("CHUNK_CACHE"); len(hits) != 0 {
		t.Errorf("The file sequence was walked twice: %d chunks were read from the cache again", len(hits))
	}

	// The single walk must still verify every file: the hashes of the collected files are checked as before.
	verified := capture.messages("SNAPSHOT_VERIFY")
	success := "All files in snapshot vm1@host1 at revision 1 have been successfully verified"
	if len(verified) == 0 || verified[len(verified)-1] != success {
		t.Errorf("Expecting the files to be reported as verified, got %v", verified)
	}
	if corrupted := capture.messages("SNAPSHOT_HASH"); len(corrupted) != 0 {
		t.Errorf("No file should have a mismatched hash, got %v", corrupted)
	}
}

// A probed chunk whose size the check does not record would be reported as missing, so the probe has to fill in the
// size of every chunk it finds -- and report the ones stored with a size of 0, exactly as the whole-tree listing does.
func TestCheckProbesReportTheChunkSizes(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager, counting, chunkHashes := checkSnapshotsProbeFixture(t, testDir, 3)
	if counting == nil {
		return
	}

	now := time.Now().Unix()
	for revision := 1; revision <= 3; revision++ {
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{chunkHashes[revision-1]}, "tag")
	}

	// Each revision references one uploaded chunk plus the metadata chunk holding its sequence, so a check of one
	// revision probes two chunks and must succeed.
	if checkSnapshotsFailed(snapshotManager, []int{1}) {
		t.Errorf("A check over chunks that all exist failed")
		return
	}

	// Empty one of the referenced chunks: the probe must see the size of 0, which the whole-tree listing would also
	// have seen, and the check must fail on it.
	chunkPath, exist, _, err := counting.FileStorage.FindChunk(0,
		snapshotManager.config.GetChunkIDFromHash(chunkHashes[1]), false)
	if err != nil || !exist {
		t.Errorf("Failed to find the chunk to empty: %v", err)
		return
	}
	if err := os.Truncate(path.Join(testDir, chunkPath), 0); err != nil {
		t.Errorf("Failed to empty the chunk: %v", err)
		return
	}

	if !checkSnapshotsFailed(snapshotManager, []int{2}) {
		t.Errorf("A check of a chunk stored with a size of 0 succeeded")
	}
}

// createPruneDeletionFixture creates 'revisions' snapshots, each referring to its own chunk, and returns a manager whose
// storage tracks snapshot-file deletions.
func createPruneDeletionFixture(t *testing.T, testDir string, revisions int) (*SnapshotManager, *instrumentedStorage) {

	snapshotManager := createTestSnapshotManager(testDir)

	threadedStorage, err := CreateFileStorage(testDir, false, revisions)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return snapshotManager, nil
	}
	tracking := &instrumentedStorage{
		FileStorage: threadedStorage,
		entered:     make(map[string]chan struct{}),
		gates:       make(map[string]chan struct{}),
		failPaths:   make(map[string]bool),
	}
	snapshotManager.storage = tracking

	now := time.Now().Unix()
	for revision := 1; revision <= revisions; revision++ {
		chunkHash := uploadRandomChunk(snapshotManager, 1024)
		if chunkHash == "" {
			t.Errorf("Failed to upload a chunk")
			return snapshotManager, tracking
		}
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now, []string{chunkHash}, "tag")
	}

	return snapshotManager, tracking
}

// snapshotPathFor returns the storage path of the snapshot file of one revision.
func snapshotPathFor(revision int) string {
	return fmt.Sprintf("snapshots/vm1@host1/%d", revision)
}

// Both the reading and the writing side derive the path of a snapshot file from snapshotDir and snapshotPath, so the
// storage layout is defined once.  This pins that layout: the snapshot file of a revision sits directly in the
// directory named after the snapshot id, and the directory carries the trailing slash the listings expect.
func TestSnapshotPathHelpers(t *testing.T) {

	if dir := snapshotDir("vm1@host1"); dir != "snapshots/vm1@host1/" {
		t.Errorf("snapshotDir returned %q instead of %q", dir, "snapshots/vm1@host1/")
	}
	if file := snapshotPath("vm1@host1", 3); file != "snapshots/vm1@host1/3" {
		t.Errorf("snapshotPath returned %q instead of %q", file, "snapshots/vm1@host1/3")
	}
}

// A chunk listed under 'chunks/' arrives as the nested path the storage lays it out in.  chunkIDFromListedPath has to
// recover the 64-character id from it, and it has to leave the '.fsl' suffix of a fossil alone: the suffix is what
// tells a caller that the entry is a fossil rather than a chunk.
func TestChunkIDFromListedPath(t *testing.T) {

	chunkID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	// The nesting directories are the first bytes of the id itself, so removing the separators has to return the id
	// unchanged.
	nestedPath := chunkID[:2] + "/" + chunkID[2:4] + "/" + chunkID[4:]

	if id := chunkIDFromListedPath(nestedPath); id != chunkID {
		t.Errorf("chunkIDFromListedPath returned %q instead of %q", id, chunkID)
	}
	if fossil := chunkIDFromListedPath(nestedPath + ".fsl"); fossil != chunkID+".fsl" {
		t.Errorf("chunkIDFromListedPath returned %q instead of %q for a fossil", fossil, chunkID+".fsl")
	}
}

// The three metadata sequences are what 'which chunks does this snapshot reference' means, and the chunk-id walk
// depends on seeing all of them.  MetadataSequences is the one definition of that set.
func TestMetadataSequences(t *testing.T) {

	snapshot := &Snapshot{
		FileSequence:   []string{"file"},
		ChunkSequence:  []string{"chunks"},
		LengthSequence: []string{"lengths"},
	}

	sequences := snapshot.MetadataSequences()
	if len(sequences) != 3 {
		t.Errorf("MetadataSequences returned %d sequences instead of 3", len(sequences))
		return
	}
	for i, expected := range []string{"file", "chunks", "lengths"} {
		if len(sequences[i]) != 1 || sequences[i][0] != expected {
			t.Errorf("MetadataSequences returned %v at index %d instead of the %s sequence", sequences[i], i, expected)
		}
	}
}

// PruneSnapshots deletes the snapshot files of the removed revisions one at a time, so -threads bought nothing for a
// storage where a delete is a round trip.  The deletions are independent, so they must overlap when more than one
// thread was asked for, and each worker must stay within the thread count the storage was created with.
func TestPruneDeletesSnapshotsConcurrently(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	for _, threads := range []int{1, 4} {

		testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

		snapshotManager, tracking := createPruneDeletionFixture(t, testDir, 8)
		if tracking == nil {
			return
		}

		snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1, 2, 3, 4, 5, 6, 7, 8}, []string{}, []string{},
			false, true, []string{}, false, false, false, threads)

		if deleted := len(tracking.finishedDeletes()); deleted != 8 {
			t.Errorf("With %d threads: expecting 8 snapshot files to be deleted, got %d", threads, deleted)
			return
		}

		// A serial loop never has two deletions in flight at the same time.
		peak := tracking.peakConcurrentDeletes()
		if threads == 1 && peak != 1 {
			t.Errorf("With 1 thread: expecting at most one deletion at a time, got %d", peak)
		}
		if threads > 1 && peak < 2 {
			t.Errorf("With %d threads: the snapshot deletions did not overlap, at most %d was in flight at a time",
				threads, peak)
		}

		if usedThreads := tracking.numberOfDeleteThreads(); usedThreads > threads {
			t.Errorf("With %d threads: the deletions used %d different thread indexes, more than the storage accepts",
				threads, usedThreads)
		}
	}
}

// The lines of the deletion loop are user-visible, so they must stay in revision order however the deletions
// themselves finish.  The deletions here are held until the test releases them, newest first, so a loop that logged
// as each deletion finished would print them in reverse order.
func TestPruneSnapshotDeletionOutputStaysInRevisionOrder(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	const revisions = 4
	snapshotManager, tracking := createPruneDeletionFixture(t, testDir, revisions)
	if tracking == nil {
		return
	}

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	// Hold every deletion, and let the test release the newest revision first.
	entered := make(map[string]chan struct{})
	gates := make(map[string]chan struct{})
	for revision := 1; revision <= revisions; revision++ {
		snapshotPath := snapshotPathFor(revision)
		entered[snapshotPath] = make(chan struct{}, 1)
		gates[snapshotPath] = make(chan struct{})
	}
	tracking.entered = entered
	tracking.gates = gates

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1, 2, 3, 4}, []string{}, []string{},
			false, true, []string{}, false, false, false, revisions)
	}()

	// Wait until every deletion is in flight, so releasing them controls the order they finish in.  A serial loop can
	// never get this far, which is the point: the order can only be checked against deletions that raced.
	for revision := 1; revision <= revisions; revision++ {
		select {
		case <-entered[snapshotPathFor(revision)]:
		case <-time.After(10 * time.Second):
			t.Errorf("The deletions did not overlap: revision %d was still waiting while the others were held", revision)
			return
		}
	}

	// Release the newest revision first and wait for its deletion to finish before releasing the next one, so the
	// deletions finish in exactly the reverse of revision order.
	for revision := revisions; revision >= 1; revision-- {
		close(gates[snapshotPathFor(revision)])
		expected := revisions - revision + 1
		deadline := time.Now().Add(10 * time.Second)
		for len(tracking.finishedDeletes()) < expected {
			if time.Now().After(deadline) {
				t.Errorf("The deletion of revision %d never finished", revision)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Errorf("Prune did not finish after the deletions were released")
		return
	}

	// The deletions finished newest first, but the log must still be oldest first.
	if finishedDeletes := tracking.finishedDeletes(); len(finishedDeletes) != revisions ||
		finishedDeletes[0] != snapshotPathFor(revisions) {
		t.Errorf("Expecting the deletions to finish newest first, got %v", finishedDeletes)
	}

	messages := capture.messages("SNAPSHOT_DELETE")
	var removals []string
	for _, message := range messages {
		if strings.Contains(message, "has been removed") {
			removals = append(removals, message)
		}
	}
	if len(removals) != revisions {
		t.Errorf("Expecting %d removal messages, got %d: %v", revisions, len(removals), removals)
		return
	}
	for i, message := range removals {
		expected := fmt.Sprintf("The snapshot vm1@host1 at revision %d has been removed", i+1)
		if message != expected {
			t.Errorf("Expecting %q at position %d, got %q", expected, i, message)
		}
	}
}

// The output must appear while the deletions are still running rather than being held back until the last one has
// finished.  Here revision 1 is released while the others are still held: its line must already be printed.
func TestPruneStreamsSnapshotDeletionOutput(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	const revisions = 4
	snapshotManager, tracking := createPruneDeletionFixture(t, testDir, revisions)
	if tracking == nil {
		return
	}

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	entered := make(map[string]chan struct{})
	gates := make(map[string]chan struct{})
	for revision := 1; revision <= revisions; revision++ {
		snapshotPath := snapshotPathFor(revision)
		entered[snapshotPath] = make(chan struct{}, 1)
		gates[snapshotPath] = make(chan struct{})
	}
	tracking.entered = entered
	tracking.gates = gates

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1, 2, 3, 4}, []string{}, []string{},
			false, true, []string{}, false, false, false, revisions)
	}()

	for revision := 1; revision <= revisions; revision++ {
		select {
		case <-entered[snapshotPathFor(revision)]:
		case <-time.After(10 * time.Second):
			t.Errorf("The deletions did not overlap: revision %d was still waiting while the others were held", revision)
			return
		}
	}

	// Release only the oldest revision.  Its line must be printed even though the other three are still held.
	close(gates[snapshotPathFor(1)])

	removalMessages := func() (removals []string) {
		for _, message := range capture.messages("SNAPSHOT_DELETE") {
			if strings.Contains(message, "has been removed") {
				removals = append(removals, message)
			}
		}
		return removals
	}

	expectedFirst := "The snapshot vm1@host1 at revision 1 has been removed"
	deadline := time.Now().Add(10 * time.Second)
	for {
		removals := removalMessages()
		if len(removals) > 0 {
			if removals[0] != expectedFirst {
				t.Errorf("Expecting %q first, got %q", expectedFirst, removals[0])
			}
			if len(removals) != 1 {
				t.Errorf("The output was not streamed: %d lines were printed before the other deletions finished: %v",
					len(removals), removals)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("The removal of revision 1 was not printed while the other deletions were still held")
			break
		}
		time.Sleep(time.Millisecond)
	}

	// Let the rest finish so that the command can return.
	for revision := 2; revision <= revisions; revision++ {
		close(gates[snapshotPathFor(revision)])
	}

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Errorf("Prune did not finish after the deletions were released")
		return
	}

	if removals := removalMessages(); len(removals) != revisions {
		t.Errorf("Expecting %d removal messages after the run, got %d: %v", revisions, len(removals), removals)
	}
}

// A deletion that fails must still stop the command, and it must do so at the same point the serial loop stopped: the
// revisions before the failure are reported, the failing revision is reported as an error, and the ones after it are
// not.  The error is raised from the calling goroutine, so a failing worker does not leave the command hung.
func TestPruneStopsAtTheFirstFailedSnapshotDeletion(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	const revisions = 4
	snapshotManager, tracking := createPruneDeletionFixture(t, testDir, revisions)
	if tracking == nil {
		return
	}

	// Revision 3 is the one whose deletion fails.
	tracking.failPaths[snapshotPathFor(3)] = true

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	// The capture installed above swallows the LOG_ERROR panic, so the command returns false instead of unwinding;
	// what matters is that it stopped and that the failure names the revision.
	succeeded := snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1, 2, 3, 4}, []string{}, []string{},
		false, true, []string{}, false, false, false, revisions)

	if succeeded {
		t.Errorf("A failed deletion should stop the command, but it reported success")
		return
	}
	expectedFailure := "Failed to delete the snapshot vm1@host1 at revision 3: injected deletion failure"
	failures := capture.failures()
	if len(failures) != 1 || !strings.Contains(failures[0], expectedFailure) {
		t.Errorf("Expecting one failure mentioning %q, got %v", expectedFailure, failures)
	}

	// The revisions before the failure are reported; the failing one and the one after it are not.
	var printed []string
	for _, message := range capture.messages("SNAPSHOT_DELETE") {
		if strings.Contains(message, "has been removed") {
			printed = append(printed, message)
		}
	}
	expectedPrinted := []string{
		"The snapshot vm1@host1 at revision 1 has been removed",
		"The snapshot vm1@host1 at revision 2 has been removed",
	}
	if len(printed) != len(expectedPrinted) {
		t.Errorf("Expecting %d removals before the failure, got %d: %v", len(expectedPrinted), len(printed), printed)
		return
	}
	for i, message := range printed {
		if message != expectedPrinted[i] {
			t.Errorf("Expecting %q at position %d, got %q", expectedPrinted[i], i, message)
		}
	}

	// The revisions after the failure were still deleted from the storage, since the workers are not stopped early,
	// but their files must not be reported as removed.
	if finished := tracking.finishedDeletes(); len(finished) != revisions {
		t.Errorf("Expecting all %d deletions to have been attempted, got %d: %v", revisions, len(finished), finished)
	}
}

// -exhaustive expands the chunk sequence of every revision it keeps, and each of those expansions is a set of metadata
// chunk downloads -- one round trip per chunk on a storage where a read is a round trip.  The expansions used to be done
// one revision at a time on the calling goroutine, so -threads did nothing for them; they must now overlap, and the
// chunk list of each revision must be the same one the serial expansion produced.
func TestPruneExpandsSequencesConcurrently(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	threadedStorage, err := CreateFileStorage(testDir, false, 4)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	counting := &instrumentedStorage{FileStorage: threadedStorage, downloadDelay: time.Millisecond}
	snapshotManager.storage = counting

	// No snapshot cache, so that every metadata chunk of a sequence is fetched from the storage; otherwise the second
	// expansion would be served from the cache the first one filled and the downloads would not be visible here.
	snapshotManager.snapshotCache = nil

	// Every revision refers to its own set of file chunks, so every revision has its own chunk sequence, and the
	// sequences all share the single metadata chunk that holds the file list.
	now := time.Now().Unix()
	var expectedChunks []map[string]bool
	for revision := 1; revision <= 8; revision++ {
		chunkHash := uploadRandomChunk(snapshotManager, 1024)
		if chunkHash == "" {
			t.Errorf("Failed to upload a chunk")
			return
		}
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{chunkHash}, "tag")
	}

	// What the serial expansion produces, taken before any concurrent run so that both go through the same code path.
	revisions, err := snapshotManager.ListSnapshotRevisions("vm1@host1")
	if err != nil {
		t.Errorf("Failed to list the revisions: %v", err)
		return
	}

	var snapshots []*Snapshot
	for _, revision := range revisions {
		snapshots = append(snapshots, snapshotManager.DownloadSnapshot("vm1@host1", revision))
	}

	// Prune creates the chunk operator with the thread count it was given before it selects any chunks, so the storage
	// and the operator are set up the same way here.
	snapshotManager.CreateChunkOperator(false, false, 4, false)
	defer func() {
		snapshotManager.chunkOperator.Stop()
		snapshotManager.chunkOperator = nil
	}()

	serialChunks := snapshotManager.expandSnapshots(snapshots, 1)
	for _, chunks := range serialChunks {
		if chunks == nil {
			t.Errorf("The serial expansion returned no chunks for a snapshot")
			return
		}
		chunkSet := make(map[string]bool)
		for _, chunk := range chunks {
			chunkSet[chunk] = true
		}
		expectedChunks = append(expectedChunks, chunkSet)
	}

	// The expansion must happen on the metadata chunks, which is what the storage download count sees.
	counting.resetDownloadStats()
	concurrentChunks := snapshotManager.expandSnapshots(snapshots, 4)

	if len(concurrentChunks) != len(expectedChunks) {
		t.Errorf("Expecting %d chunk lists, got %d", len(expectedChunks), len(concurrentChunks))
		return
	}

	for i, chunkSet := range expectedChunks {
		got := make(map[string]bool)
		for _, chunk := range concurrentChunks[i] {
			got[chunk] = true
		}
		if len(got) != len(chunkSet) {
			t.Errorf("Snapshot %d: expecting %d chunks, got %d", i, len(chunkSet), len(got))
			continue
		}
		for chunk := range chunkSet {
			if !got[chunk] {
				t.Errorf("Snapshot %d: the chunk %s is missing from the concurrent expansion", i, chunk)
			}
		}
	}

	// The metadata chunk downloads of the expansions must overlap: a serial expansion never has two in flight.
	if peak := counting.peakConcurrentDownloads(); peak < 2 {
		t.Errorf("The sequences were not expanded concurrently, at most %d was in flight at a time", peak)
	}
}

// A revision that did not change its file list or chunk list shares the chunk sequence of the revision before it; a run
// of such revisions stores one sequence and references it from all of them.  Expanding each of them separately would
// fetch that sequence once per revision -- concurrently, so the snapshot cache would not spare it -- which is exactly
// what the cache used to absorb.  A shared sequence must therefore be expanded once, however many threads were asked for.
func TestSharedSequenceIsExpandedOnce(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	threadedStorage, err := CreateFileStorage(testDir, false, 4)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	counting := &instrumentedStorage{FileStorage: threadedStorage, downloadDelay: time.Millisecond}
	snapshotManager.storage = counting

	// No snapshot cache: the cache would also fold the repeated reads of a shared sequence, so it has to be out of the
	// way for the download count to measure the grouping itself.
	snapshotManager.snapshotCache = nil

	// Every revision refers to the same file chunk, so every revision has the same chunk sequence: the file chunk, the
	// chunk sequence and the length sequence are all stored once and shared by all of them.
	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	for revision := 1; revision <= 8; revision++ {
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{chunkHash}, "tag")
	}

	revisions, err := snapshotManager.ListSnapshotRevisions("vm1@host1")
	if err != nil {
		t.Errorf("Failed to list the revisions: %v", err)
		return
	}

	var snapshots []*Snapshot
	for _, revision := range revisions {
		snapshots = append(snapshots, snapshotManager.DownloadSnapshot("vm1@host1", revision))
	}

	snapshotManager.CreateChunkOperator(false, false, 4, false)
	defer func() {
		snapshotManager.chunkOperator.Stop()
		snapshotManager.chunkOperator = nil
	}()

	counting.resetDownloadStats()
	chunkLists := snapshotManager.expandSnapshots(snapshots, 4)

	if len(chunkLists) != len(snapshots) {
		t.Errorf("Expecting %d chunk lists, got %d", len(snapshots), len(chunkLists))
		return
	}

	// Every revision must still be told about every chunk it references, even though only one expansion was done:
	// the chunk sequence chunk and the file chunk.
	for i, chunks := range chunkLists {
		if len(chunks) != 2 {
			t.Errorf("Snapshot %d: expecting 2 chunks, got %d", i, len(chunks))
		}
	}

	// The shared metadata chunk must have been fetched from the storage once, not once per revision.
	downloads := counting.chunkDownloadCounts()
	metadataDownloads := 0
	for filePath, count := range downloads {
		if count > 1 {
			t.Errorf("The chunk %s was downloaded %d times; a shared sequence must be expanded once", filePath, count)
		}
		metadataDownloads += count
	}

	if metadataDownloads != 1 {
		t.Errorf("Expecting the single shared metadata chunk to be downloaded once, got %d downloads", metadataDownloads)
	}
}

// A sequence must decode to exactly the same content however many chunks it has and however many threads fetch them:
// the pieces have to be put back in the order of the sequence, not in the order the downloads finish in.
func TestDownloadSequencePreservesOrderConcurrently(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	// One sequence of ten chunks, each with a distinctive content, so that a swapped pair is detectable.
	var sequence []string
	expected := make([]byte, 0)
	for i := 0; i < 10; i++ {
		piece := bytes.Repeat([]byte{byte('a' + i)}, 100)
		sequence = append(sequence, uploadTestChunk(snapshotManager, piece))
		expected = append(expected, piece...)
	}

	counting.resetDownloadStats()
	content := snapshotManager.DownloadSequence(sequence)

	if !bytes.Equal(content, expected) {
		t.Errorf("The sequence was decoded to %d bytes that do not match the %d bytes that were encoded",
			len(content), len(expected))
	}
}

// 'list -files' printed the file list by walking the file sequence twice: once to compute the total size and the width
// of the size column, and once to print the entries.  Each walk downloads every metadata chunk of the sequence again,
// so the second one is pure overhead (and a round trip per chunk on cloud storage).  The file list must instead be
// produced from a single walk, with the same output as before, and it must fetch only the metadata it reads.
func TestListFilesWalksTheFileSequenceOnce(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	// The entries are deliberately not in sorted order: the order they are printed in must be the order they are
	// stored in, which is how the backup writes them.
	fileNames := []string{"file1", "file2", "file10", "dir1/file3"}
	fileSizes := []int64{9, 1234, 12345678, 10}

	now := time.Now().Unix()
	fileHashes := createTestSnapshotWithFiles(snapshotManager, "vm1@host1", 1, now-3600, now, fileNames, fileSizes, "tag")

	// Capture what list prints instead of letting it go to the test log.
	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	counting.resetDownloadStats()

	if numberOfSnapshots := snapshotManager.ListSnapshots("vm1@host1", []int{}, "", true, false, 1); numberOfSnapshots != 1 {
		t.Errorf("Expecting 1 snapshot, got %d", numberOfSnapshots)
	}

	if failures := capture.failures(); len(failures) > 0 {
		t.Errorf("Listing the files of the snapshot failed: %v", failures)
		return
	}

	// Printing the files reads the file sequence and the length sequence, and nothing else.  The chunk sequence is
	// only the list of chunk hashes that -chunks prints, so fetching it here would be one metadata chunk per
	// revision that this branch never reads.  A chunk that is fetched a second time is served from the snapshot cache
	// rather than from the storage, so the downloads and the cache hits are counted together; a second walk of the
	// file sequence would add a cache hit for its chunk, and fetching the chunk sequence would add one storage read.
	fetches := len(capture.messages("CHUNK_DOWNLOAD")) + len(capture.messages("CHUNK_CACHE"))
	if fetches != 2 {
		t.Errorf("Expecting the file and length sequences to be fetched once each, got %d fetches", fetches)
	}

	downloads := counting.chunkDownloadCounts()
	if len(downloads) != 2 {
		t.Errorf("Expecting the file and length sequences to be downloaded, got %v", downloads)
	}
	for chunkPath, count := range downloads {
		if count != 1 {
			t.Errorf("The metadata chunk %s was downloaded %d times instead of once", chunkPath, count)
		}
	}

	// The printed file list must contain every file, in the order the entries are stored, with the size column
	// widened to the largest file.  The width follows the rule used by list: it grows a digit at a time while a
	// larger size is found.
	maxSize := int64(9)
	maxSizeDigits := 1
	for _, fileSize := range fileSizes {
		if fileSize > maxSize {
			maxSize = maxSize*10 + 9
			maxSizeDigits++
		}
	}
	modifiedTime := time.Unix(now-3600, 0).Format("2006-01-02 15:04:05")

	files := capture.messages("SNAPSHOT_FILE")
	if len(files) != len(fileNames) {
		t.Errorf("Expecting %d files to be printed, got %d: %v", len(fileNames), len(files), files)
		return
	}

	for i, fileName := range fileNames {
		expectedFile := fmt.Sprintf("%*d %s %s %s", maxSizeDigits, fileSizes[i], modifiedTime, fileHashes[i], fileName)
		if files[i] != expectedFile {
			t.Errorf("Expecting the file %s to be printed as %q, got %q", fileName, expectedFile, files[i])
		}
	}

	// The statistics computed from the same walk must still be printed.
	stats := capture.messages("SNAPSHOT_STATS")
	if len(stats) != 2 {
		t.Errorf("Expecting the file count and the sizes to be printed, got %v", stats)
		return
	}
	if stats[0] != fmt.Sprintf("Files: %d", len(fileNames)) {
		t.Errorf("Expecting %q, got %q", fmt.Sprintf("Files: %d", len(fileNames)), stats[0])
	}
	expectedTotalSize := int64(9 + 1234 + 12345678 + 10)
	expectedStats := fmt.Sprintf("Total size: %d, file chunks: 4, metadata chunks: 3", expectedTotalSize)
	if stats[1] != expectedStats {
		t.Errorf("Expecting %q, got %q", expectedStats, stats[1])
	}
}

// 'list -files' no longer fetches the chunk hash sequence it does not read, but 'list -chunks' does need it: the
// chunk ids are printed from it.  This pins the other half of the change down, so that the sequence is not dropped
// for the one mode that reads it.
func TestListChunksStillFetchesTheChunkSequence(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	now := time.Now().Unix()
	createTestSnapshotWithFiles(snapshotManager, "vm1@host1", 1, now-3600, now,
		[]string{"file1", "file2"}, []int64{9, 1234}, "tag")

	counting.resetDownloadStats()

	if numberOfSnapshots := snapshotManager.ListSnapshots("vm1@host1", []int{}, "", true, true, 1); numberOfSnapshots != 1 {
		t.Errorf("Expecting 1 snapshot, got %d", numberOfSnapshots)
	}

	// The file, length and chunk sequences are all needed for -files -chunks, and each is fetched once.
	downloads := counting.chunkDownloadCounts()
	if len(downloads) != 3 {
		t.Errorf("Expecting the 3 metadata chunks of the snapshot to be downloaded, got %v", downloads)
	}
	for chunkPath, count := range downloads {
		if count != 1 {
			t.Errorf("The metadata chunk %s was downloaded %d times instead of once", chunkPath, count)
		}
	}
}

// 'history' reads a file's hash and size from the file entries of each revision, and Entry.check validates each entry
// against the length sequence; it never reads the chunk hash sequence, which only RetrieveFile indexes.  Expanding both
// sequences fetched one metadata chunk per revision that no printed line depended on.  This pins the fetch count to the
// file and length sequences, and checks that the revision line still carries the entry's hash and size.
func TestShowHistoryFetchesOnlyTheLengthSequence(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	now := time.Now().Unix()
	fileHashes := createTestSnapshotWithFiles(snapshotManager, "vm1@host1", 1, now-3600, now,
		[]string{"file1", "file2"}, []int64{9, 1234}, "tag")

	// Capture what history prints instead of letting it go to the test log.
	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	counting.resetDownloadStats()

	if !snapshotManager.ShowHistory(testDir, "vm1@host1", []int{1}, "file1", false, 1) {
		t.Errorf("Showing the history of the snapshot failed: %v", capture.failures())
		return
	}

	// Printing the file and length sequences is what history reads; the chunk sequence is what -chunks would print, so
	// expanding it here would be one metadata chunk per revision that nothing reads.  A chunk fetched a second time is
	// served from the snapshot cache, so downloads and cache hits are counted together.
	if fetches := len(capture.messages("CHUNK_DOWNLOAD")) + len(capture.messages("CHUNK_CACHE")); fetches != 2 {
		t.Errorf("Expecting the file and length sequences to be fetched once each, got %d fetches", fetches)
	}

	downloads := counting.chunkDownloadCounts()
	if len(downloads) != 2 {
		t.Errorf("Expecting the file and length sequences to be downloaded, got %v", downloads)
	}
	for chunkPath, count := range downloads {
		if count != 1 {
			t.Errorf("The metadata chunk %s was downloaded %d times instead of once", chunkPath, count)
		}
	}

	// The revision line must be the one the entry carries.  The current revision is left out because the file does
	// not exist under 'testDir', so it is reported as an empty line.
	modifiedTime := time.Unix(now-3600, 0).Format("2006-01-02 15:04:05")
	expected := fmt.Sprintf("%7d: %15d %s %64s %s", 1, int64(9), modifiedTime, fileHashes[0], "file1")
	lines := capture.messages("SNAPSHOT_HISTORY")
	if len(lines) != 2 || lines[0] != expected || lines[1] != "current:" {
		t.Errorf("Expecting the revision line %q and an empty current line, got %v", expected, lines)
	}
}

// 'history' sorts the revisions it was given before it prints them, and with the concurrent read that sort has to
// happen before the downloads: each downloaded snapshot is matched to the revision at the same index.  A revision list
// given out of order must therefore still be printed under its own revision number, not the number next to it in the
// sorted list.
func TestShowHistoryMatchesSnapshotsToSortedRevisions(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	now := time.Now().Unix()
	// The file grows with the revision, so a mislabelled line shows up as the wrong size.
	fileHashes := make([]string, 3)
	for revision := 1; revision <= 3; revision++ {
		hashes := createTestSnapshotWithFiles(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{"file1"}, []int64{int64(revision)}, "tag")
		fileHashes[revision-1] = hashes[0]
	}

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	// Revision 3 is given before revision 1, so the sort has to reorder both the revisions and the snapshots.
	if !snapshotManager.ShowHistory(testDir, "vm1@host1", []int{3, 1}, "file1", false, 2) {
		t.Errorf("Showing the history failed: %v", capture.failures())
		return
	}

	lines := capture.messages("SNAPSHOT_HISTORY")
	if len(lines) != 3 {
		t.Errorf("Expecting two revision lines and the current line, got %v", lines)
		return
	}

	for i, revision := range []int{1, 3} {
		size := int64(revision)
		expected := fmt.Sprintf("%7d: %15d", revision, size)
		if !strings.HasPrefix(lines[i], expected) ||
			!strings.Contains(lines[i], fileHashes[revision-1]) {
			t.Errorf("Expecting revision %d to be printed as %q with hash %s, got %q",
				revision, expected, fileHashes[revision-1], lines[i])
		}
	}
}

// Naming the revisions with -r used to check the existence of each one separately before downloading it, even though
// the snapshot directory is enumerated anyway to resolve them.  A single listing answers the question for every named
// revision, so no per-revision check is made.  A revision that is not in the listing must still be reported as missing
// rather than turning into a failed download.
func TestShowHistorySkipsExistenceCheckForNamedRevisions(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	now := time.Now().Unix()
	for revision := 1; revision <= 3; revision++ {
		createTestSnapshotWithFiles(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{"file1"}, []int64{9}, "tag")
	}

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	// Every named revision exists, so the listing replaces the per-revision checks.
	counting.resetCounters()
	if !snapshotManager.ShowHistory(testDir, "vm1@host1", []int{1, 2}, "file1", false, 1) {
		t.Errorf("Showing the history of the snapshot failed: %v", capture.failures())
		return
	}

	if checks := atomic.LoadInt64(&counting.snapshotInfoCalls); checks != 0 {
		t.Errorf("Naming revisions should not check the existence of each one, but %d checks were made", checks)
	}
	if listings := atomic.LoadInt64(&counting.snapshotListings); listings != 1 {
		t.Errorf("Expecting the snapshot directory to be listed once, got %d listings", listings)
	}

	// A revision that is not in the listing is still checked individually, which is what reports it as missing.  The
	// capture is removed first, because it would record the expected LOG_ERROR instead of raising it.
	LogFunction = savedLogFunction

	counting.resetCounters()
	if !showHistoryMissingRevision(snapshotManager, testDir, "vm1@host1", 999, "file1") {
		t.Errorf("A revision that is not in the listing should be reported as missing")
	}
	if checks := atomic.LoadInt64(&counting.snapshotInfoCalls); checks == 0 {
		t.Errorf("A revision that is not in the listing should still be checked individually")
	}
}

// showHistoryMissingRevision calls ShowHistory for a revision that does not exist and reports whether it was reported
// as missing, which is signaled by LOG_ERROR raising a SNAPSHOT_NOT_EXIST Exception.
func showHistoryMissingRevision(manager *SnapshotManager, top string, snapshotID string, revision int,
	filePath string) (missing bool) {
	// A previous test may have left testingT set, which would turn the expected error into a test failure
	savedTestingT := testingT
	testingT = nil

	defer func() {
		testingT = savedTestingT
		if r := recover(); r != nil {
			if exception, ok := r.(Exception); ok && exception.LogID == "SNAPSHOT_NOT_EXIST" {
				missing = true
				return
			}
			panic(r)
		}
	}()

	manager.ShowHistory(top, snapshotID, []int{revision}, filePath, false, 1)
	return false
}

// 'history' read the snapshot of every requested revision one at a time however many threads were asked for.  The
// snapshot files and the length sequences are independent per revision, so with more than one thread the reads must
// overlap; the printed lines must stay in revision order and be identical to the single-threaded run.
func TestShowHistoryReadsRevisionsConcurrently(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	// History creates its storage with the number of threads it was given, so the storage under test is created the
	// same way, with four threads.
	threadedStorage, err := CreateFileStorage(testDir, false, 4)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	counting := &instrumentedStorage{FileStorage: threadedStorage, downloadDelay: time.Millisecond}
	snapshotManager.storage = counting

	now := time.Now().Unix()
	for revision := 1; revision <= 8; revision++ {
		createTestSnapshotWithFiles(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{"file1"}, []int64{9}, "tag")
	}

	savedLogFunction := LogFunction
	defer func() {
		LogFunction = savedLogFunction
	}()

	var singleThreadedLines []string

	for _, threads := range []int{1, 4} {
		capture := &logCapture{}
		LogFunction = capture.log
		counting.resetDownloadStats()

		if !snapshotManager.ShowHistory(testDir, "vm1@host1", nil, "file1", false, threads) {
			t.Errorf("With %d threads: showing the history failed: %v", threads, capture.failures())
			continue
		}

		lines := capture.messages("SNAPSHOT_HISTORY")
		if len(lines) != 9 {
			t.Errorf("With %d threads: expecting 8 revision lines and the current line, got %d", threads, len(lines))
		}

		if threads == 1 {
			singleThreadedLines = lines
			// A serial loop never has two downloads in flight at the same time.
			if peak := counting.peakConcurrentDownloads(); peak != 1 {
				t.Errorf("With 1 thread: expecting at most one download at a time, got %d", peak)
			}
		} else {
			// A serial loop never has two revisions in flight at the same time.
			if peak := counting.peakConcurrentDownloads(); peak < 2 {
				t.Errorf("With %d threads: the revisions were not read concurrently, at most %d was in flight at a time",
					threads, peak)
			}

			// The workers must stay within the thread indexes the storage was told to expect, since some backends
			// index a per-thread client or nested directory with the thread index.
			if usedThreads := counting.numberOfDownloadThreads(); usedThreads > threads {
				t.Errorf("With %d threads: the storage saw %d different thread indexes, more than it was created with",
					threads, usedThreads)
			}

			if fmt.Sprintf("%v", lines) != fmt.Sprintf("%v", singleThreadedLines) {
				t.Errorf("The printed history depends on the thread count: %v vs %v", lines, singleThreadedLines)
			}
		}
	}
}

// Reading snapshots must never create directories.  The snapshot directory is created by the code that uploads a
// snapshot, so read-only commands (list, check, cat, ...) leave the storage and the snapshot cache untouched.
func TestReadSnapshotsDoesNotCreateDirectories(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	storageDir := snapshotManager.storage.(*FileStorage).storageDir
	cacheDir := snapshotManager.snapshotCache.storageDir

	nonexistentStorageDir := path.Join(storageDir, "snapshots", "vm1@host1")
	nonexistentCacheDir := path.Join(cacheDir, "snapshots", "vm1@host1")

	// Listing the revisions of a snapshot id that has never been backed up reports no revisions, and must not
	// create the snapshot directory on the storage or in the snapshot cache.
	revisions, err := snapshotManager.ListSnapshotRevisions("vm1@host1")
	if err != nil {
		t.Errorf("Failed to list the revisions of a nonexistent snapshot: %v", err)
		return
	}
	if len(revisions) != 0 {
		t.Errorf("Expecting no revisions for a nonexistent snapshot, got %d", len(revisions))
	}
	if _, err := os.Stat(nonexistentStorageDir); !os.IsNotExist(err) {
		t.Errorf("Listing the revisions should not create the snapshot directory in the storage")
	}
	if _, err := os.Stat(nonexistentCacheDir); !os.IsNotExist(err) {
		t.Errorf("Listing the revisions should not create the snapshot directory in the snapshot cache")
	}

	// Uploading a snapshot is what creates the snapshot directory.
	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3600, now, []string{chunkHash}, "tag")

	if _, err := os.Stat(nonexistentStorageDir); err != nil {
		t.Errorf("Uploading a snapshot should create the snapshot directory: %v", err)
	}

	// Downloading it back must again not touch the snapshot cache, since a file storage doesn't need a cache.
	snapshot := snapshotManager.DownloadSnapshot("vm1@host1", 1)
	if snapshot == nil || snapshot.ID != "vm1@host1" || snapshot.Revision != 1 {
		t.Errorf("Failed to download the snapshot vm1@host1 at revision 1")
		return
	}

	if _, err := os.Stat(nonexistentCacheDir); !os.IsNotExist(err) {
		t.Errorf("Downloading a snapshot should not create the snapshot directory in the snapshot cache")
	}
}

func TestPruneSingleRepository(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "repository1", 1, now-4*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "repository1", 2, now-4*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "repository1", 3, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "repository1", 4, now-1*day-3600, now-1*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 4, 0)

	t.Logf("Removing snapshot repository1 revisions 1 and 2 with --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{1, 2}, []string{}, []string{}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot repository1 revision 3 without --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{3}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 1, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "repository1", 5, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestPruneSingleHost(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "vm2@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm2@host1", 2, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 0)

}

func TestPruneMultipleHost(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshot")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "vm2@host2", 1, now-3*day-3600, now-3*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm2@host2", 2, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash6 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now+1*day-3600, now+1*day, []string{chunkHash5, chunkHash6}, "tag")
	checkTestSnapshots(snapshotManager, 4, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 4, 0)
}

func TestPruneAndResurrect(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 1, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 4, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash1}, "tag")
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- one fossil will be resurrected")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestPruneWithInactiveHost(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshot")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	// Host2 is inactive
	createTestSnapshot(snapshotManager, "vm2@host2", 1, now-7*day-3600, now-7*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 0)
}

func TestPruneWithRetentionPolicy(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	var chunkHashes []string
	for i := 0; i < 30; i++ {
		chunkHashes = append(chunkHashes, uploadRandomChunk(snapshotManager, chunkSize))
	}

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 30 snapshots")
	for i := 0; i < 30; i++ {
		createTestSnapshot(snapshotManager, "vm1@host1", i+1, now-int64(30-i)*day-3600, now-int64(30-i)*day-60, []string{chunkHashes[i]}, "tag")
	}

	checkTestSnapshots(snapshotManager, 30, 0)

	t.Logf("Removing snapshot vm1@host1 0:20 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"0:20"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 19, 0)

	t.Logf("Removing snapshot vm1@host1 -k 0:20 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"0:20"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 19, 0)

	t.Logf("Removing snapshot vm1@host1 -k 3:14 -k 2:7 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"3:14", "2:7"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 12, 0)
}

func TestPruneWithRetentionPolicyAndTag(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	var chunkHashes []string
	for i := 0; i < 30; i++ {
		chunkHashes = append(chunkHashes, uploadRandomChunk(snapshotManager, chunkSize))
	}

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 30 snapshots")
	for i := 0; i < 30; i++ {
		tag := "auto"
		if i%3 == 0 {
			tag = "manual"
		}
		createTestSnapshot(snapshotManager, "vm1@host1", i+1, now-int64(30-i)*day-3600, now-int64(30-i)*day-60, []string{chunkHashes[i]}, tag)
	}

	checkTestSnapshots(snapshotManager, 30, 0)

	t.Logf("Removing snapshot vm1@host1 0:20 with --exclusive and --tag manual")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{"manual"}, []string{"0:7"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 22, 0)
}

// Test that an unreferenced fossil shouldn't be removed as it may be the result of another prune job in-progress.
func TestPruneWithFossils(t *testing.T) {
	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	// Create an unreferenced fossil
	snapshotManager.storage.UploadFile(0, "chunks/113b6a2350dcfd836829c47304dd330fa6b58b93dd7ac696c6b7b913e6868662.fsl", []byte("this is a test fossil"))

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	checkTestSnapshots(snapshotManager, 2, 1)

	t.Logf("Prune without removing any snapshots but with --exhaustive")
	// The unreferenced fossil shouldn't be removed
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, true, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 1)

	t.Logf("Prune without removing any snapshots but with --exclusive")
	// Now the unreferenced fossil should be removed
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestPruneMultipleThread(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	numberOfChunks := 256
	numberOfThreads := 4

	chunkList1 := uploadRandomChunks(snapshotManager, chunkSize, numberOfChunks)
	chunkList2 := uploadRandomChunks(snapshotManager, chunkSize, numberOfChunks)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "repository1", 1, now-4*day-3600, now-3*day-60, chunkList1, "tag")
	createTestSnapshot(snapshotManager, "repository1", 2, now-3*day-3600, now-2*day-60, chunkList2, "tag")
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot revisions 1 with --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{1}, []string{}, []string{}, false, true, []string{}, false, false, false, numberOfThreads)
	checkTestSnapshots(snapshotManager, 1, 0)

	t.Logf("Creating 1 more snapshot")
	chunkList3 := uploadRandomChunks(snapshotManager, chunkSize, numberOfChunks)
	createTestSnapshot(snapshotManager, "repository1", 3, now-2*day-3600, now-1*day-60, chunkList3, "tag")

	t.Logf("Removing snapshot repository1 revision 2 without --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{2}, []string{}, []string{}, false, false, []string{}, false, false, false, numberOfThreads)

	t.Logf("Prune without removing any snapshots but with --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{}, []string{}, []string{}, false, true, []string{}, false, false, false, numberOfThreads)
	checkTestSnapshots(snapshotManager, 1, 0)
}

// Unlike the chunk cache, the cached snapshot files, fossil collections and verified-chunk list are read back
// without any check on the content, so those writes must stay durable -- which is why only the chunk cache is
// written through FileStorage.UploadFileNoSync.  This test records how each of the three behaves when its cache
// entry is torn, so that the reason the durable default was kept for them is not lost: a torn snapshot file or
// fossil collection is an error the command reports and stops on, while the verified-chunk list is only a record of
// work already done and is rebuilt by verifying again.
//
// The tears are made by truncating the cached files directly rather than by crashing, so this does not depend on
// how the files were written; it documents the reading side, which is what the no-fsync decision was based on.
func TestCorruptNonChunkCacheEntries(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "corrupt_nonchunk_cache_test")

	snapshotManager := createTestSnapshotManager(testDir)
	// A storage that needs a cache is the one whose snapshot files are cached and read back.
	snapshotManager.storage.(*FileStorage).isCacheNeeded = true

	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3600, now, []string{chunkHash}, "tag")

	cachedSnapshotPath := path.Join(snapshotManager.snapshotCache.storageDir, "snapshots", "vm1@host1", "1")
	if _, err := os.Stat(cachedSnapshotPath); err != nil {
		t.Errorf("The snapshot file was not added to the cache: %v", err)
		return
	}

	// Capture the log instead of letting an error fail the test through setTestingT.  Note that this also stops
	// LOG_ERROR from aborting the command the way it does in production, so a case that reports an error and then
	// returns continues to run here; that is what lets the test observe the failure and still clean up after it.
	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	// A snapshot that is still readable must come back without an error.
	if snapshot := snapshotManager.DownloadSnapshot("vm1@host1", 1); snapshot == nil {
		t.Errorf("Failed to download an intact cached snapshot")
		return
	}
	if failures := capture.failures(); len(failures) > 0 {
		t.Errorf("Reading an intact cached snapshot failed: %v", failures)
		return
	}

	// Truncate the cached snapshot file to simulate a write torn by a crash.  Nothing verifies it, so the parse is
	// what catches it; the failure must be reported, not papered over.
	if err := os.Truncate(cachedSnapshotPath, 20); err != nil {
		t.Errorf("Failed to truncate the cached snapshot %s: %v", cachedSnapshotPath, err)
		return
	}

	if snapshot := snapshotManager.DownloadSnapshot("vm1@host1", 1); snapshot != nil {
		t.Errorf("A torn cached snapshot file was accepted instead of reported")
	}
	parseFailures := capture.messages("SNAPSHOT_PARSE")
	if len(parseFailures) == 0 {
		t.Errorf("A torn cached snapshot file did not produce a SNAPSHOT_PARSE error")
	}

	// The entry is unusable, so remove it to give the rest of the test a clean cache; a later download then falls
	// back to the storage, which is the recovery a user gets in practice.
	if err := os.Remove(cachedSnapshotPath); err != nil {
		t.Errorf("Failed to remove the torn cached snapshot %s: %v", cachedSnapshotPath, err)
		return
	}

	// The same for a fossil collection, which prune reads back and parses before it acts on it.
	collectionPath := path.Join(snapshotManager.snapshotCache.storageDir, "fossils", "1")
	if err := os.MkdirAll(path.Dir(collectionPath), 0700); err != nil {
		t.Errorf("Failed to create the fossil collection directory: %v", err)
		return
	}
	if err := ioutil.WriteFile(collectionPath, []byte(`{"last_revisions":{},"deleted_revisions":{}`), 0600); err != nil {
		t.Errorf("Failed to write a torn fossil collection: %v", err)
		return
	}

	// prune must fail rather than proceed on a collection it could not read.
	if success := snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{},
		false, false, []string{}, false, false, false, 1); success {
		t.Errorf("PruneSnapshots succeeded despite a torn fossil collection")
	}
	// Match on the message, not just the log id: FOSSIL_COLLECT is also used for the "Fossil collection N found"
	// info line, so an id-only check would pass even without the failure.
	collectionFailures := 0
	for _, message := range capture.messages("FOSSIL_COLLECT") {
		if strings.Contains(message, "Failed to load the fossil collection file") {
			collectionFailures++
		}
	}
	if collectionFailures == 0 {
		t.Errorf("A torn fossil collection did not report that it could not be loaded")
	}

	// The verified-chunk list is the third cached file that is read back unverified.  Unlike the other two it is
	// only a cache of work already done, so 'check -chunks' recovers from a torn copy by verifying the chunks
	// again rather than failing; that is what keeps it harmless to lose.
	verifiedChunksPath := path.Join(snapshotManager.snapshotCache.storageDir, "verified_chunks")
	if err := ioutil.WriteFile(verifiedChunksPath, []byte(`{"aaaaaaaa":1}`), 0600); err != nil {
		t.Errorf("Failed to write the verified chunks file: %v", err)
		return
	}
	if err := os.Truncate(verifiedChunksPath, 5); err != nil {
		t.Errorf("Failed to truncate the verified chunks file %s: %v", verifiedChunksPath, err)
		return
	}

	// checkFiles=false, checkChunks=true: the verified chunks file is only read when chunks are being checked.
	if !snapshotManager.CheckSnapshots("vm1@host1", []int{1}, "", false, false, false, true,
		false, false, false, 1, false) {
		t.Errorf("CheckSnapshots failed because of a torn verified chunks file")
	}
	// As above, match the message rather than the log id: SNAPSHOT_VERIFY also carries info lines.
	parseWarnings := 0
	for _, message := range capture.messages("SNAPSHOT_VERIFY") {
		if strings.Contains(message, "Failed to parse the file containing verified chunks") {
			parseWarnings++
		}
	}
	if parseWarnings == 0 {
		t.Errorf("A torn verified chunks file did not report that it could not be parsed")
	}
}

// A snapshot not seen by a fossil collection should always be consider a new snapshot in the fossil deletion step
func TestPruneNewSnapshots(t *testing.T) {
	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "vm2@host1", 1, now-2*day-3600, now-2*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Prune snapshot 1")
	// chunkHash1 should be marked as fossil
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	// Create another snapshot of vm1 that brings back chunkHash1
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now-0*day-3600, now-0*day-60, []string{chunkHash1, chunkHash3}, "tag")
	// Create another snapshot of vm2 so the fossil collection will be processed by next prune
	createTestSnapshot(snapshotManager, "vm2@host1", 2, now+3600, now+3600*2, []string{chunkHash4, chunkHash5}, "tag")

	// Now chunkHash1 wil be resurrected
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 4, 0)
	snapshotManager.CheckSnapshots("vm1@host1", []int{2, 3}, "", false, false, false, false, false, false, false, 1, false)
}

// A fossil collection left by an aborted prune should be ignored if any supposedly deleted snapshot exists
func TestPruneGhostSnapshots(t *testing.T) {
	setTestingT(t)

	EnableStackTrace()

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	checkTestSnapshots(snapshotManager, 2, 0)

	snapshot1, err := ioutil.ReadFile(path.Join(testDir, "snapshots", "vm1@host1", "1"))
	if err != nil {
		t.Errorf("Failed to read snapshot file: %v", err)
	}

	t.Logf("Prune snapshot 1")
	// chunkHash1 should be marked as fossil
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 1, 2)

	// Recover the snapshot file for revision 1; this is to simulate a scenario where prune may encounter a network error after
	// leaving the fossil collection but before deleting any snapshots.
	err = ioutil.WriteFile(path.Join(testDir, "snapshots", "vm1@host1", "1"), snapshot1, 0644)
	if err != nil {
		t.Errorf("Failed to write snapshot file: %v", err)
	}

	// Create another snapshot of vm1 so the fossil collection becomes eligible for processing.
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now-day-3600, now-day-60, []string{chunkHash3, chunkHash4}, "tag")

	// Run the prune again but the fossil collection should be igored, since revision 1 still exists
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 2)
	snapshotManager.CheckSnapshots("vm1@host1", []int{1, 2, 3}, "", false, false, false, false, true /*searchFossils*/, false, false, 1, false)

	// Prune snapshot 1 again
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	// Create another snapshot
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 4, now+3600, now+3600*2, []string{chunkHash5, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	// Run the prune again and this time the fossil collection will be processed and the fossils removed
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 0)
	snapshotManager.CheckSnapshots("vm1@host1", []int{2, 3, 4}, "", false, false, false, false, false, false, false, 1, false)
}

// The chunks of a sequence are submitted all at once, so the thread count of the snapshot manager's operator -- and
// not DownloadSequence itself -- is what decides whether they overlap.  That is the property a command relies on when
// it creates the operator with the user's -threads before expanding, as check and prune do and restore now does too:
// the same sequence is fetched strictly serially through a one-thread operator and by several workers at once through
// an eight-thread one.
func TestDownloadSequencesOverlapUnderThreads(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	counting.downloadDelay = time.Millisecond
	snapshotManager.storage = counting

	// Ten chunks, each downloaded exactly once by the expansion that follows.
	var sequence []string
	for i := 0; i < 10; i++ {
		piece := bytes.Repeat([]byte{byte('a' + i)}, 100)
		sequence = append(sequence, uploadTestChunk(snapshotManager, piece))
	}

	stopOperator := func() {
		snapshotManager.chunkOperator.Stop()
		snapshotManager.chunkOperator = nil
	}

	// One thread: the downloads cannot overlap, so the peak is one.
	snapshotManager.CreateChunkOperator(false, false, 1, false)
	counting.resetDownloadStats()
	snapshotManager.DownloadSequence(sequence)
	if peak := counting.peakConcurrentDownloads(); peak != 1 {
		t.Errorf("Expected a single concurrent download at one thread, saw %d", peak)
	}
	stopOperator()

	// Eight threads: with the same sequence the downloads must overlap.
	snapshotManager.CreateChunkOperator(false, false, 8, false)
	counting.resetDownloadStats()
	content := snapshotManager.DownloadSequence(sequence)
	if peak := counting.peakConcurrentDownloads(); peak < 2 {
		t.Errorf("Expected the sequence downloads to overlap at eight threads, saw a peak of %d", peak)
	}
	if len(content) != 1000 {
		t.Errorf("The sequence decoded to %d bytes instead of 1000", len(content))
	}
	stopOperator()
}

// A snapshot file whose erasure-coded shard had to be reconstructed comes back from Decrypt asking for a rewrite, but
// backup and restore read the snapshot file before they create the chunk operator, so that field is nil when it
// happens.  Reading rewriteChunks through the nil operator used to crash; the guard makes the two commands skip the
// rewrite, exactly as list does, whose operator is created with rewriteChunks false.  An operator that does exist must
// still get the rewrite, or the guard would have disabled it everywhere rather than only where there is no operator.
func TestDownloadFileWithoutChunkOperatorDoesNotPanic(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	snapshotManager.config.DataShards = 5
	snapshotManager.config.ParityShards = 2

	storage := snapshotManager.storage.(*FileStorage)

	// Uploaded the same way the snapshot file is, so that it is compressed and erasure coded the way Decrypt expects.
	snapshotFile := snapshotPath("test", 1)
	content := bytes.Repeat([]byte("snapshot description"), 8)
	if !snapshotManager.UploadFile(snapshotFile, snapshotFile, content) {
		t.Errorf("Failed to upload the snapshot file")
		return
	}

	// Corrupt one data shard, so that it no longer matches its hash and Decrypt has to reconstruct it, which is what
	// makes it report rewriteNeeded.  The remaining data and parity shards are untouched, so there are still enough of
	// them to recover from.
	storedFile := path.Join(storage.storageDir, snapshotFile)
	stored, err := ioutil.ReadFile(storedFile)
	if err != nil {
		t.Errorf("Failed to read the stored snapshot file: %v", err)
		return
	}
	dataOffset := len(ERASURE_CODING_BANNER) + 14 + (snapshotManager.config.DataShards+snapshotManager.config.ParityShards)*32
	stored[dataOffset] ^= 0xff
	if err := ioutil.WriteFile(storedFile, stored, 0644); err != nil {
		t.Errorf("Failed to write the corrupted snapshot file: %v", err)
		return
	}

	// No chunk operator exists here, as it does not yet when backup and restore read the snapshot file.
	var downloaded []byte
	recovered := recoverPanicFrom(func() {
		downloaded = snapshotManager.DownloadFile(snapshotFile, snapshotFile)
	})
	if recovered != nil {
		t.Errorf("Downloading a snapshot file with a reconstructed shard without a chunk operator failed: %v", recovered)
		return
	}
	if !bytes.Equal(downloaded, content) {
		t.Errorf("The recovered snapshot file does not hold the original content")
	}

	// With an operator asking for rewrites, the reconstructed file must be uploaded back.
	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	snapshotManager.chunkOperator = CreateChunkOperator(snapshotManager.config, storage, nil, false, true, 1, false)
	defer func() {
		snapshotManager.chunkOperator.Stop()
		snapshotManager.chunkOperator = nil
	}()

	snapshotManager.DownloadFile(snapshotFile, snapshotFile)

	rewrites := 0
	for _, message := range capture.messages("DOWNLOAD_REWRITE") {
		if strings.Contains(message, "has been re-uploaded") {
			rewrites++
		}
	}
	if rewrites != 1 {
		t.Errorf("Expected the reconstructed snapshot file to be re-uploaded once, saw %d rewrites", rewrites)
	}
}
