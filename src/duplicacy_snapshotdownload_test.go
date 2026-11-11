// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack"
)

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
