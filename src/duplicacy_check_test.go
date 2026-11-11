// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"
)

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
		snapshotManager.config.GetChunkIDFromHash(chunkHashes[1]), false, false)
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
