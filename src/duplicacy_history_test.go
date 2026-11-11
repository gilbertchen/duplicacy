// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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
