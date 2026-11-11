// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

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
