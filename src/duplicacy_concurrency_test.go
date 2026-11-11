// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recoverPanicFrom runs 'f' with logging detached from the test and returns what it panicked with, if anything.  A
// LOG_ERROR raises an Exception, which with testingT set is also recorded as a test failure and with LogFunction set
// never reaches the panic at all; both are detached here so that a test can assert on an expected failure instead of
// failing on it.
func recoverPanicFrom(f func()) (recovered interface{}) {

	savedTestingT := testingT
	savedLogFunction := LogFunction

	testingT = nil
	LogFunction = nil

	defer func() {
		testingT = savedTestingT
		LogFunction = savedLogFunction
		recovered = recover()
	}()

	f()
	return nil
}

// When only one worker is needed the whole range must run on the calling goroutine, which is what lets a serial call
// stay the plain loop it was before and touch the caller's state without a synchronization barrier.
func TestRunConcurrentlySerial(t *testing.T) {

	var order []int
	var inFlight int64

	runConcurrently(1, 5, func(threadIndex, index int) {
		if threadIndex != 0 {
			t.Errorf("A serial run should use the thread index 0, got %d", threadIndex)
		}
		if current := atomic.AddInt64(&inFlight, 1); current != 1 {
			t.Errorf("A serial run should have one job in flight, got %d", current)
		}
		time.Sleep(time.Millisecond)
		order = append(order, index)
		atomic.AddInt64(&inFlight, -1)
	})

	if len(order) != 5 {
		t.Errorf("Expecting the 5 indices to be visited, got %v", order)
		return
	}
	for i, index := range order {
		if index != i {
			t.Errorf("A serial run should visit the indices in order, got %v", order)
			return
		}
	}
}

// With several workers every index must be visited exactly once, the work must overlap, and the thread index handed to
// the job must stay within the number of workers, since some storages index a per-thread client or directory with it.
func TestRunConcurrentlyVisitsEveryIndexOnce(t *testing.T) {

	const count = 64
	const threads = 4

	visits := make([]int64, count)
	var inFlight, peak int64

	var usedLock sync.Mutex
	usedThreads := make(map[int]bool)

	runConcurrently(threads, count, func(threadIndex, index int) {

		current := atomic.AddInt64(&inFlight, 1)
		for {
			previous := atomic.LoadInt64(&peak)
			if current <= previous || atomic.CompareAndSwapInt64(&peak, previous, current) {
				break
			}
		}

		usedLock.Lock()
		usedThreads[threadIndex] = true
		usedLock.Unlock()

		// Hold the slot long enough for the other workers to enter the job too.
		time.Sleep(time.Millisecond)
		atomic.AddInt64(&visits[index], 1)
		atomic.AddInt64(&inFlight, -1)
	})

	for index, visited := range visits {
		if visited != 1 {
			t.Errorf("The index %d was visited %d times instead of once", index, visited)
		}
	}

	if peak < 2 {
		t.Errorf("The work did not overlap, at most %d job was in flight at a time", peak)
	}

	usedLock.Lock()
	defer usedLock.Unlock()
	for threadIndex := range usedThreads {
		if threadIndex < 0 || threadIndex >= threads {
			t.Errorf("The job was given the thread index %d, outside [0,%d)", threadIndex, threads)
		}
	}
}

// Asking for more workers than there is work must not hand out thread indexes the job above the item count, so a
// backend that indexes a client by the thread index sees no more of them than there are items.
func TestRunConcurrentlyClampsToWorkAvailable(t *testing.T) {

	var visited int64

	var usedLock sync.Mutex
	usedThreads := make(map[int]bool)

	runConcurrently(100, 3, func(threadIndex, index int) {
		usedLock.Lock()
		usedThreads[threadIndex] = true
		usedLock.Unlock()
		atomic.AddInt64(&visited, 1)
	})

	if visited != 3 {
		t.Errorf("Expecting the 3 indices to be visited, got %d", visited)
	}

	usedLock.Lock()
	defer usedLock.Unlock()
	if len(usedThreads) > 3 {
		t.Errorf("Expecting at most 3 thread indexes for 3 items, got %d: %v", len(usedThreads), usedThreads)
	}
}

// An empty range must run nothing, whether or not workers were asked for.
func TestRunConcurrentlyWithNothingToDo(t *testing.T) {

	called := false
	runConcurrently(4, 0, func(threadIndex, index int) { called = true })

	if called {
		t.Errorf("The job was called for an empty range")
	}
}

// A panic raised by one worker must be re-raised in the calling goroutine, and only after the other workers stopped:
// the caller therefore sees exactly what the single-threaded loop produced, and a failing worker never leaves the
// command hung.
func TestRunConcurrentlyReraisesAWorkerPanic(t *testing.T) {

	const count = 4
	var visited int64

	recovered := recoverPanicFrom(func() {
		runConcurrently(count, count, func(threadIndex, index int) {
			atomic.AddInt64(&visited, 1)
			if index >= 2 {
				panic(fmt.Sprintf("worker failure at index %d", index))
			}
		})
	})

	if recovered == nil {
		t.Errorf("A worker panic should be re-raised in the calling goroutine")
		return
	}
	if message, ok := recovered.(string); !ok || !strings.HasPrefix(message, "worker failure at index ") {
		t.Errorf("Unexpected value re-raised from a worker: %v", recovered)
	}

	if visited != count {
		t.Errorf("Every index should be visited before the panic is re-raised, got %d of %d", visited, count)
	}
}

// The panic must not cut the run short: whatever is still running has to finish first.
func TestRunConcurrentlyWaitsForTheWorkersBeforeRaising(t *testing.T) {

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	finished := make(chan struct{})

	var recovered interface{}

	go func() {
		defer close(finished)
		recovered = recoverPanicFrom(func() {
			runConcurrently(2, 2, func(threadIndex, index int) {
				if index == 0 {
					panic("early failure")
				}
				entered <- struct{}{}
				<-release
			})
		})
	}()

	<-entered

	select {
	case <-finished:
		t.Errorf("The panic was raised while a worker was still running")
		return
	default:
	}

	close(release)
	<-finished

	if recovered == nil {
		t.Errorf("The worker panic was not re-raised")
	}
}

// The benchmark command used to run its uploads, downloads and deletes on a channel-based fan-out of its own, which
// caught a LOG_ERROR panic and called os.Exit from the worker goroutine instead of reporting it.  It now runs on
// runConcurrently, so a failing worker must surface from Benchmark itself.
func TestBenchmarkReraisesWorkerFailure(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "benchmark_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	storage, err := CreateFileStorage(testDir, false, 4)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}

	failing := &instrumentedStorage{FileStorage: storage, uploadFailures: true}

	// One chunk and one thread keep the run small, and a zero file size skips the local disk write.  The chunk size has
	// to be a power of two, so it is the smallest valid one rather than zero.
	recovered := recoverPanicFrom(func() {
		Benchmark(testDir, failing, 0, 1024, 1, 1, 1)
	})

	exception, ok := recovered.(Exception)
	if !ok {
		t.Errorf("Expecting a benchmark worker failure to be re-raised as an Exception, got %v", recovered)
		return
	}
	if exception.LogID != "BENCHMARK_UPLOAD" {
		t.Errorf("Expecting the failure to be reported by %s, got %s", "BENCHMARK_UPLOAD", exception.LogID)
	}

	// The upload was attempted, and every chunk of the run went through the worker before the failure was raised.
	if attempted := atomic.LoadInt64(&failing.attempted); attempted != 1 {
		t.Errorf("Expecting the single chunk to be uploaded once, got %d attempts", attempted)
	}
}

// The snapshot downloads run on the helper, so a failure in one of them has to surface from downloadSnapshots itself
// rather than unwinding its own goroutine and leaving the caller waiting.
func TestDownloadSnapshotsReraisesWorkerFailure(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkHash := uploadRandomChunk(snapshotManager, 1024)
	if chunkHash == "" {
		t.Errorf("Failed to upload a chunk")
		return
	}

	now := time.Now().Unix()
	revisions := []int{1, 2, 3, 4}
	for _, revision := range revisions {
		createTestSnapshot(snapshotManager, "vm1@host1", revision, now-int64(revision)*3600, now,
			[]string{chunkHash}, "tag")
	}

	failing := &instrumentedStorage{
		FileStorage:      snapshotManager.storage.(*FileStorage),
		downloadFailPath: "snapshots/vm1@host1/3",
	}
	snapshotManager.storage = failing

	recovered := recoverPanicFrom(func() {
		snapshotManager.downloadSnapshots("vm1@host1", revisions, true, 4)
	})

	exception, ok := recovered.(Exception)
	if !ok {
		t.Errorf("Expecting the failure to be re-raised as an Exception, got %v", recovered)
		return
	}
	if exception.LogID != "DOWNLOAD_FILE" {
		t.Errorf("Expecting the failure to be reported by %s, got %s", "DOWNLOAD_FILE", exception.LogID)
	}
	if !strings.Contains(exception.Message, "injected download failure for snapshots/vm1@host1/3") {
		t.Errorf("Expecting the failure to name the file, got %q", exception.Message)
	}

	// The workers are not stopped early, so every revision is still attempted.
	if attempted := atomic.LoadInt64(&failing.attempted); attempted != int64(len(revisions)) {
		t.Errorf("Expecting all %d revisions to be downloaded, got %d attempts", len(revisions), attempted)
	}
}
