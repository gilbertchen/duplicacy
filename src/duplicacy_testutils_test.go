// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	crypto_rand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recovering reports a panic that unwound a test as a test failure rather than letting it fail the whole binary.  A
// LOG_ERROR raises an Exception, which is reported with its log id, since that is what tells a reader which check
// failed.  It is meant to be deferred: 'defer recovering(t)'.
func recovering(t *testing.T) {
	reportRecovered(t, recover(), false)
}

// recoveringWithStack is recovering plus the stack of the goroutine that panicked, for the tests where an unexpected
// panic is likely to come from deep inside the library and the stack is what makes it diagnosable.
func recoveringWithStack(t *testing.T) {
	reportRecovered(t, recover(), true)
}

// reportRecovered is handed what recover() returned rather than calling it itself, because recover only stops a panic
// when it is called directly by the deferred function.
func reportRecovered(t *testing.T, r interface{}, withStack bool) {
	if r == nil {
		return
	}

	switch e := r.(type) {
	case Exception:
		t.Errorf("%s %s", e.LogID, e.Message)
	default:
		t.Errorf("%v", e)
	}

	if withStack {
		debug.PrintStack()
	}
}

// createRandomFileSeeded writes a file of a size between half and all of 'maxSize', filled from a generator seeded with
// 'seed' so that the same seed always produces the same file.  A private generator is used rather than rand.Seed, which
// is a no-op for modules that declare go 1.24 or later.
func createRandomFileSeeded(path string, maxSize int, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		LOG_ERROR("RANDOM_FILE", "Can't open %s for writing: %v", path, err)
		return
	}

	defer file.Close()

	size := maxSize/2 + rng.Int()%(maxSize/2)

	buffer := make([]byte, 32*1024)
	for size > 0 {
		bytes := size
		if bytes > cap(buffer) {
			bytes = cap(buffer)
		}
		rng.Read(buffer[:bytes])
		bytes, err = file.Write(buffer[:bytes])
		if err != nil {
			LOG_ERROR("RANDOM_FILE", "Failed to write to %s: %v", path, err)
			return
		}
		size -= bytes
	}
}

// readChunkTree reads every chunk file under '<storageDir>/chunks' into a map from its path relative to 'chunks/' to
// its content, so that two storages can be compared byte for byte.
func readChunkTree(t *testing.T, storageDir string) map[string][]byte {
	tree := make(map[string][]byte)
	root := path.Join(storageDir, "chunks")
	err := filepath.Walk(root, func(chunkPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || strings.HasSuffix(chunkPath, ".fsl") {
			return nil
		}
		content, err := ioutil.ReadFile(chunkPath)
		if err != nil {
			return err
		}
		relativePath, err := filepath.Rel(root, chunkPath)
		if err != nil {
			return err
		}
		tree[relativePath] = content
		return nil
	})
	if err != nil {
		t.Errorf("Failed to read the chunk tree under %s: %v", root, err)
	}
	return tree
}

// createChunkDirectories creates 'numberOfDirectories' directories under 'chunks/'.
func createChunkDirectories(t *testing.T, storageDir string, numberOfDirectories int) {
	for i := 0; i < numberOfDirectories; i++ {
		dir := path.Join(storageDir, "chunks", fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Errorf("Failed to create the chunk directory %s: %v", dir, err)
			return
		}
	}
}

// createTestSnapshotManager builds a manager over a fresh file storage and an empty snapshot cache under 'testDir'.
func createTestSnapshotManager(testDir string) *SnapshotManager {

	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	storage, _ := CreateFileStorage(testDir, false, 1)
	storage.CreateDirectory(0, "chunks")
	storage.CreateDirectory(0, "snapshots")
	config := CreateConfig()
	snapshotManager := CreateSnapshotManager(config, storage)

	cacheDir := path.Join(testDir, "cache")
	snapshotCache, _ := CreateFileStorage(cacheDir, false, 1)
	snapshotCache.CreateDirectory(0, "chunks")
	snapshotCache.CreateDirectory(0, "snapshots")

	snapshotManager.snapshotCache = snapshotCache

	SetDuplicacyPreferencePath(testDir + "/.duplicacy")

	return snapshotManager
}

func uploadTestChunk(manager *SnapshotManager, content []byte) string {

	chunkOperator := CreateChunkOperator(manager.config, manager.storage, nil, false, false, *testThreads, false)
	chunkOperator.UploadCompletionFunc = func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
		LOG_INFO("UPLOAD_CHUNK", "Chunk %s size %d uploaded", chunk.GetID(), chunkSize)
	}

	chunk := CreateChunk(manager.config, true)
	chunk.Reset(true)
	chunk.Write(content)

	chunkOperator.Upload(chunk, 0, false)
	chunkOperator.WaitForCompletion()
	chunkOperator.Stop()

	return chunk.GetHash()
}

func uploadRandomChunk(manager *SnapshotManager, chunkSize int) string {
	content := make([]byte, chunkSize)
	_, err := crypto_rand.Read(content)
	if err != nil {
		LOG_ERROR("UPLOAD_RANDOM", "Error generating random content: %v", err)
		return ""
	}

	return uploadTestChunk(manager, content)
}

func uploadRandomChunks(manager *SnapshotManager, chunkSize int, numberOfChunks int) []string {
	chunkList := make([]string, 0)
	for i := 0; i < numberOfChunks; i++ {
		chunkHash := uploadRandomChunk(manager, chunkSize)
		chunkList = append(chunkList, chunkHash)
	}
	return chunkList
}

func createTestSnapshot(manager *SnapshotManager, snapshotID string, revision int, startTime int64, endTime int64, chunkHashes []string, tag string) {

	snapshot := &Snapshot{
		ID:          snapshotID,
		Revision:    revision,
		StartTime:   startTime,
		EndTime:     endTime,
		ChunkHashes: chunkHashes,
		Tag:         tag,
	}

	var chunkHashesInHex []string
	for _, chunkHash := range chunkHashes {
		chunkHashesInHex = append(chunkHashesInHex, hex.EncodeToString([]byte(chunkHash)))
	}

	sequence, _ := json.Marshal(chunkHashesInHex)
	snapshot.ChunkSequence = []string{uploadTestChunk(manager, sequence)}

	description, _ := snapshot.MarshalJSON()
	path := snapshotPath(snapshotID, snapshot.Revision)
	manager.UploadFile(path, path, description)
}

// uploadTestMetadataChunk uploads 'content' as a metadata chunk, the same way the backup code does for the sequences
// that make up a snapshot.
func uploadTestMetadataChunk(manager *SnapshotManager, content []byte) string {

	chunkOperator := CreateChunkOperator(manager.config, manager.storage, manager.snapshotCache, false, false, 1, false)
	defer chunkOperator.Stop()

	chunkOperator.UploadCompletionFunc = func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
	}

	chunk := CreateChunk(manager.config, true)
	chunk.Reset(true)
	chunk.Write(content)

	chunkOperator.Upload(chunk, 0, true)
	chunkOperator.WaitForCompletion()

	return chunk.GetHash()
}

// instrumentedStorage is the one test double over FileStorage: it counts the operations a test wants to observe, can
// hold a download until the test releases it, and can fail a download or a snapshot deletion on demand.  A test turns on
// only the behaviour it needs by setting the corresponding field, and leaves the rest at their zero value; the methods
// not overridden here are promoted from the embedded FileStorage, so an untouched double behaves like the real storage.
type instrumentedStorage struct {
	*FileStorage

	// isFastListing answers IsFastListing when the test needs to control whether the storage claims to support fast
	// listing, which decides whether a copy lists the destination or probes each chunk.
	isFastListing bool

	// The operation counters a copy test reads.  getFileInfoCalls counts every existence check, while snapshotInfoCalls
	// counts only the ones for a snapshot path, which is what a per-revision check would ask about.
	getFileInfoCalls  int64
	findChunkCalls    int64
	snapshotInfoCalls int64
	snapshotListings  int64
	chunkListings     int64
	snapshotDirCalls  int64
	uploadedChunks    int64
	uploadedSnapshots int64
	chunkDownloads    int64

	// The concurrent-download observation.  downloadDelay widens the window in which downloads overlap, so the peak
	// observed here is what a test uses to tell a serial loop from a parallel one.
	downloadLock     sync.Mutex
	downloadThreads  map[int]bool
	downloadPaths    map[string]int
	downloadInFlight int
	downloadPeak     int
	downloadDelay    time.Duration

	// A download that must be held until the test releases it.
	blockEntered chan struct{}
	blockRelease chan struct{}
	blockOnce    sync.Once

	// An injected failure: every upload fails when uploadFailures is set, and the download of downloadFailPath fails
	// when it is not empty.  attempted counts the failing operation, so a test can tell how often it was reached.
	uploadFailures   bool
	downloadFailPath string
	attempted        int64

	// The snapshot-deletion observation.  entered is signalled once when a gated deletion has started, gates holds a
	// deletion until it is closed, and failPaths are the paths whose deletion reports an error instead of deleting.
	deleteLock     sync.Mutex
	deleteInFlight int
	deletePeak     int
	deleteThreads  map[int]bool
	deleteOrder    []string
	entered        map[string]chan struct{}
	gates          map[string]chan struct{}
	failPaths      map[string]bool
}

func (storage *instrumentedStorage) IsFastListing() bool { return storage.isFastListing }

// CreateDirectory counts the creation of a snapshot id's directory, which the copy must do once rather than once per
// revision.  The directories created at init time are 'chunks' and 'snapshots', neither of which is under 'snapshots/'.
func (storage *instrumentedStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	if strings.HasPrefix(dir, "snapshots/") {
		atomic.AddInt64(&storage.snapshotDirCalls, 1)
	}
	return storage.FileStorage.CreateDirectory(threadIndex, dir)
}

func (storage *instrumentedStorage) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {
	atomic.AddInt64(&storage.findChunkCalls, 1)
	return storage.FileStorage.FindChunk(threadIndex, chunkID, isFossil)
}

func (storage *instrumentedStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {
	// Only the listing of one snapshot id's directory is counted; the listing of 'snapshots/' itself enumerates the ids
	// and is not what the copy can use to learn the destination's revisions.
	if strings.HasPrefix(dir, "snapshots/") && dir != "snapshots/" {
		atomic.AddInt64(&storage.snapshotListings, 1)
	}
	// The listing of a nested chunk directory is what the whole-tree walk costs one call per directory; a check that
	// probes the referenced chunks instead makes none of these.  The listing of 'chunks/' itself is made by both, so
	// it is not counted.
	if strings.HasPrefix(dir, "chunks/") && dir != "chunks/" {
		atomic.AddInt64(&storage.chunkListings, 1)
	}
	return storage.FileStorage.ListFiles(threadIndex, dir)
}

func (storage *instrumentedStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	atomic.AddInt64(&storage.getFileInfoCalls, 1)
	// A chunk lookup goes through FindChunk, which calls this method with a chunk path; only the snapshot paths are
	// counted here, since they are what a per-revision check would ask about.
	if strings.HasPrefix(filePath, "snapshots/") {
		atomic.AddInt64(&storage.snapshotInfoCalls, 1)
	}
	return storage.FileStorage.GetFileInfo(threadIndex, filePath)
}

func (storage *instrumentedStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	if strings.HasPrefix(filePath, "chunks/") {
		atomic.AddInt64(&storage.uploadedChunks, 1)
	} else if strings.HasPrefix(filePath, "snapshots/") {
		atomic.AddInt64(&storage.uploadedSnapshots, 1)
	}
	if storage.uploadFailures {
		atomic.AddInt64(&storage.attempted, 1)
		return fmt.Errorf("injected benchmark failure for %s", filePath)
	}
	return storage.FileStorage.UploadFile(threadIndex, filePath, content)
}

func (storage *instrumentedStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	// Chunk files are read both while the snapshots are read and while the chunks are copied; snapshot files are not
	// chunk data and are not counted.
	if strings.HasPrefix(filePath, "chunks/") {
		atomic.AddInt64(&storage.chunkDownloads, 1)
	}

	if storage.downloadFailPath != "" {
		atomic.AddInt64(&storage.attempted, 1)
		if filePath == storage.downloadFailPath {
			return fmt.Errorf("injected download failure for %s", filePath)
		}
	}

	storage.downloadLock.Lock()
	if storage.downloadThreads == nil {
		storage.downloadThreads = make(map[int]bool)
	}
	if storage.downloadPaths == nil {
		storage.downloadPaths = make(map[string]int)
	}
	storage.downloadThreads[threadIndex] = true
	storage.downloadPaths[filePath]++
	storage.downloadInFlight++
	if storage.downloadInFlight > storage.downloadPeak {
		storage.downloadPeak = storage.downloadInFlight
	}
	storage.downloadLock.Unlock()

	// Give the other workers a chance to enter this method before this download finishes.
	if storage.downloadDelay > 0 {
		time.Sleep(storage.downloadDelay)
	}

	if storage.blockRelease != nil {
		storage.blockOnce.Do(func() { close(storage.blockEntered) })
		<-storage.blockRelease
	}

	err = storage.FileStorage.DownloadFile(threadIndex, filePath, chunk)

	storage.downloadLock.Lock()
	storage.downloadInFlight--
	storage.downloadLock.Unlock()

	return err
}

func (storage *instrumentedStorage) DeleteFile(threadIndex int, filePath string) (err error) {

	if !strings.HasPrefix(filePath, "snapshots/") {
		return storage.FileStorage.DeleteFile(threadIndex, filePath)
	}

	storage.deleteLock.Lock()
	if storage.deleteThreads == nil {
		storage.deleteThreads = make(map[int]bool)
	}
	storage.deleteThreads[threadIndex] = true
	storage.deleteInFlight++
	if storage.deleteInFlight > storage.deletePeak {
		storage.deletePeak = storage.deleteInFlight
	}
	entered := storage.entered[filePath]
	gate := storage.gates[filePath]
	fail := storage.failPaths[filePath]
	storage.deleteLock.Unlock()

	if entered != nil {
		entered <- struct{}{}
	}
	if gate != nil {
		<-gate
	}

	if fail {
		err = fmt.Errorf("injected deletion failure for %s", filePath)
	} else {
		err = storage.FileStorage.DeleteFile(threadIndex, filePath)
	}

	storage.deleteLock.Lock()
	storage.deleteInFlight--
	storage.deleteOrder = append(storage.deleteOrder, filePath)
	storage.deleteLock.Unlock()

	return err
}

// resetCounters clears the operation counters between operations.
func (storage *instrumentedStorage) resetCounters() {
	atomic.StoreInt64(&storage.getFileInfoCalls, 0)
	atomic.StoreInt64(&storage.findChunkCalls, 0)
	atomic.StoreInt64(&storage.snapshotInfoCalls, 0)
	atomic.StoreInt64(&storage.snapshotListings, 0)
	atomic.StoreInt64(&storage.chunkListings, 0)
	atomic.StoreInt64(&storage.snapshotDirCalls, 0)
	atomic.StoreInt64(&storage.uploadedChunks, 0)
	atomic.StoreInt64(&storage.uploadedSnapshots, 0)
	atomic.StoreInt64(&storage.chunkDownloads, 0)
}

// peakConcurrentDownloads returns the largest number of downloads that were in flight at the same time.
func (storage *instrumentedStorage) peakConcurrentDownloads() int {
	storage.downloadLock.Lock()
	defer storage.downloadLock.Unlock()
	return storage.downloadPeak
}

// numberOfDownloadThreads returns how many distinct thread indexes the downloads were attributed to.
func (storage *instrumentedStorage) numberOfDownloadThreads() int {
	storage.downloadLock.Lock()
	defer storage.downloadLock.Unlock()
	return len(storage.downloadThreads)
}

// chunkDownloadCounts returns how many times each chunk file was read from the storage.
func (storage *instrumentedStorage) chunkDownloadCounts() (counts map[string]int) {
	storage.downloadLock.Lock()
	defer storage.downloadLock.Unlock()

	counts = make(map[string]int)
	for filePath, count := range storage.downloadPaths {
		if strings.HasPrefix(filePath, "chunks/") {
			counts[filePath] = count
		}
	}
	return counts
}

// resetDownloadStats forgets the thread indexes and the peak number of concurrent downloads seen so far.
func (storage *instrumentedStorage) resetDownloadStats() {
	storage.downloadLock.Lock()
	defer storage.downloadLock.Unlock()
	storage.downloadThreads = nil
	storage.downloadPaths = nil
	storage.downloadPeak = 0
}

// peakConcurrentDeletes returns the largest number of snapshot deletions that were in flight at the same time.
func (storage *instrumentedStorage) peakConcurrentDeletes() int {
	storage.deleteLock.Lock()
	defer storage.deleteLock.Unlock()
	return storage.deletePeak
}

// numberOfDeleteThreads returns how many distinct thread indexes the snapshot deletions were attributed to.
func (storage *instrumentedStorage) numberOfDeleteThreads() int {
	storage.deleteLock.Lock()
	defer storage.deleteLock.Unlock()
	return len(storage.deleteThreads)
}

// finishedDeletes returns the snapshot paths in the order their deletions finished.
func (storage *instrumentedStorage) finishedDeletes() []string {
	storage.deleteLock.Lock()
	defer storage.deleteLock.Unlock()
	return append([]string{}, storage.deleteOrder...)
}
