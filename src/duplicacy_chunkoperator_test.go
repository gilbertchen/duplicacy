// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	crypto_rand "crypto/rand"
	"math/rand"
)

// WaitForCompletion has to be woken by the task that finishes last.  It used to poll the task counter every
// 100 ms, so a command whose chunk work takes a few milliseconds -- the whole run on a local storage --
// spent most of the poll interval waiting for nothing.  The download here is held until WaitForCompletion
// is already waiting on it, so the elapsed time is one poll interval on a polling implementation and the
// release itself on a signalled one.
func TestWaitForCompletionIsWokenNotPolled(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "chunk_operator_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	storage, err := CreateFileStorage(testDir, false, 1)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}

	config := CreateConfig()

	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i)
	}

	chunk := CreateChunk(config, true)
	chunk.Reset(true)
	chunk.Write(content)
	chunkHash := chunk.GetHash()

	// Upload the chunk with a plain operator, so that there is something for the operator below to download.
	uploader := CreateChunkOperator(config, storage, nil, false, false, 1, false)
	uploader.UploadCompletionFunc = func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int) {}
	uploader.Upload(chunk, 0, false)
	uploader.WaitForCompletion()
	uploader.Stop()

	blocked := &instrumentedStorage{
		FileStorage:  storage,
		blockEntered: make(chan struct{}),
		blockRelease: make(chan struct{}),
	}

	// No snapshot cache, so that the download goes to the storage and reaches the blocked DownloadFile.
	operator := CreateChunkOperator(config, blocked, nil, false, false, 1, false)
	defer operator.Stop()

	operator.DownloadAsync(chunkHash, 0, false, func(downloaded *Chunk, chunkIndex int) {
		config.PutChunk(downloaded)
	})

	<-blocked.blockEntered

	start := time.Now()
	finished := make(chan struct{})
	go func() {
		operator.WaitForCompletion()
		close(finished)
	}()

	// The download is still held here, so WaitForCompletion is waiting on it and the release is the only thing that can
	// wake it up.  This sleep is well under the 100 ms poll interval that this test is against.
	time.Sleep(20 * time.Millisecond)
	close(blocked.blockRelease)
	<-finished

	if elapsed := time.Since(start); elapsed >= 60*time.Millisecond {
		t.Errorf("WaitForCompletion took %v after the last task finished; it is waiting on a timer, not on the task",
			elapsed)
	}
}

// TestDownloadRawChunk checks the reader half of the verbatim copy: with 'rawData' set, the operator hands back the
// stored bytes without decrypting them, and -- because the caller is the one that knows the destination stores them
// identically -- without marking the chunk as raw, so the id is derived from the bytes actually held.
func TestDownloadRawChunk(t *testing.T) {

	setTestingT(t)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "raw_chunk_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	storage, err := CreateFileStorage(testDir, false, 1)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}

	config := CreateConfig()

	content := make([]byte, 8192)
	crypto_rand.Read(content)

	chunk := CreateChunk(config, true)
	chunk.Reset(true)
	chunk.Write(content)
	chunkHash := chunk.GetHash()
	chunkID := chunk.GetID()

	uploader := CreateChunkOperator(config, storage, nil, false, false, 1, false)
	uploader.UploadCompletionFunc = func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int) {}
	uploader.Upload(chunk, 0, false)
	uploader.WaitForCompletion()
	uploader.Stop()

	chunkPath, err := storage.ChunkPath(chunkID)
	if err != nil {
		t.Errorf("Failed to derive the chunk path: %v", err)
		return
	}
	storedData, err := ioutil.ReadFile(path.Join(testDir, chunkPath))
	if err != nil {
		t.Errorf("Failed to read the stored chunk: %v", err)
		return
	}

	operator := CreateChunkOperator(config, storage, nil, false, false, 1, false)
	operator.rawData = true
	defer operator.Stop()

	downloaded := operator.Download(chunkHash, 0, false)

	if !bytes.Equal(downloaded.GetBytes(), storedData) {
		t.Errorf("The raw download returned %d bytes that differ from the %d stored bytes",
			downloaded.GetLength(), len(storedData))
	}
	if downloaded.isRawData {
		t.Errorf("The download marked the chunk as raw; only the caller turning it into an uploaded chunk may do that")
	}
	if downloaded.GetID() != chunkID {
		t.Errorf("The raw download lost the chunk identity: %s instead of %s", downloaded.GetID(), chunkID)
	}
	if !bytes.Equal([]byte(downloaded.GetHash()), []byte(chunkHash)) {
		t.Errorf("The raw download lost the chunk hash: %x instead of %x", downloaded.GetHash(), chunkHash)
	}
	config.PutChunk(downloaded)
}

// A storage that needs no cache must not touch the chunk cache at all, on either side: reading it would trade a storage
// read for a cache read on a filesystem where the two cost the same, and writing it would add the write cycle for an
// entry nothing will read back.  Every other cache access is guarded by IsCacheNeeded(); the chunk operator used to be
// the exception, so a local storage populated and reported hits against a cache it had declared it did not want.
func TestChunkCacheIsGuardedByIsCacheNeeded(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "chunk_cache_guard_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	// A storage whose IsCacheNeeded() is false, which is what a plain local path gets.
	storage, err := CreateFileStorage(path.Join(testDir, "storage"), false, 1)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	storage.CreateDirectory(0, "chunks")
	storage.CreateDirectory(0, "snapshots")

	cache, err := CreateFileStorage(path.Join(testDir, "cache"), false, 1)
	if err != nil {
		t.Errorf("Failed to create the cache: %v", err)
		return
	}
	cache.CreateDirectory(0, "chunks")
	cache.CreateDirectory(0, "snapshots")

	config := CreateConfig()

	// uploadMetadataChunk uploads 'content' as a metadata chunk through its own operator and returns the chunk hash.
	uploadMetadataChunk := func(content []byte) string {
		uploader := CreateChunkOperator(config, storage, cache, false, false, 1, false)
		uploader.UploadCompletionFunc = func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int) {}
		chunk := CreateChunk(config, true)
		chunk.Reset(true)
		chunk.Write(content)
		uploader.Upload(chunk, 0, true)
		uploader.WaitForCompletion()
		uploader.Stop()
		return chunk.GetHash()
	}

	// The write side with the cache off: the chunk must not be added to the cache.
	content := make([]byte, 4096)
	crypto_rand.Read(content)
	chunkHash := uploadMetadataChunk(content)
	chunkID := config.GetChunkIDFromHash(chunkHash)

	if _, exist, _, err := cache.FindChunk(0, chunkID, false); err != nil || exist {
		t.Errorf("The metadata chunk was written to the cache of a storage that needs none (exist=%t, err=%v)", exist, err)
	}

	// Capture the log so the read side can be observed by its CHUNK_CACHE line.
	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	// The CHUNK_CACHE id covers both halves -- the upload records that it saved the chunk and the download that it
	// loaded it -- so only the load is what tells the read side apart.
	cacheLoads := func() int {
		loads := 0
		for _, message := range capture.messages("CHUNK_CACHE") {
			if strings.Contains(message, "loaded from the snapshot cache") {
				loads++
			}
		}
		return loads
	}

	// The read side with the cache off: the download must not consult the cache, so no CHUNK_CACHE load is produced,
	// and it must not populate it either -- the write-back is reached through the read, so leaving the read unguarded
	// also leaves the write unguarded.  The chunk is in the storage but absent from the cache, which is the state the
	// download starts from.
	operator := CreateChunkOperator(config, storage, cache, false, false, 1, false)
	if downloaded := operator.Download(chunkHash, 0, true); downloaded == nil || downloaded.GetID() != chunkID {
		operator.Stop()
		t.Errorf("Failed to download the metadata chunk %s", chunkID)
		return
	} else {
		config.PutChunk(downloaded)
	}
	operator.Stop()

	if loads := cacheLoads(); loads != 0 {
		t.Errorf("The chunk cache of a storage that needs none was read: %d loads", loads)
	}
	if _, exist, _, err := cache.FindChunk(0, chunkID, false); err != nil || exist {
		t.Errorf("The metadata chunk was written back to the cache of a storage that needs none (exist=%t, err=%v)",
			exist, err)
	}

	// A storage that does need a cache must still get both halves; otherwise the guard would have turned the cache off
	// everywhere rather than only where it does not pay.  The same download now populates the cache, and a second one
	// is served from it.
	storage.isCacheNeeded = true

	readOperator := CreateChunkOperator(config, storage, cache, false, false, 1, false)
	if downloaded := readOperator.Download(chunkHash, 0, true); downloaded == nil {
		t.Errorf("Failed to download the metadata chunk %s with the cache enabled", chunkID)
		readOperator.Stop()
		return
	} else {
		config.PutChunk(downloaded)
	}
	readOperator.Stop()

	if _, exist, _, err := cache.FindChunk(0, chunkID, false); err != nil || !exist {
		t.Errorf("The metadata chunk was not written to the cache of a storage that needs one (exist=%t, err=%v)",
			exist, err)
	}

	secondOperator := CreateChunkOperator(config, storage, cache, false, false, 1, false)
	if downloaded := secondOperator.Download(chunkHash, 0, true); downloaded == nil {
		t.Errorf("Failed to download the cached metadata chunk %s", chunkID)
		secondOperator.Stop()
		return
	} else {
		config.PutChunk(downloaded)
	}
	secondOperator.Stop()

	if loads := cacheLoads(); loads != 1 {
		t.Errorf("Expecting the cache-enabled storage to read the chunk back from the cache, got %d loads", loads)
	}
}

func TestChunkOperator(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	setTestingT(t)
	SetLoggingLevel(DEBUG)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "storage_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	t.Logf("storage: %s", *testStorageName)

	storage, err := loadStorage(testDir, 1)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	storage.EnableTestMode()
	storage.SetRateLimits(*testRateLimit, *testRateLimit)

	for _, dir := range []string{"chunks", "snapshots"} {
		err = storage.CreateDirectory(0, dir)
		if err != nil {
			t.Errorf("Failed to create directory %s: %v", dir, err)
			return
		}
	}

	numberOfChunks := 100
	maxChunkSize := 64 * 1024

	if *testQuickMode {
		numberOfChunks = 10
	}

	var chunks []*Chunk

	config := CreateConfig()
	config.MinimumChunkSize = 100
	config.chunkPool = make(chan *Chunk, numberOfChunks*2)
	totalFileSize := 0

	for i := 0; i < numberOfChunks; i++ {
		content := make([]byte, rand.Int()%maxChunkSize+1)
		_, err = crypto_rand.Read(content)
		if err != nil {
			t.Errorf("Error generating random content: %v", err)
			return
		}

		chunk := CreateChunk(config, true)
		chunk.Reset(true)
		chunk.Write(content)
		chunks = append(chunks, chunk)

		t.Logf("Chunk: %s, size: %d", chunk.GetID(), chunk.GetLength())
		totalFileSize += chunk.GetLength()
	}

	chunkOperator := CreateChunkOperator(config, storage, nil, false, false, *testThreads, false)
	chunkOperator.UploadCompletionFunc = func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
		t.Logf("Chunk %s size %d (%d/%d) uploaded", chunk.GetID(), chunkSize, chunkIndex, len(chunks))
	}

	for i, chunk := range chunks {
		chunkOperator.Upload(chunk, i, false)
	}

	chunkOperator.WaitForCompletion()

	for i, chunk := range chunks {
		downloaded := chunkOperator.Download(chunk.GetHash(), i, false)
		if downloaded.GetID() != chunk.GetID() {
			t.Errorf("Uploaded: %s, downloaded: %s", chunk.GetID(), downloaded.GetID())
		}
	}

	chunkOperator.Stop()

	for _, file := range listChunks(storage) {
		err = storage.DeleteFile(0, "chunks/"+file)
		if err != nil {
			t.Errorf("Failed to delete the file %s: %v", file, err)
			return
		}
	}

}
