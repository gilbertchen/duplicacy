// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"os"
	"path"
	"runtime/debug"
	"testing"
	"time"

	crypto_rand "crypto/rand"
	"math/rand"
)

func TestChunkOperator(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	setTestingT(t)
	SetLoggingLevel(DEBUG)

	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case Exception:
				t.Errorf("%s %s", e.LogID, e.Message)
				debug.PrintStack()
			default:
				t.Errorf("%v", e)
				debug.PrintStack()
			}
		}
	}()

	testDir := path.Join(os.TempDir(), "duplicacy_test", "storage_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	t.Logf("storage: %s", testStorageName)

	storage, err := loadStorage(testDir, 1)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	storage.EnableTestMode()
	storage.SetRateLimits(testRateLimit, testRateLimit)

	for _, dir := range []string{"chunks", "snapshots"} {
		err = storage.CreateDirectory(0, dir)
		if err != nil {
			t.Errorf("Failed to create directory %s: %v", dir, err)
			return
		}
	}

	numberOfChunks := 100
	maxChunkSize := 64 * 1024

	if testQuickMode {
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

	chunkOperator := CreateChunkOperator(config, storage, nil, false, testThreads, false)
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
