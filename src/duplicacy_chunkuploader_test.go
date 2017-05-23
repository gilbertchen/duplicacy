// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "os"
    "time"
    "path"
    "testing"
    "runtime/debug"

    crypto_rand "crypto/rand"
    "math/rand"
)

func TestUploaderAndDownloader(t *testing.T) {

    rand.Seed(time.Now().UnixNano())
    setTestingT(t)
    SetLoggingLevel(INFO)

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
    } ()

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

    for _, dir := range []string { "chunks", "snapshots" }  {
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
    config.chunkPool = make(chan *Chunk, numberOfChunks * 2)
    totalFileSize := 0

    for i := 0; i < numberOfChunks; i++ {
        content := make([]byte, rand.Int() % maxChunkSize + 1)
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

    completionFunc := func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
        t.Logf("Chunk %s size %d (%d/%d) uploaded", chunk.GetID(), chunkSize, chunkIndex, len(chunks))
    }

    chunkUploader := CreateChunkUploader(config, storage, nil, testThreads, nil)
    chunkUploader.completionFunc = completionFunc
    chunkUploader.Start()

    for i, chunk := range chunks {
        chunkUploader.StartChunk(chunk, i)
    }

    chunkUploader.Stop()


    chunkDownloader := CreateChunkDownloader(config, storage, nil, true, testThreads)
    chunkDownloader.totalFileSize = int64(totalFileSize)

    for _, chunk := range chunks {
        chunkDownloader.AddChunk(chunk.GetHash())
    }

    for i, chunk := range chunks {
        downloaded := chunkDownloader.WaitForChunk(i)
        if downloaded.GetID() != chunk.GetID() {
            t.Error("Uploaded: %s, downloaded: %s", chunk.GetID(), downloaded.GetID())
        }
    }

    chunkDownloader.Stop()

    for _, file := range listChunks(storage) {
        err = storage.DeleteFile(0, "chunks/" + file)
        if err != nil {
            t.Errorf("Failed to delete the file %s: %v", file, err)
            return
        }
    }

}
