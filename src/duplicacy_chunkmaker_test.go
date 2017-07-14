// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
    "testing"
    "bytes"
    crypto_rand "crypto/rand"
    "math/rand"
    "io"
    "sort"
)

func splitIntoChunks(content []byte, n, averageChunkSize, maxChunkSize, minChunkSize,
                     bufferCapacity int) ([]string, int) {

    config := CreateConfig()

    config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
    config.AverageChunkSize = averageChunkSize
    config.MaximumChunkSize = maxChunkSize
    config.MinimumChunkSize = minChunkSize
    config.ChunkSeed = []byte("duplicacy")

    config.HashKey = DEFAULT_KEY
    config.IDKey = DEFAULT_KEY

    maker := CreateChunkMaker(config, false)

    var chunks [] string
    totalChunkSize := 0
    totalFileSize := int64(0)

    //LOG_INFO("CHUNK_SPLIT", "bufferCapacity: %d", bufferCapacity)

    buffers := make([] *bytes.Buffer, n)
    sizes := make([] int, n)
    sizes[0] = 0
    for i := 1; i < n; i++ {
        same := true
        for same {
            same = false
            sizes[i] = rand.Int() % n
            for j := 0; j < i; j++ {
                if sizes[i] == sizes[j] {
                    same = true
                    break
                }
            }
        }
    }

    sort.Sort(sort.IntSlice(sizes))

    for i := 0; i < n - 1; i++ {
        buffers[i] = bytes.NewBuffer(content[sizes[i] : sizes[i + 1]])
    }
    buffers[n - 1] = bytes.NewBuffer(content[sizes[n - 1]:])

    i := 0

    maker.ForEachChunk(buffers[0],
        func (chunk *Chunk, final bool) {
            //LOG_INFO("CHUNK_SPLIT", "i: %d, chunk: %s, size: %d", i, chunk.GetHash(), size)
            chunks = append(chunks, chunk.GetHash())
            totalChunkSize += chunk.GetLength()
        },
        func (size int64, hash string) (io.Reader, bool) {
            totalFileSize += size
            i++
            if i >= len(buffers) {
                return nil, false
            }
            return buffers[i], true
        })

    if (totalFileSize != int64(totalChunkSize)) {
        LOG_ERROR("CHUNK_SPLIT", "total chunk size: %d, total file size: %d", totalChunkSize, totalFileSize)
    }
    return chunks, totalChunkSize
}

func TestChunkMaker(t *testing.T) {


    //sizes := [...] int { 64 }
    sizes := [...] int { 64, 256, 1024, 1024 * 10 }

    for _, size := range sizes {

        content := make([]byte, size)
        _, err := crypto_rand.Read(content)
        if err != nil {
            t.Errorf("Error generating random content: %v", err)
            continue
        }

        chunkArray1, totalSize1 := splitIntoChunks(content, 10, 32, 64, 16, 32)

        capacities := [...]int { 32, 33, 34, 61, 62, 63, 64, 65, 66, 126, 127, 128, 129, 130,
                                 255, 256, 257, 511, 512, 513, 1023, 1024, 1025,
                                 32, 48, 64, 128, 256, 512, 1024, 2048, }

        //capacities := [...]int { 32 }

        for _, capacity := range capacities {

            for _, n := range [...]int { 6, 7, 8, 9, 10 } {
                chunkArray2, totalSize2 := splitIntoChunks(content, n, 32, 64, 16, capacity)

                if totalSize1 != totalSize2 {
                    t.Errorf("[size %d, capacity %d] total size is %d instead of %d",
                             size, capacity, totalSize2, totalSize1)
                }

                if len(chunkArray1) != len(chunkArray2) {
                    t.Errorf("[size %d, capacity %d] number of chunks is %d instead of %d",
                             size, capacity, len(chunkArray2), len(chunkArray1))
                } else {
                    for i := 0; i < len(chunkArray1); i++ {
                        if chunkArray1[i] != chunkArray2[i] {
                            t.Errorf("[size %d, capacity %d, chunk %d] chunk is different", size, capacity, i)
                        }
                    }
                }
            }
        }
    }

}
