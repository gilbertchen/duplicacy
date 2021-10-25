// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	crypto_rand "crypto/rand"
	"math/rand"
	"sort"
	"testing"
)

func splitIntoChunks(content []byte, n, averageChunkSize, maxChunkSize, minChunkSize int) ([]string, int) {

	config := CreateConfig()

	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
	config.AverageChunkSize = averageChunkSize
	config.MaximumChunkSize = maxChunkSize
	config.MinimumChunkSize = minChunkSize
	config.ChunkSeed = []byte("duplicacy")

	config.HashKey = DEFAULT_KEY
	config.IDKey = DEFAULT_KEY

	maker := CreateFileChunkMaker(config, false)

	var chunks []string
	totalChunkSize := 0
	totalFileSize := int64(0)

	buffers := make([]*bytes.Buffer, n)
	sizes := make([]int, n)
	sizes[0] = 0
	for i := 1; i < n; i++ {
		same := true
		for same {
			same = false
			sizes[i] = rand.Int() % len(content)
			for j := 0; j < i; j++ {
				if sizes[i] == sizes[j] {
					same = true
					break
				}
			}
		}
	}

	sort.Sort(sort.IntSlice(sizes))

	for i := 0; i < n-1; i++ {
		buffers[i] = bytes.NewBuffer(content[sizes[i]:sizes[i+1]])
	}
	buffers[n-1] = bytes.NewBuffer(content[sizes[n-1]:])

	chunkFunc := func(chunk *Chunk) {
		chunks = append(chunks, chunk.GetHash())
		totalChunkSize += chunk.GetLength()
		config.PutChunk(chunk)
	}

	for _, buffer := range buffers {
		fileSize, _ := maker.AddData(buffer, chunkFunc)
		totalFileSize += fileSize
	}
	maker.AddData(nil, chunkFunc)

	if totalFileSize != int64(totalChunkSize) {
		LOG_ERROR("CHUNK_SPLIT", "total chunk size: %d, total file size: %d", totalChunkSize, totalFileSize)
	}
	return chunks, totalChunkSize
}

func TestChunkMaker(t *testing.T) {

	//sizes := [...] int { 64 }
	sizes := [...]int{64, 256, 1024, 1024 * 10}

	for _, size := range sizes {

		content := make([]byte, size)
		_, err := crypto_rand.Read(content)
		if err != nil {
			t.Errorf("Error generating random content: %v", err)
			continue
		}

		chunkArray1, totalSize1 := splitIntoChunks(content, 10, 32, 64, 16)


		for _, n := range [...]int{6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16} {
			chunkArray2, totalSize2 := splitIntoChunks(content, n, 32, 64, 16)

			if totalSize1 != totalSize2 {
				t.Errorf("[size %d] total size is %d instead of %d",
					size, totalSize2, totalSize1)
			}

			if len(chunkArray1) != len(chunkArray2) {
				t.Errorf("[size %d] number of chunks is %d instead of %d",
					size, len(chunkArray2), len(chunkArray1))
			} else {
				for i := 0; i < len(chunkArray1); i++ {
					if chunkArray1[i] != chunkArray2[i] {
						t.Errorf("[size %d, chunk %d] chunk is different", size, i)
					}
				}
			}

		}
	}

}
