// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	crypto_rand "crypto/rand"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"testing"
)

// errorReader fails every read with 'err', so that the read-error path of the chunk maker can be exercised.
type errorReader struct {
	err error
}

func (reader *errorReader) Read(p []byte) (int, error) {
	return 0, reader.err
}

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
		fileSize, _, _ := maker.AddData(buffer, chunkFunc)
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

// The OneDrive denial is the only read failure that may be skipped instead of aborting, so the helper has to keep that
// distinction: a plain read error still has to go through LOG_ERROR.
func TestChunkMakerReadErrorStillAborts(t *testing.T) {

	for _, fixedChunkSize := range []bool{false, true} {

		config := CreateConfig()
		config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
		config.AverageChunkSize = 32
		config.MaximumChunkSize = 64
		config.MinimumChunkSize = 16
		if fixedChunkSize {
			config.MinimumChunkSize = config.MaximumChunkSize
		}
		config.ChunkSeed = []byte("duplicacy")

		maker := CreateFileChunkMaker(config, false)

		recovered := recoverPanicFrom(func() {
			maker.AddData(&errorReader{err: fmt.Errorf("input/output error")}, func(chunk *Chunk) {
				t.Errorf("[fixed chunk size %v] A failing read should not produce a chunk", fixedChunkSize)
			})
		})

		exception, ok := recovered.(Exception)
		if !ok {
			t.Errorf("[fixed chunk size %v] Expecting a read failure to abort, got %v", fixedChunkSize, recovered)
			continue
		}
		if exception.LogID != "CHUNK_MAKER" {
			t.Errorf("[fixed chunk size %v] Expecting the failure to be reported by %s, got %s",
				fixedChunkSize, "CHUNK_MAKER", exception.LogID)
		}
	}
}

// The denial is recognized by its message suffix, and only on Windows where the error exists.
func TestIsCloudFileError(t *testing.T) {

	denial := fmt.Errorf("read file: Access to the cloud file is denied.")
	if recognized := isCloudFileError(denial); recognized != (runtime.GOOS == "windows") {
		t.Errorf("Expecting the cloud-file denial to be recognized on Windows only, got %v on %s",
			recognized, runtime.GOOS)
	}

	if isCloudFileError(fmt.Errorf("input/output error")) {
		t.Errorf("An unrelated read error was mistaken for a cloud-file denial")
	}
}

// On Windows the denial has to be downgraded to a warning and reported with the sentinel, which is what makes the
// backup skip the file; elsewhere the error cannot occur, so the test does not apply.
func TestChunkMakerCloudFileDeniedIsSkipped(t *testing.T) {

	if runtime.GOOS != "windows" {
		t.Skip("The OneDrive cloud-file error only exists on Windows")
	}

	config := CreateConfig()
	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
	config.AverageChunkSize = 32
	config.MaximumChunkSize = 64
	config.MinimumChunkSize = 16
	config.ChunkSeed = []byte("duplicacy")

	maker := CreateFileChunkMaker(config, false)

	savedTestingT := testingT
	testingT = nil
	defer func() {
		testingT = savedTestingT
	}()

	size, hash, reason := maker.AddData(
		&errorReader{err: fmt.Errorf("read file: Access to the cloud file is denied.")},
		func(chunk *Chunk) {
			t.Errorf("A denied cloud file should not produce a chunk")
		})

	if size >= 0 || hash != "" || reason != cloudFileFailure {
		t.Errorf("Expecting a denied cloud file to be skipped with the sentinel, got (%d, %q, %q)",
			size, hash, reason)
	}
}
