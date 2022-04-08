// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

func benchmarkSplit(reader *bytes.Reader, fileSize int64, chunkSize int, compression bool, encryption bool, annotation string) {

	config := CreateConfig()
	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
	config.AverageChunkSize = chunkSize
	config.MaximumChunkSize = chunkSize * 4
	config.MinimumChunkSize = chunkSize / 4
	config.ChunkSeed = []byte("duplicacy")

	config.HashKey = DEFAULT_KEY
	config.IDKey = DEFAULT_KEY

	maker := CreateFileChunkMaker(config, false)

	startTime := float64(time.Now().UnixNano()) / 1e9
	numberOfChunks := 0
	reader.Seek(0, os.SEEK_SET)

	chunkFunc := func(chunk *Chunk) {
		if compression {
			key := ""
			if encryption {
				key = "0123456789abcdef0123456789abcdef"
			}
			err := chunk.Encrypt([]byte(key), "", false)
			if err != nil {
				LOG_ERROR("BENCHMARK_ENCRYPT", "Failed to encrypt the chunk: %v", err)
			}
		}
		config.PutChunk(chunk)
		numberOfChunks++
	}

	maker.AddData(reader, chunkFunc)
	maker.AddData(nil, chunkFunc)

	runningTime := float64(time.Now().UnixNano())/1e9 - startTime
	speed := int64(float64(fileSize) / runningTime)
	LOG_INFO("BENCHMARK_SPLIT", "Split %s bytes into %d chunks %s in %.2fs: %s/s", PrettySize(fileSize), numberOfChunks, annotation,
		runningTime, PrettySize(speed))

	return
}

func benchmarkRun(threads int, chunkCount int, job func(threadIndex int, chunkIndex int)) {
	indexChannel := make(chan int, chunkCount)
	stopChannel := make(chan int, threads)
	finishChannel := make(chan int, threads)

	// Start the uploading goroutines
	for i := 0; i < threads; i++ {
		go func(threadIndex int) {
			defer CatchLogException()
			for {
				select {
				case chunkIndex := <-indexChannel:
					job(threadIndex, chunkIndex)
					finishChannel <- 0
				case <-stopChannel:
					return
				}
			}
		}(i)
	}

	for i := 0; i < chunkCount; i++ {
		indexChannel <- i
	}

	for i := 0; i < chunkCount; i++ {
		<-finishChannel
	}

	for i := 0; i < threads; i++ {
		stopChannel <- 0
	}
}

func Benchmark(localDirectory string, storage Storage, fileSize int64, chunkSize int, chunkCount int, uploadThreads int, downloadThreads int) bool {

	filename := filepath.Join(localDirectory, "benchmark.dat")

	defer func() {
		os.Remove(filename)
	}()

	LOG_INFO("BENCHMARK_GENERATE", "Generating %s byte random data in memory", PrettySize(fileSize))
	data := make([]byte, fileSize)
	_, err := rand.Read(data)
	if err != nil {
		LOG_ERROR("BENCHMARK_RAND", "Failed to generate random data: %v", err)
		return false
	}

	startTime := float64(time.Now().UnixNano()) / 1e9
	LOG_INFO("BENCHMARK_WRITE", "Writing random data to local disk")
	err = ioutil.WriteFile(filename, data, 0600)
	if err != nil {
		LOG_ERROR("BENCHMARK_WRITE", "Failed to write the random data: %v", err)
		return false
	}

	runningTime := float64(time.Now().UnixNano())/1e9 - startTime
	speed := int64(float64(fileSize) / runningTime)
	LOG_INFO("BENCHMARK_WRITE", "Wrote %s bytes in %.2fs: %s/s", PrettySize(fileSize), runningTime, PrettySize(speed))

	startTime = float64(time.Now().UnixNano()) / 1e9
	LOG_INFO("BENCHMARK_READ", "Reading the random data from local disk")
	file, err := os.Open(filename)
	if err != nil {
		LOG_ERROR("BENCHMARK_OPEN", "Failed to open the random data file: %v", err)
		return false
	}
	segment := make([]byte, 1024*1024)
	for err == nil {
		_, err = file.Read(segment)
	}
	if err != io.EOF {
		LOG_ERROR("BENCHMARK_OPEN", "Failed to read the random data file: %v", err)
		return false
	}
	file.Close()
	runningTime = float64(time.Now().UnixNano())/1e9 - startTime
	speed = int64(float64(fileSize) / runningTime)
	LOG_INFO("BENCHMARK_READ", "Read %s bytes in %.2fs: %s/s", PrettySize(fileSize), runningTime, PrettySize(speed))

	buffer := bytes.NewReader(data)
	benchmarkSplit(buffer, fileSize, chunkSize, false, false, "without compression/encryption")
	benchmarkSplit(buffer, fileSize, chunkSize, true, false, "with compression but without encryption")
	benchmarkSplit(buffer, fileSize, chunkSize, true, true, "with compression and encryption")

	storage.CreateDirectory(0, "benchmark")
	existingFiles, _, err := storage.ListFiles(0, "benchmark/")
	if err != nil {
		LOG_ERROR("BENCHMARK_LIST", "Failed to list the benchmark directory: %v", err)
		return false
	}

	var existingChunks []string
	for _, f := range existingFiles {
		if len(f) > 0 && f[len(f)-1] != '/' {
			existingChunks = append(existingChunks, "benchmark/"+f)
		}
	}

	if len(existingChunks) > 0 {
		LOG_INFO("BENCHMARK_DELETE", "Deleting %d temporary files from previous benchmark runs", len(existingChunks))
		benchmarkRun(uploadThreads, len(existingChunks), func(threadIndex int, chunkIndex int) {
			storage.DeleteFile(threadIndex, existingChunks[chunkIndex])
		})
	}

	chunks := make([][]byte, chunkCount)
	chunkHashes := make([]string, chunkCount)
	LOG_INFO("BENCHMARK_GENERATE", "Generating %d chunks", chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunks[i] = make([]byte, chunkSize)
		_, err = rand.Read(chunks[i])
		if err != nil {
			LOG_ERROR("BENCHMARK_RAND", "Failed to generate random data: %v", err)
			return false
		}
		hashInBytes := sha256.Sum256(chunks[i])
		chunkHashes[i] = hex.EncodeToString(hashInBytes[:])

	}

	startTime = float64(time.Now().UnixNano()) / 1e9
	benchmarkRun(uploadThreads, chunkCount, func(threadIndex int, chunkIndex int) {
		err := storage.UploadFile(threadIndex, fmt.Sprintf("benchmark/chunk%d", chunkIndex), chunks[chunkIndex])
		if err != nil {
			LOG_ERROR("BENCHMARK_UPLOAD", "Failed to upload the chunk: %v", err)
			return
		}
	})

	runningTime = float64(time.Now().UnixNano())/1e9 - startTime
	speed = int64(float64(chunkSize*chunkCount) / runningTime)
	LOG_INFO("BENCHMARK_UPLOAD", "Uploaded %s bytes in %.2fs: %s/s", PrettySize(int64(chunkSize*chunkCount)), runningTime, PrettySize(speed))

	config := CreateConfig()

	startTime = float64(time.Now().UnixNano()) / 1e9
	hashError := false
	benchmarkRun(downloadThreads, chunkCount, func(threadIndex int, chunkIndex int) {
		chunk := config.GetChunk()
		chunk.Reset(false)
		err := storage.DownloadFile(threadIndex, fmt.Sprintf("benchmark/chunk%d", chunkIndex), chunk)
		if err != nil {
			LOG_ERROR("BENCHMARK_DOWNLOAD", "Failed to download the chunk: %v", err)
			return
		}

		hashInBytes := sha256.Sum256(chunk.GetBytes())
		hash := hex.EncodeToString(hashInBytes[:])
		if hash != chunkHashes[chunkIndex] {
			LOG_WARN("BENCHMARK_HASH", "Chunk %d has mismatched hashes: %s != %s", chunkIndex, chunkHashes[chunkIndex], hash)
			hashError = true
		}

		config.PutChunk(chunk)
	})

	runningTime = float64(time.Now().UnixNano())/1e9 - startTime
	speed = int64(float64(chunkSize*chunkCount) / runningTime)
	LOG_INFO("BENCHMARK_DOWNLOAD", "Downloaded %s bytes in %.2fs: %s/s", PrettySize(int64(chunkSize*chunkCount)), runningTime, PrettySize(speed))

	if !hashError {
		benchmarkRun(uploadThreads, chunkCount, func(threadIndex int, chunkIndex int) {
			storage.DeleteFile(threadIndex, fmt.Sprintf("benchmark/chunk%d", chunkIndex))
		})
		LOG_INFO("BENCHMARK_DELETE", "Deleted %d temporary files from the storage", chunkCount)
	}

	return true
}
