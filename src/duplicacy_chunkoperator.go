// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// These are operations that ChunkOperator will perform.
const (
	ChunkOperationDownload  = 0
	ChunkOperationUpload    = 1
	ChunkOperationDelete    = 2
	ChunkOperationFossilize = 3
	ChunkOperationResurrect = 4
	ChunkOperationFind = 5
)

// ChunkTask is used to pass parameters for different kinds of chunk operations.
type ChunkTask struct {
	operation  int    // The type of operation
	chunkID    string // The chunk id
	chunkHash  string // The chunk hash
	chunkIndex int    // The chunk index
	filePath   string // The path of the chunk file; it may be empty

	isMetadata bool
	chunk      *Chunk

	completionFunc func(chunk *Chunk, chunkIndex int)
}

// ChunkOperator is capable of performing multi-threaded operations on chunks.
type ChunkOperator struct {
	config *Config               // Associated config
	storage Storage              // This storage
	snapshotCache  *FileStorage
	showStatistics bool
	threads  int                 // Number of threads
	taskQueue chan ChunkTask     // Operating goroutines are waiting on this channel for input
	stopChannel chan bool        // Used to stop all the goroutines

	numberOfActiveTasks int64    // The number of chunks that are being operated on

	fossils []string             // For fossilize operation, the paths of the fossils are stored in this slice
	collectionLock *sync.Mutex   // The lock for accessing 'fossils'

	startTime int64              // The time it starts downloading
	totalChunkSize int64         // Total chunk size
	downloadedChunkSize int64    // Downloaded chunk size

	allowFailures bool           // Whether to fail on download error, or continue
	NumberOfFailedChunks int64   // The number of chunks that can't be downloaded

	UploadCompletionFunc func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int)
}

// CreateChunkOperator creates a new ChunkOperator.
func CreateChunkOperator(config *Config, storage Storage, snapshotCache *FileStorage, showStatistics bool, threads int, allowFailures bool) *ChunkOperator {

	operator := &ChunkOperator{
		config: config,
		storage: storage,
		snapshotCache: snapshotCache,
		showStatistics: showStatistics,
		threads: threads,

		taskQueue:   make(chan ChunkTask, threads),
		stopChannel: make(chan bool),

		collectionLock: &sync.Mutex{},
		startTime: time.Now().Unix(),
		allowFailures: allowFailures,
	}

	// Start the operator goroutines
	for i := 0; i < operator.threads; i++ {
		go func(threadIndex int) {
			defer CatchLogException()
			for {
				select {
				case task := <-operator.taskQueue:
					operator.Run(threadIndex, task)
				case <-operator.stopChannel:
					return
				}
			}
		}(i)
	}

	return operator
}

func (operator *ChunkOperator) Stop() {
	if atomic.LoadInt64(&operator.numberOfActiveTasks) < 0 {
		return
	}

	for atomic.LoadInt64(&operator.numberOfActiveTasks) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < operator.threads; i++ {
		operator.stopChannel <- false
	}

	// Assign -1 to numberOfActiveTasks so Stop() can be called multiple times
	atomic.AddInt64(&operator.numberOfActiveTasks, int64(-1))
}

func (operator *ChunkOperator) WaitForCompletion() {

	for atomic.LoadInt64(&operator.numberOfActiveTasks) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

func (operator *ChunkOperator) AddTask(operation int, chunkID string, chunkHash string, filePath string, chunkIndex int, chunk *Chunk, isMetadata bool, completionFunc func(*Chunk, int))  {

	task := ChunkTask {
		operation:  operation,
		chunkID:    chunkID,
		chunkHash:  chunkHash,
		chunkIndex: chunkIndex,
		filePath:   filePath,
		chunk:      chunk,
		isMetadata: isMetadata,
		completionFunc: completionFunc,
	}

	operator.taskQueue <- task
	atomic.AddInt64(&operator.numberOfActiveTasks, int64(1))

	return
}

func (operator *ChunkOperator) Download(chunkHash string, chunkIndex int, isMetadata bool) *Chunk {
	chunkID := operator.config.GetChunkIDFromHash(chunkHash)
	completionChannel := make(chan *Chunk)
	completionFunc := func(chunk *Chunk, chunkIndex int) {
		completionChannel <- chunk
	}
	operator.AddTask(ChunkOperationDownload, chunkID, chunkHash, "", chunkIndex, nil, isMetadata, completionFunc)
	return <- completionChannel
}

func (operator *ChunkOperator) DownloadAsync(chunkHash string, chunkIndex int, isMetadata bool, completionFunc func(*Chunk, int)) {
	chunkID := operator.config.GetChunkIDFromHash(chunkHash)
	operator.AddTask(ChunkOperationDownload, chunkID, chunkHash, "", chunkIndex, nil, isMetadata, completionFunc)
}

func (operator *ChunkOperator) Upload(chunk *Chunk, chunkIndex int, isMetadata bool) {
	chunkHash := chunk.GetHash()
	chunkID := operator.config.GetChunkIDFromHash(chunkHash)
	operator.AddTask(ChunkOperationUpload, chunkID, chunkHash, "", chunkIndex, chunk, isMetadata, nil)
}

func (operator *ChunkOperator) Delete(chunkID string, filePath string) {
	operator.AddTask(ChunkOperationDelete, chunkID, "", filePath, 0, nil, false, nil)
}

func (operator *ChunkOperator) Fossilize(chunkID string, filePath string) {
	operator.AddTask(ChunkOperationFossilize, chunkID, "", filePath, 0, nil, false, nil)
}

func (operator *ChunkOperator) Resurrect(chunkID string, filePath string) {
	operator.AddTask(ChunkOperationResurrect, chunkID, "", filePath, 0, nil, false, nil)
}

func (operator *ChunkOperator) Run(threadIndex int, task ChunkTask) {
	defer func() {
		atomic.AddInt64(&operator.numberOfActiveTasks, int64(-1))
	}()

	if task.operation == ChunkOperationDownload {
		operator.DownloadChunk(threadIndex, task)
		return
	} else if task.operation == ChunkOperationUpload {
		operator.UploadChunk(threadIndex, task)
		return
	}

	// task.filePath may be empty.  If so, find the chunk first.
	if task.operation == ChunkOperationDelete || task.operation == ChunkOperationFossilize {
		if task.filePath == "" {
			filePath, exist, _, err := operator.storage.FindChunk(threadIndex, task.chunkID, false)
			if err != nil {
				LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", task.chunkID, err)
				return
			} else if !exist {
				if task.operation == ChunkOperationDelete {
					LOG_WARN("CHUNK_FIND", "Chunk %s does not exist in the storage", task.chunkID)
					return
				}

				fossilPath, exist, _, _ := operator.storage.FindChunk(threadIndex, task.chunkID, true)
				if exist {
					LOG_WARN("CHUNK_FOSSILIZE", "Chunk %s is already a fossil", task.chunkID)
					operator.collectionLock.Lock()
					operator.fossils = append(operator.fossils, fossilPath)
					operator.collectionLock.Unlock()
				} else {
					LOG_ERROR("CHUNK_FIND", "Chunk %s does not exist in the storage", task.chunkID)
				}
				return
			}
			task.filePath = filePath
		}
	}

	if task.operation == ChunkOperationFind {
		_, exist, _, err := operator.storage.FindChunk(threadIndex, task.chunkID, false)
		if err != nil {
			LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", task.chunkID, err)
		} else if !exist {
			LOG_ERROR("CHUNK_FIND", "Chunk %s does not exist in the storage", task.chunkID)
		} else {
			LOG_DEBUG("CHUNK_FIND", "Chunk %s exists in the storage", task.chunkID)
		}
	} else if task.operation == ChunkOperationDelete {
		err := operator.storage.DeleteFile(threadIndex, task.filePath)
		if err != nil {
			LOG_WARN("CHUNK_DELETE", "Failed to remove the file %s: %v", task.filePath, err)
		} else {
			if task.chunkID != "" {
				LOG_INFO("CHUNK_DELETE", "The chunk %s has been permanently removed", task.chunkID)
			} else {
				LOG_INFO("CHUNK_DELETE", "Deleted file %s from the storage", task.filePath)
			}
		}
	} else if task.operation == ChunkOperationFossilize {

		fossilPath := task.filePath + ".fsl"

		err := operator.storage.MoveFile(threadIndex, task.filePath, fossilPath)
		if err != nil {
			if _, exist, _, _ := operator.storage.FindChunk(threadIndex, task.chunkID, true); exist {
				err := operator.storage.DeleteFile(threadIndex, task.filePath)
				if err == nil {
					LOG_TRACE("CHUNK_DELETE", "Deleted chunk file %s as the fossil already exists", task.chunkID)
				}
				operator.collectionLock.Lock()
				operator.fossils = append(operator.fossils, fossilPath)
				operator.collectionLock.Unlock()
			} else {
				LOG_ERROR("CHUNK_DELETE", "Failed to fossilize the chunk %s: %v", task.chunkID, err)
			}
		} else {
			LOG_TRACE("CHUNK_FOSSILIZE", "The chunk %s has been marked as a fossil", task.chunkID)
			operator.collectionLock.Lock()
			operator.fossils = append(operator.fossils, fossilPath)
			operator.collectionLock.Unlock()
		}
	} else if task.operation == ChunkOperationResurrect {
		chunkPath, exist, _, err := operator.storage.FindChunk(threadIndex, task.chunkID, false)
		if err != nil {
			LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", task.chunkID, err)
		}

		if exist {
			operator.storage.DeleteFile(threadIndex, task.filePath)
			LOG_INFO("FOSSIL_RESURRECT", "The chunk %s already exists", task.chunkID)
		} else {
			err := operator.storage.MoveFile(threadIndex, task.filePath, chunkPath)
			if err != nil {
				LOG_ERROR("FOSSIL_RESURRECT", "Failed to resurrect the chunk %s from the fossil %s: %v",
					task.chunkID, task.filePath, err)
			} else {
				LOG_INFO("FOSSIL_RESURRECT", "The chunk %s has been resurrected", task.filePath)
			}
		}
	}
}

// Download downloads a chunk from the storage.
func (operator *ChunkOperator) DownloadChunk(threadIndex int, task ChunkTask) {

	cachedPath := ""
	chunk := operator.config.GetChunk()
	chunk.isMetadata = task.isMetadata
	chunkID := task.chunkID

	defer func() {
		if chunk != nil {
			operator.config.PutChunk(chunk)
		}
	} ()

	if task.isMetadata && operator.snapshotCache != nil {

		var exist bool
		var err error

		// Reset the chunk with a hasher -- we're reading from the cache where chunk are not encrypted or compressed
		chunk.Reset(true)

		cachedPath, exist, _, err = operator.snapshotCache.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			LOG_WARN("DOWNLOAD_CACHE", "Failed to find the cache path for the chunk %s: %v", chunkID, err)
		} else if exist {
			err = operator.snapshotCache.DownloadFile(0, cachedPath, chunk)
			if err != nil {
				LOG_WARN("DOWNLOAD_CACHE", "Failed to load the chunk %s from the snapshot cache: %v", chunkID, err)
			} else {
				actualChunkID := chunk.GetID()
				if actualChunkID != chunkID {
					LOG_WARN("DOWNLOAD_CACHE_CORRUPTED",
						"The chunk %s load from the snapshot cache has a hash id of %s", chunkID, actualChunkID)
				} else {
					LOG_DEBUG("CHUNK_CACHE", "Chunk %s has been loaded from the snapshot cache", chunkID)

					task.completionFunc(chunk, task.chunkIndex)
					chunk = nil
					return
				}
			}
		}
	}

	// Reset the chunk without a hasher -- the downloaded content will be encrypted and/or compressed and the hasher
	// will be set up before the encryption
	chunk.Reset(false)
	chunk.isMetadata = task.isMetadata

	// If failures are allowed, complete the task properly
	completeFailedChunk := func() {

		atomic.AddInt64(&operator.NumberOfFailedChunks, 1)
		if operator.allowFailures {
			task.completionFunc(chunk, task.chunkIndex)
		}
	}

	const MaxDownloadAttempts = 3
	for downloadAttempt := 0; ; downloadAttempt++ {

		// Find the chunk by ID first.
		chunkPath, exist, _, err := operator.storage.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			completeFailedChunk()
			LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to find the chunk %s: %v", chunkID, err)
			return
		}

		if !exist {
			// No chunk is found.  Have to find it in the fossil pool again.
			fossilPath, exist, _, err := operator.storage.FindChunk(threadIndex, chunkID, true)
			if err != nil {
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to find the chunk %s: %v", chunkID, err)
				return
			}

			if !exist {

				retry := false

				// Retry for Hubic or WebDAV as it may return 404 even when the chunk exists
				if _, ok := operator.storage.(*HubicStorage); ok {
					retry = true
				}

				if _, ok := operator.storage.(*WebDAVStorage); ok {
					retry = true
				}

				if retry && downloadAttempt < MaxDownloadAttempts {
					LOG_WARN("DOWNLOAD_RETRY", "Failed to find the chunk %s; retrying", chunkID)
					continue
				}

				// A chunk is not found.  This is a serious error and hopefully it will never happen.
				completeFailedChunk()
				if err != nil {
					LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Chunk %s can't be found: %v", chunkID, err)
				} else {
					LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Chunk %s can't be found", chunkID)
				}
				return
			}

			// We can't download the fossil directly.  We have to turn it back into a regular chunk and try
			// downloading again.
			err = operator.storage.MoveFile(threadIndex, fossilPath, chunkPath)
			if err != nil {
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to resurrect chunk %s: %v", chunkID, err)
				return
			}

			LOG_WARN("DOWNLOAD_RESURRECT", "Fossil %s has been resurrected", chunkID)
			continue
		}

		err = operator.storage.DownloadFile(threadIndex, chunkPath, chunk)
		if err != nil {
			_, isHubic := operator.storage.(*HubicStorage)
			// Retry on EOF or if it is a Hubic backend as it may return 404 even when the chunk exists
			if (err == io.ErrUnexpectedEOF || isHubic) && downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "Failed to download the chunk %s: %v; retrying", chunkID, err)
				chunk.Reset(false)
				chunk.isMetadata = task.isMetadata
				continue
			} else {
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to download the chunk %s: %v", chunkID, err)
				return
			}
		}

		err = chunk.Decrypt(operator.config.ChunkKey, task.chunkHash)
		if err != nil {
			if downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "Failed to decrypt the chunk %s: %v; retrying", chunkID, err)
				chunk.Reset(false)
				chunk.isMetadata = task.isMetadata
				continue
			} else {
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_DECRYPT", "Failed to decrypt the chunk %s: %v", chunkID, err)
				return
			}
		}

		actualChunkID := chunk.GetID()
		if actualChunkID != chunkID {
			if downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "The chunk %s has a hash id of %s; retrying", chunkID, actualChunkID)
				chunk.Reset(false)
				chunk.isMetadata = task.isMetadata
				continue
			} else {
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_CORRUPTED", "The chunk %s has a hash id of %s", chunkID, actualChunkID)
				return
			}
		}

		break
	}

	if chunk.isMetadata && len(cachedPath) > 0 {
		// Save a copy to the local snapshot cache
		err := operator.snapshotCache.UploadFile(threadIndex, cachedPath, chunk.GetBytes())
		if err != nil {
			LOG_WARN("DOWNLOAD_CACHE", "Failed to add the chunk %s to the snapshot cache: %v", chunkID, err)
		}
	}

	downloadedChunkSize := atomic.AddInt64(&operator.downloadedChunkSize, int64(chunk.GetLength()))

	if (operator.showStatistics || IsTracing()) && operator.totalChunkSize > 0 {

		now := time.Now().Unix()
		if now <= operator.startTime {
			now = operator.startTime + 1
		}
		speed := downloadedChunkSize / (now - operator.startTime)
		remainingTime := int64(0)
		if speed > 0 {
			remainingTime = (operator.totalChunkSize-downloadedChunkSize)/speed + 1
		}
		percentage := float32(downloadedChunkSize * 1000 / operator.totalChunkSize)
		LOG_INFO("DOWNLOAD_PROGRESS", "Downloaded chunk %d size %d, %sB/s %s %.1f%%",
			task.chunkIndex+1, chunk.GetLength(),
			PrettySize(speed), PrettyTime(remainingTime), percentage/10)
	} else {
		LOG_DEBUG("CHUNK_DOWNLOAD", "Chunk %s has been downloaded", chunkID)
	}

	task.completionFunc(chunk, task.chunkIndex)
	chunk = nil
	return
}

// UploadChunk is called by the task goroutines to perform the actual uploading
func (operator *ChunkOperator) UploadChunk(threadIndex int, task ChunkTask) bool {

	chunk := task.chunk
	chunkID := task.chunkID
	chunkSize := chunk.GetLength()

	// For a snapshot chunk, verify that its chunk id is correct
	if task.isMetadata {
		chunk.VerifyID()
	}

	if task.isMetadata && operator.storage.IsCacheNeeded() {
		// Save a copy to the local snapshot.
		chunkPath, exist, _, err := operator.snapshotCache.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to find the cache path for the chunk %s: %v", chunkID, err)
		} else if exist {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s already exists in the snapshot cache", chunkID)
		} else if err = operator.snapshotCache.UploadFile(threadIndex, chunkPath, chunk.GetBytes()); err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to save the chunk %s to the snapshot cache: %v", chunkID, err)
		} else {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s has been saved to the snapshot cache", chunkID)
		}
	}

	// This returns the path the chunk file should be at.
	chunkPath, exist, _, err := operator.storage.FindChunk(threadIndex, chunkID, false)
	if err != nil {
		LOG_ERROR("UPLOAD_CHUNK", "Failed to find the path for the chunk %s: %v", chunkID, err)
		return false
	}

	if exist {
		// Chunk deduplication by name in effect here.
		LOG_DEBUG("CHUNK_DUPLICATE", "Chunk %s already exists", chunkID)

		operator.UploadCompletionFunc(chunk, task.chunkIndex, false, chunkSize, 0)
		return false
	}

	// Encrypt the chunk only after we know that it must be uploaded.
	err = chunk.Encrypt(operator.config.ChunkKey, chunk.GetHash(), task.isMetadata)
	if err != nil {
		LOG_ERROR("UPLOAD_CHUNK", "Failed to encrypt the chunk %s: %v", chunkID, err)
		return false
	}

	if !operator.config.dryRun {
		err = operator.storage.UploadFile(threadIndex, chunkPath, chunk.GetBytes())
		if err != nil {
			LOG_ERROR("UPLOAD_CHUNK", "Failed to upload the chunk %s: %v", chunkID, err)
			return false
		}
		LOG_DEBUG("CHUNK_UPLOAD", "Chunk %s has been uploaded", chunkID)
	} else {
		LOG_DEBUG("CHUNK_UPLOAD", "Uploading was skipped for chunk %s", chunkID)
	}

	operator.UploadCompletionFunc(chunk, task.chunkIndex, false, chunkSize, chunk.GetLength())
	return true
}