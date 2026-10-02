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

	completionLock *sync.Mutex   // Guards the three fields below
	numberOfActiveTasks int64    // The number of chunks that are being operated on, queued or running
	stopped bool                 // Set by the first Stop, so that a second one does nothing
	idleCond *sync.Cond          // Signalled when the last active task finishes, and by Stop

	fossils []string             // For fossilize operation, the paths of the fossils are stored in this slice
	collectionLock *sync.Mutex   // The lock for accessing 'fossils'

	startTime int64              // The time it starts downloading
	totalChunkSize int64         // Total chunk size
	downloadedChunkSize int64    // Downloaded chunk size

	allowFailures bool           // Whether to fail on download error, or continue
	NumberOfFailedChunks int64   // The number of chunks that can't be downloaded

	rewriteChunks bool           // Whether to rewrite corrupted chunks when erasure coding is enabled

	UploadCompletionFunc func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int)

	// Set when every chunk given to the uploader is known to be absent from the storage, so that it doesn't have to
	// ask the storage before uploading.  'copy' sets this after filtering out the chunks the destination holds;
	// 'backup' leaves it unset because its check is what skips a chunk another client has uploaded.
	skipChunkCheck bool

	// Set when this operator reads the stored form of the chunks instead of their plaintext, because the storage they
	// are copied to stores them identically.  'copy' sets this when the two storages share their encryption,
	// compression and erasure-coding parameters, so that the chunks don't have to be decoded and encoded again.
	rawData bool
}

// CreateChunkOperator creates a new ChunkOperator.
func CreateChunkOperator(config *Config, storage Storage, snapshotCache *FileStorage, showStatistics bool, rewriteChunks bool, threads int,
						 allowFailures bool) *ChunkOperator {

	operator := &ChunkOperator{
		config: config,
		storage: storage,
		snapshotCache: snapshotCache,
		showStatistics: showStatistics,
		threads: threads,

		taskQueue:   make(chan ChunkTask, threads),
		stopChannel: make(chan bool),

		collectionLock: &sync.Mutex{},
		completionLock: &sync.Mutex{},
		startTime: time.Now().Unix(),
		allowFailures: allowFailures,
		rewriteChunks: rewriteChunks,
	}

	operator.idleCond = sync.NewCond(operator.completionLock)

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

// Stop waits for the outstanding tasks and then stops the worker goroutines.  It only takes effect the first time it
// is called, so that the callers that stop the operator from a deferred function can also stop it explicitly.
func (operator *ChunkOperator) Stop() {

	operator.WaitForCompletion()

	operator.completionLock.Lock()
	if operator.stopped {
		operator.completionLock.Unlock()
		return
	}
	operator.stopped = true
	operator.completionLock.Unlock()

	for i := 0; i < operator.threads; i++ {
		operator.stopChannel <- false
	}
}

// WaitForCompletion waits until every task that has been added has finished.  The wait is woken by the task that
// finishes last rather than by a timer, so it costs nothing on a fast storage; a task that is still being queued
// counts as outstanding, which is why AddTask increments the counter before handing the task to the workers.
func (operator *ChunkOperator) WaitForCompletion() {

	operator.completionLock.Lock()
	for operator.numberOfActiveTasks > 0 {
		operator.idleCond.Wait()
	}
	operator.completionLock.Unlock()
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

	// Counted before the task is handed over, so that a caller waiting for completion can not observe the operator as
	// idle while this task is on its way to the workers.
	operator.completionLock.Lock()
	operator.numberOfActiveTasks++
	operator.completionLock.Unlock()

	operator.taskQueue <- task

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
		operator.completionLock.Lock()
		operator.numberOfActiveTasks--
		if operator.numberOfActiveTasks == 0 {
			operator.idleCond.Broadcast()
		}
		operator.completionLock.Unlock()
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

	if task.isMetadata && operator.snapshotCache != nil && operator.storage.IsCacheNeeded() {

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
			chunk.isBroken = true
			task.completionFunc(chunk, task.chunkIndex)
		}
	}

	chunkPath := ""
	fossilPath := ""
	filePath := ""

	const MaxDownloadAttempts = 3
	for downloadAttempt := 0; ; downloadAttempt++ {

		exist := false
		var err error

		// Find the chunk by ID first.
		chunkPath, exist, _, err = operator.storage.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			completeFailedChunk()
			LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to find the chunk %s: %v", chunkID, err)
			return
		}

		if exist {
			filePath = chunkPath
		} else {
			// No chunk is found.  Have to find it in the fossil pool again.
			fossilPath, exist, _, err = operator.storage.FindChunk(threadIndex, chunkID, true)
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

			filePath = fossilPath
			LOG_WARN("DOWNLOAD_FOSSIL", "Chunk %s is a fossil", chunkID)
		}

		err = operator.storage.DownloadFile(threadIndex, filePath, chunk)
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

		// When the destination storage stores the chunk identically, the downloaded bytes are exactly what must be
		// uploaded, so they are passed on as they are instead of being decrypted and encrypted again.  Only the hash
		// and id they were stored under are recorded, since they can't be recomputed: the raw flag is left to the
		// caller, which is the one that turns the bytes into the chunk to upload.
		if operator.rawData {
			chunk.SetStoredHash(task.chunkHash)
			break
		}

		rewriteNeeded := false
		err, rewriteNeeded = chunk.Decrypt(operator.config.ChunkKey, task.chunkHash)
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

		if rewriteNeeded && operator.rewriteChunks {

			if filePath != fossilPath {
				fossilPath = filePath + ".fsl"
				err := operator.storage.MoveFile(threadIndex, chunkPath, fossilPath)
				if err != nil {
					LOG_WARN("CHUNK_REWRITE", "Failed to fossilize the chunk %s: %v", task.chunkID, err)
				} else {
					LOG_TRACE("CHUNK_REWRITE", "The existing chunk %s has been marked as a fossil for rewrite", task.chunkID)
					operator.collectionLock.Lock()
					operator.fossils = append(operator.fossils, fossilPath)
					operator.collectionLock.Unlock()
				}
			}

			newChunk := operator.config.GetChunk()
			newChunk.Reset(true)
			newChunk.Write(chunk.GetBytes())
			// Encrypt the chunk only after we know that it must be uploaded.
			err = newChunk.Encrypt(operator.config.ChunkKey, chunk.GetHash(), task.isMetadata)
			if err == nil {
				// Re-upload the chunk
				err = operator.storage.UploadFile(threadIndex, chunkPath, newChunk.GetBytes())
				if err != nil {
					LOG_WARN("CHUNK_REWRITE", "Failed to re-upload the chunk %s: %v", chunkID, err)
				} else {
					LOG_INFO("CHUNK_REWRITE", "The chunk %s has been re-uploaded", chunkID)
				}
			}
			operator.config.PutChunk(newChunk)
		}

		break
	}

	if chunk.isMetadata && !chunk.isRawData && len(cachedPath) > 0 {
		// Save a copy to the local snapshot cache.  Skipping fsync is safe for the chunk cache and only for it: the
		// entry is read back through the id check at the top of this function, so a write torn by a crash is rejected
		// and the chunk is fetched from the storage instead.  This write happens once per metadata chunk, and
		// fsyncing it is the bulk of the cost of populating the cache.
		err := operator.snapshotCache.UploadFileNoSync(threadIndex, cachedPath, chunk.GetBytes())
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

	// The data in the buffer is checked against the chunk id by Encrypt() below, unless the chunk already holds the
	// stored form and is uploaded as it is.

	if task.isMetadata && !chunk.isRawData && operator.snapshotCache != nil && operator.storage.IsCacheNeeded() {
		// Save a copy to the local snapshot.  As in DownloadChunk, the chunk cache is safe to write without fsync
		// because a torn entry fails the id check on the next read and is re-fetched from the storage.
		chunkPath, exist, _, err := operator.snapshotCache.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to find the cache path for the chunk %s: %v", chunkID, err)
		} else if exist {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s already exists in the snapshot cache", chunkID)
		} else if err = operator.snapshotCache.UploadFileNoSync(threadIndex, chunkPath, chunk.GetBytes()); err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to save the chunk %s to the snapshot cache: %v", chunkID, err)
		} else {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s has been saved to the snapshot cache", chunkID)
		}
	}

	// When set, the caller has established that the chunk is absent and the path is derived locally.  Otherwise
	// FindChunk returns the path and reports whether the chunk is already there, which is how a chunk uploaded by
	// another client is skipped.
	chunkPath := ""
	exist := false
	var err error

	if operator.skipChunkCheck {
		chunkPath, err = operator.storage.ChunkPath(chunkID)
		if err != nil {
			LOG_ERROR("UPLOAD_CHUNK", "Failed to derive the path for the chunk %s: %v", chunkID, err)
			return false
		}
	} else {
		chunkPath, exist, _, err = operator.storage.FindChunk(threadIndex, chunkID, false)
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
	}

	// Encrypt the chunk only after we know that it must be uploaded.  A chunk that already holds the stored form is
	// uploaded as it is; it was produced by copying the same chunk from a storage that stores it identically.
	if !chunk.isRawData {
		err = chunk.Encrypt(operator.config.ChunkKey, chunk.GetHash(), task.isMetadata)
		if err != nil {
			LOG_ERROR("UPLOAD_CHUNK", "Failed to encrypt the chunk %s: %v", chunkID, err)
			return false
		}
	} else {
		// Encrypt would have performed this check; it still applies because the buffer is what is about to be stored
		chunk.VerifyChecksum()
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