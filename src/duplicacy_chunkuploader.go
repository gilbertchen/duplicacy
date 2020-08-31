// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"sync/atomic"
	"time"
)

// ChunkUploadTask represents a chunk to be uploaded.
type ChunkUploadTask struct {
	chunk      *Chunk
	chunkIndex int
}

// ChunkUploader uploads chunks to the storage using one or more uploading goroutines.  Chunks are added
// by the call to StartChunk(), and then passed to the uploading goroutines.  The completion function is
// called when the downloading is completed.  Note that ChunkUploader does not release chunks to the
// chunk pool; instead
type ChunkUploader struct {
	config        *Config              // Associated config
	storage       Storage              // Download from this storage
	snapshotCache *FileStorage         // Used as cache if not nil; usually for uploading snapshot chunks
	threads       int                  // Number of uploading goroutines
	taskQueue     chan ChunkUploadTask // Uploading goroutines are listening on this channel for upload jobs
	stopChannel   chan bool            // Used to terminate uploading goroutines

	numberOfUploadingTasks int32 // The number of uploading tasks

	// Uploading goroutines call this function after having downloaded chunks
	completionFunc func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int)
}

// CreateChunkUploader creates a chunk uploader.
func CreateChunkUploader(config *Config, storage Storage, snapshotCache *FileStorage, threads int,
	completionFunc func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int)) *ChunkUploader {
	uploader := &ChunkUploader{
		config:         config,
		storage:        storage,
		snapshotCache:  snapshotCache,
		threads:        threads,
		taskQueue:      make(chan ChunkUploadTask, 1),
		stopChannel:    make(chan bool),
		completionFunc: completionFunc,
	}

	return uploader
}

// Starts starts uploading goroutines.
func (uploader *ChunkUploader) Start() {
	for i := 0; i < uploader.threads; i++ {
		go func(threadIndex int) {
			defer CatchLogException()
			for {
				select {
				case task := <-uploader.taskQueue:
					uploader.Upload(threadIndex, task)
				case <-uploader.stopChannel:
					return
				}
			}
		}(i)
	}
}

// StartChunk sends a chunk to be uploaded to  a waiting uploading goroutine.  It may block if all uploading goroutines are busy.
func (uploader *ChunkUploader) StartChunk(chunk *Chunk, chunkIndex int) {
	atomic.AddInt32(&uploader.numberOfUploadingTasks, 1)
	uploader.taskQueue <- ChunkUploadTask{
		chunk:      chunk,
		chunkIndex: chunkIndex,
	}
}

// Stop stops all uploading goroutines.
func (uploader *ChunkUploader) Stop() {
	for atomic.LoadInt32(&uploader.numberOfUploadingTasks) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < uploader.threads; i++ {
		uploader.stopChannel <- false
	}
}

// Upload is called by the uploading goroutines to perform the actual uploading
func (uploader *ChunkUploader) Upload(threadIndex int, task ChunkUploadTask) bool {

	chunk := task.chunk
	chunkSize := chunk.GetLength()
	chunkID := chunk.GetID()
	var stropt StorageOption

	// For a snapshot chunk, verify that its chunk id is correct
	if uploader.snapshotCache != nil {
		chunk.VerifyID()
		// initialize StorageClass for meta chunk
		if st, ok := uploader.storage.(*S3CStorage); ok {
			stropt = st.stOptionMeta
		}
	} else {
		// initialize StorageClass for data chunk
		if st, ok := uploader.storage.(*S3CStorage); ok {
			stropt = st.stOptionData
		}
	}

	if uploader.snapshotCache != nil && uploader.storage.IsCacheNeeded() {
		// Save a copy to the local snapshot.
		chunkPath, exist, _, err := uploader.snapshotCache.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to find the cache path for the chunk %s: %v", chunkID, err)
		} else if exist {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s already exists in the snapshot cache", chunkID)
		} else if err = uploader.snapshotCache.UploadFile(threadIndex, chunkPath, chunk.GetBytes(), stropt); err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to save the chunk %s to the snapshot cache: %v", chunkID, err)
		} else {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s has been saved to the snapshot cache", chunkID)
		}
	}

	// This returns the path the chunk file should be at.
	chunkPath, exist, _, err := uploader.storage.FindChunk(threadIndex, chunkID, false)
	if err != nil {
		LOG_ERROR("UPLOAD_CHUNK", "Failed to find the path for the chunk %s: %v", chunkID, err)
		return false
	}

	if exist {
		// Chunk deduplication by name in effect here.
		LOG_DEBUG("CHUNK_DUPLICATE", "Chunk %s already exists", chunkID)

		uploader.completionFunc(chunk, task.chunkIndex, true, chunkSize, 0)
		atomic.AddInt32(&uploader.numberOfUploadingTasks, -1)
		return false
	}

	// Encrypt the chunk only after we know that it must be uploaded.
	err = chunk.Encrypt(uploader.config.ChunkKey, chunk.GetHash(), uploader.snapshotCache != nil)
	if err != nil {
		LOG_ERROR("UPLOAD_CHUNK", "Failed to encrypt the chunk %s: %v", chunkID, err)
		return false
	}

	if !uploader.config.dryRun {
		err = uploader.storage.UploadFile(threadIndex, chunkPath, chunk.GetBytes(), stropt)
		if err != nil {
			LOG_ERROR("UPLOAD_CHUNK", "Failed to upload the chunk %s: %v", chunkID, err)
			return false
		}
		LOG_DEBUG("CHUNK_UPLOAD", "Chunk %s has been uploaded", chunkID)
	} else {
		LOG_DEBUG("CHUNK_UPLOAD", "Uploading was skipped for chunk %s", chunkID)
	}

	uploader.completionFunc(chunk, task.chunkIndex, false, chunkSize, chunk.GetLength())
	atomic.AddInt32(&uploader.numberOfUploadingTasks, -1)
	return true
}
