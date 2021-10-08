// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"io"
	"sync/atomic"
	"time"
)

// ChunkDownloadTask encapsulates information need to download a chunk.
type ChunkDownloadTask struct {
	chunk         *Chunk // The chunk that will be downloaded; initially nil
	chunkIndex    int    // The index of this chunk in the chunk list
	chunkHash     string // The chunk hash
	chunkLength   int    // The length of the chunk; may be zero
	needed        bool   // Whether this chunk can be skipped if a local copy exists
	isDownloading bool   // 'true' means the chunk has been downloaded or is being downloaded
}

// ChunkDownloadCompletion represents the nofication when a chunk has been downloaded.
type ChunkDownloadCompletion struct {
	chunkIndex int    // The index of this chunk in the chunk list
	chunk      *Chunk // The chunk that has been downloaded
}

// ChunkDownloader is capable of performing multi-threaded downloading.  Chunks to be downloaded are first organized
// as a list of ChunkDownloadTasks, with only the chunkHash field initialized.  When a chunk is needed, the
// corresponding ChunkDownloadTask is sent to the dowloading goroutine.  Once a chunk is downloaded, it will be
// inserted in the completed task list.
type ChunkDownloader struct {
	totalChunkSize            int64 // Total chunk size
	downloadedChunkSize       int64 // Downloaded chunk size

	config         *Config      // Associated config
	storage        Storage      // Download from this storage
	snapshotCache  *FileStorage // Used as cache if not nil; usually for downloading snapshot chunks
	showStatistics bool         // Show a stats log for each chunk if true
	threads        int          // Number of threads
	allowFailures  bool         // Whether to failfast on download error, or continue

	taskList       []ChunkDownloadTask // The list of chunks to be downloaded
	completedTasks map[int]bool        // Store downloaded chunks
	lastChunkIndex int                 // a monotonically increasing number indicating the last chunk to be downloaded

	taskQueue         chan ChunkDownloadTask       // Downloading goroutines are waiting on this channel for input
	stopChannel       chan bool                    // Used to stop the dowloading goroutines
	completionChannel chan ChunkDownloadCompletion // A downloading goroutine sends back the chunk via this channel after downloading

	startTime                 int64 // The time it starts downloading
	numberOfDownloadedChunks  int   // The number of chunks that have been downloaded
	numberOfDownloadingChunks int   // The number of chunks still being downloaded
	numberOfActiveChunks      int   // The number of chunks that is being downloaded or has been downloaded but not reclaimed

	NumberOfFailedChunks      int   // The number of chunks that can't be downloaded
}

func CreateChunkDownloader(config *Config, storage Storage, snapshotCache *FileStorage, showStatistics bool, threads int, allowFailures bool) *ChunkDownloader {
	downloader := &ChunkDownloader{
		config:         config,
		storage:        storage,
		snapshotCache:  snapshotCache,
		showStatistics: showStatistics,
		threads:        threads,
		allowFailures:  allowFailures,

		taskList:       nil,
		completedTasks: make(map[int]bool),
		lastChunkIndex: 0,

		taskQueue:         make(chan ChunkDownloadTask, threads),
		stopChannel:       make(chan bool),
		completionChannel: make(chan ChunkDownloadCompletion),

		startTime: time.Now().Unix(),
	}

	// Start the downloading goroutines
	for i := 0; i < downloader.threads; i++ {
		go func(threadIndex int) {
			defer CatchLogException()
			for {
				select {
				case task := <-downloader.taskQueue:
					downloader.Download(threadIndex, task)
				case <-downloader.stopChannel:
					return
				}
			}
		}(i)
	}

	return downloader
}

// AddFiles adds chunks needed by the specified files to the download list.
func (downloader *ChunkDownloader) AddFiles(snapshot *Snapshot, files []*Entry) {

	downloader.taskList = nil
	lastChunkIndex := -1
	maximumChunks := 0
	downloader.totalChunkSize = 0
	for _, file := range files {
		if file.Size == 0 {
			continue
		}
		for i := file.StartChunk; i <= file.EndChunk; i++ {
			if lastChunkIndex != i {
				task := ChunkDownloadTask{
					chunkIndex:  len(downloader.taskList),
					chunkHash:   snapshot.ChunkHashes[i],
					chunkLength: snapshot.ChunkLengths[i],
					needed:      false,
				}
				downloader.taskList = append(downloader.taskList, task)
				downloader.totalChunkSize += int64(snapshot.ChunkLengths[i])
			} else {
				downloader.taskList[len(downloader.taskList)-1].needed = true
			}
			lastChunkIndex = i
		}
		file.StartChunk = len(downloader.taskList) - (file.EndChunk - file.StartChunk) - 1
		file.EndChunk = len(downloader.taskList) - 1
		if file.EndChunk-file.StartChunk > maximumChunks {
			maximumChunks = file.EndChunk - file.StartChunk
		}
	}
}

// AddChunk adds a single chunk the download list.
func (downloader *ChunkDownloader) AddChunk(chunkHash string) int {

	task := ChunkDownloadTask{
		chunkIndex:    len(downloader.taskList),
		chunkHash:     chunkHash,
		chunkLength:   0,
		needed:        true,
		isDownloading: false,
	}
	downloader.taskList = append(downloader.taskList, task)
	if downloader.numberOfActiveChunks < downloader.threads {
		downloader.taskQueue <- task
		downloader.numberOfDownloadingChunks++
		downloader.numberOfActiveChunks++
		downloader.taskList[len(downloader.taskList)-1].isDownloading = true
	}
	return len(downloader.taskList) - 1
}

// Prefetch adds up to 'threads' chunks needed by a file to the download list
func (downloader *ChunkDownloader) Prefetch(file *Entry) {

	// Any chunks before the first chunk of this filea are not needed any more, so they can be reclaimed.
	downloader.Reclaim(file.StartChunk)

	for i := file.StartChunk; i <= file.EndChunk; i++ {
		task := &downloader.taskList[i]
		if task.needed {
			if !task.isDownloading {
				if downloader.numberOfActiveChunks >= downloader.threads {
					return
				}

				LOG_DEBUG("DOWNLOAD_PREFETCH", "Prefetching %s chunk %s", file.Path,
					downloader.config.GetChunkIDFromHash(task.chunkHash))
				downloader.taskQueue <- *task
				task.isDownloading = true
				downloader.numberOfDownloadingChunks++
				downloader.numberOfActiveChunks++
			}
		} else {
			LOG_DEBUG("DOWNLOAD_PREFETCH", "%s chunk %s is not needed", file.Path,
				downloader.config.GetChunkIDFromHash(task.chunkHash))
		}
	}
}

// Reclaim releases the downloaded chunk to the chunk pool
func (downloader *ChunkDownloader) Reclaim(chunkIndex int) {

	if downloader.lastChunkIndex >= chunkIndex {
		return
	}

	for i := range downloader.completedTasks {
		if i < chunkIndex && downloader.taskList[i].chunk != nil {
			downloader.config.PutChunk(downloader.taskList[i].chunk)
			downloader.taskList[i].chunk = nil
			delete(downloader.completedTasks, i)
			downloader.numberOfActiveChunks--
		}
	}

	for i := downloader.lastChunkIndex; i < chunkIndex; i++ {
		// These chunks are never downloaded if 'isDownloading' is false; note that 'isDownloading' isn't reset to
		// false after a chunk has been downloaded
		if !downloader.taskList[i].isDownloading {
			atomic.AddInt64(&downloader.totalChunkSize, -int64(downloader.taskList[i].chunkLength))
		}
	}
	downloader.lastChunkIndex = chunkIndex
}

// Return the chunk last downloaded and its hash
func (downloader *ChunkDownloader) GetLastDownloadedChunk() (chunk *Chunk, chunkHash string) {
	if downloader.lastChunkIndex >= len(downloader.taskList) {
		return nil, ""
	}

	task := downloader.taskList[downloader.lastChunkIndex]
	return task.chunk, task.chunkHash
}

// WaitForChunk waits until the specified chunk is ready
func (downloader *ChunkDownloader) WaitForChunk(chunkIndex int) (chunk *Chunk) {

	// Reclaim any chunk not needed
	downloader.Reclaim(chunkIndex)

	// If we haven't started download the specified chunk, download it now
	if !downloader.taskList[chunkIndex].isDownloading {
		LOG_DEBUG("DOWNLOAD_FETCH", "Fetching chunk %s",
			downloader.config.GetChunkIDFromHash(downloader.taskList[chunkIndex].chunkHash))
		downloader.taskQueue <- downloader.taskList[chunkIndex]
		downloader.taskList[chunkIndex].isDownloading = true
		downloader.numberOfDownloadingChunks++
		downloader.numberOfActiveChunks++
	}

	// We also need to look ahead and prefetch other chunks as many as permitted by the number of threads
	for i := chunkIndex + 1; i < len(downloader.taskList); i++ {
		if downloader.numberOfActiveChunks >= downloader.threads {
			break
		}
		task := &downloader.taskList[i]
		if !task.needed {
			break
		}

		if !task.isDownloading {
			LOG_DEBUG("DOWNLOAD_PREFETCH", "Prefetching chunk %s", downloader.config.GetChunkIDFromHash(task.chunkHash))
			downloader.taskQueue <- *task
			task.isDownloading = true
			downloader.numberOfDownloadingChunks++
			downloader.numberOfActiveChunks++
		}
	}

	// Now wait until the chunk to be downloaded appears in the completed tasks
	for _, found := downloader.completedTasks[chunkIndex]; !found; _, found = downloader.completedTasks[chunkIndex] {
		completion := <-downloader.completionChannel
		downloader.completedTasks[completion.chunkIndex] = true
		downloader.taskList[completion.chunkIndex].chunk = completion.chunk
		downloader.numberOfDownloadedChunks++
		downloader.numberOfDownloadingChunks--
		if completion.chunk.isBroken {
			downloader.NumberOfFailedChunks++
		}
	}
	return downloader.taskList[chunkIndex].chunk
}

// WaitForCompletion waits until all chunks have been downloaded
func (downloader *ChunkDownloader) WaitForCompletion() {

	// Tasks in completedTasks have not been counted by numberOfActiveChunks
	downloader.numberOfActiveChunks -= len(downloader.completedTasks)

	// find the completed task with the largest index; we'll start from the next index
	for index := range downloader.completedTasks {
		if downloader.lastChunkIndex < index {
			downloader.lastChunkIndex = index
		}
	}

	// Looping until there isn't a download task in progress
	for downloader.numberOfActiveChunks > 0 || downloader.lastChunkIndex + 1 < len(downloader.taskList) {

		// Wait for a completion event first
		if downloader.numberOfActiveChunks > 0 {
			completion := <-downloader.completionChannel
			downloader.config.PutChunk(completion.chunk)
			downloader.numberOfActiveChunks--
			downloader.numberOfDownloadedChunks++
			downloader.numberOfDownloadingChunks--
			if completion.chunk.isBroken {
				downloader.NumberOfFailedChunks++
			}
		}

		// Pass the tasks one by one to the download queue
		if downloader.lastChunkIndex + 1 < len(downloader.taskList) {
			task := &downloader.taskList[downloader.lastChunkIndex + 1]
			if task.isDownloading {
				downloader.lastChunkIndex++
				continue
			}
			downloader.taskQueue <- *task
			task.isDownloading = true
			downloader.numberOfDownloadingChunks++
			downloader.numberOfActiveChunks++
			downloader.lastChunkIndex++
		}
	}
}

// Stop terminates all downloading goroutines
func (downloader *ChunkDownloader) Stop() {
	for downloader.numberOfDownloadingChunks > 0 {
		completion := <-downloader.completionChannel
		downloader.completedTasks[completion.chunkIndex] = true
		downloader.taskList[completion.chunkIndex].chunk = completion.chunk
		downloader.numberOfDownloadedChunks++
		downloader.numberOfDownloadingChunks--
		if completion.chunk.isBroken {
			downloader.NumberOfFailedChunks++
		}
}

	for i := range downloader.completedTasks {
		downloader.config.PutChunk(downloader.taskList[i].chunk)
		downloader.taskList[i].chunk = nil
		downloader.numberOfActiveChunks--
	}

	for i := 0; i < downloader.threads; i++ {
		downloader.stopChannel <- true
	}
}

// Download downloads a chunk from the storage.
func (downloader *ChunkDownloader) Download(threadIndex int, task ChunkDownloadTask) bool {

	cachedPath := ""
	chunk := downloader.config.GetChunk()
	chunkID := downloader.config.GetChunkIDFromHash(task.chunkHash)

	if downloader.snapshotCache != nil && downloader.storage.IsCacheNeeded() {

		var exist bool
		var err error

		// Reset the chunk with a hasher -- we're reading from the cache where chunk are not encrypted or compressed
		chunk.Reset(true)

		cachedPath, exist, _, err = downloader.snapshotCache.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			LOG_WARN("DOWNLOAD_CACHE", "Failed to find the cache path for the chunk %s: %v", chunkID, err)
		} else if exist {
			err = downloader.snapshotCache.DownloadFile(0, cachedPath, chunk)
			if err != nil {
				LOG_WARN("DOWNLOAD_CACHE", "Failed to load the chunk %s from the snapshot cache: %v", chunkID, err)
			} else {
				actualChunkID := chunk.GetID()
				if actualChunkID != chunkID {
					LOG_WARN("DOWNLOAD_CACHE_CORRUPTED",
						"The chunk %s load from the snapshot cache has a hash id of %s", chunkID, actualChunkID)
				} else {
					LOG_DEBUG("CHUNK_CACHE", "Chunk %s has been loaded from the snapshot cache", chunkID)

					downloader.completionChannel <- ChunkDownloadCompletion{chunk: chunk, chunkIndex: task.chunkIndex}
					return false
				}
			}
		}
	}

	// Reset the chunk without a hasher -- the downloaded content will be encrypted and/or compressed and the hasher
	// will be set up before the encryption
	chunk.Reset(false)

	// If failures are allowed, complete the task properly
	completeFailedChunk := func(chunk *Chunk) {
		if downloader.allowFailures {
			chunk.isBroken = true
			downloader.completionChannel <- ChunkDownloadCompletion{chunk: chunk, chunkIndex: task.chunkIndex}
		}
	}

	const MaxDownloadAttempts = 3
	for downloadAttempt := 0; ; downloadAttempt++ {

		// Find the chunk by ID first.
		chunkPath, exist, _, err := downloader.storage.FindChunk(threadIndex, chunkID, false)
		if err != nil {
			completeFailedChunk(chunk)
			LOG_WERROR(downloader.allowFailures, "DOWNLOAD_CHUNK", "Failed to find the chunk %s: %v", chunkID, err)
			return false
		}

		if !exist {
			// No chunk is found.  Have to find it in the fossil pool again.
			fossilPath, exist, _, err := downloader.storage.FindChunk(threadIndex, chunkID, true)
			if err != nil {
				completeFailedChunk(chunk)
				LOG_WERROR(downloader.allowFailures, "DOWNLOAD_CHUNK", "Failed to find the chunk %s: %v", chunkID, err)
				return false
			}

			if !exist {

				retry := false

				// Retry for Hubic or WebDAV as it may return 404 even when the chunk exists
				if _, ok := downloader.storage.(*HubicStorage); ok {
					retry = true
				}

				if _, ok := downloader.storage.(*WebDAVStorage); ok {
					retry = true
				}

				if retry && downloadAttempt < MaxDownloadAttempts {
					LOG_WARN("DOWNLOAD_RETRY", "Failed to find the chunk %s; retrying", chunkID)
					continue
				}

				completeFailedChunk(chunk)
				// A chunk is not found.  This is a serious error and hopefully it will never happen.
				if err != nil {
					LOG_WERROR(downloader.allowFailures,  "DOWNLOAD_CHUNK", "Chunk %s can't be found: %v", chunkID, err)
				} else {
					LOG_WERROR(downloader.allowFailures, "DOWNLOAD_CHUNK", "Chunk %s can't be found", chunkID)
				}
				return false
			}

			// Don't try to resurrect the fossil as we did before.  This is to avoid the potential read-after-rename
			// consistency issue.  Instead, download the fossil directly; resurrection should be taken care of later.
			chunkPath = fossilPath
			LOG_WARN("DOWNLOAD_FOSSIL", "Chunk %s is a fossil", chunkID)
		}

		err = downloader.storage.DownloadFile(threadIndex, chunkPath, chunk)
		if err != nil {
			_, isHubic := downloader.storage.(*HubicStorage)
			// Retry on EOF or if it is a Hubic backend as it may return 404 even when the chunk exists
			if (err == io.ErrUnexpectedEOF || isHubic) && downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "Failed to download the chunk %s: %v; retrying", chunkID, err)
				chunk.Reset(false)
				continue
			} else {
				completeFailedChunk(chunk)
				LOG_WERROR(downloader.allowFailures, "DOWNLOAD_CHUNK", "Failed to download the chunk %s: %v", chunkID, err)
				return false
			}
		}

		err = chunk.Decrypt(downloader.config.ChunkKey, task.chunkHash)
		if err != nil {
			if downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "Failed to decrypt the chunk %s: %v; retrying", chunkID, err)
				chunk.Reset(false)
				continue
			} else {
				completeFailedChunk(chunk)
				LOG_WERROR(downloader.allowFailures, "DOWNLOAD_DECRYPT", "Failed to decrypt the chunk %s: %v", chunkID, err)
				return false
			}
		}

		actualChunkID := chunk.GetID()
		if actualChunkID != chunkID {
			if downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "The chunk %s has a hash id of %s; retrying", chunkID, actualChunkID)
				chunk.Reset(false)
				continue
			} else {
				completeFailedChunk(chunk)
				LOG_WERROR(downloader.allowFailures, "DOWNLOAD_CORRUPTED", "The chunk %s has a hash id of %s", chunkID, actualChunkID)
				return false
			}
		}

		break
	}

	if len(cachedPath) > 0 {
		// Save a copy to the local snapshot cache
		err := downloader.snapshotCache.UploadFile(threadIndex, cachedPath, chunk.GetBytes())
		if err != nil {
			LOG_WARN("DOWNLOAD_CACHE", "Failed to add the chunk %s to the snapshot cache: %v", chunkID, err)
		}
	}

	downloadedChunkSize := atomic.AddInt64(&downloader.downloadedChunkSize, int64(chunk.GetLength()))

	if (downloader.showStatistics || IsTracing()) && downloader.totalChunkSize > 0 {

		now := time.Now().Unix()
		if now <= downloader.startTime {
			now = downloader.startTime + 1
		}
		speed := downloadedChunkSize / (now - downloader.startTime)
		remainingTime := int64(0)
		if speed > 0 {
			remainingTime = (downloader.totalChunkSize-downloadedChunkSize)/speed + 1
		}
		percentage := float32(downloadedChunkSize * 1000 / downloader.totalChunkSize)
		LOG_INFO("DOWNLOAD_PROGRESS", "Downloaded chunk %d size %d, %sB/s %s %.1f%%",
			task.chunkIndex+1, chunk.GetLength(),
			PrettySize(speed), PrettyTime(remainingTime), percentage/10)
	} else {
		LOG_DEBUG("CHUNK_DOWNLOAD", "Chunk %s has been downloaded", chunkID)
	}

	downloader.completionChannel <- ChunkDownloadCompletion{chunk: chunk, chunkIndex: task.chunkIndex}
	return true
}
