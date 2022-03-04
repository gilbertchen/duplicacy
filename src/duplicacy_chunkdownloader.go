// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
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

type ChunkDownloadCompletion struct {
	chunk      *Chunk
	chunkIndex int
}

// ChunkDownloader is a wrapper of ChunkOperator and is only used by the restore procedure.capable of performing multi-threaded downloading.  Chunks to be downloaded are first organized
// as a list of ChunkDownloadTasks, with only the chunkHash field initialized.  When a chunk is needed, the
// corresponding ChunkDownloadTask is sent to the dowloading goroutine.  Once a chunk is downloaded, it will be
// inserted in the completed task list.
type ChunkDownloader struct {

	operator                  *ChunkOperator

	totalChunkSize            int64 // Total chunk size
	downloadedChunkSize       int64 // Downloaded chunk size

	taskList       []ChunkDownloadTask // The list of chunks to be downloaded
	completedTasks map[int]bool        // Store downloaded chunks
	lastChunkIndex int                 // a monotonically increasing number indicating the last chunk to be downloaded

	completionChannel chan ChunkDownloadCompletion // A downloading goroutine sends back the chunk via this channel after downloading

	startTime                 int64 // The time it starts downloading
	numberOfDownloadedChunks  int   // The number of chunks that have been downloaded
	numberOfDownloadingChunks int   // The number of chunks still being downloaded
	numberOfActiveChunks      int   // The number of chunks that is being downloaded or has been downloaded but not reclaimed
}

func CreateChunkDownloader(operator *ChunkOperator) *ChunkDownloader {
	downloader := &ChunkDownloader{
		operator:       operator,

		taskList:       nil,
		completedTasks: make(map[int]bool),
		lastChunkIndex: 0,

		completionChannel: make(chan ChunkDownloadCompletion),

		startTime: time.Now().Unix(),
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
	downloader.operator.totalChunkSize = downloader.totalChunkSize
}

// Prefetch adds up to 'threads' chunks needed by a file to the download list
func (downloader *ChunkDownloader) Prefetch(file *Entry) {

	// Any chunks before the first chunk of this filea are not needed any more, so they can be reclaimed.
	downloader.Reclaim(file.StartChunk)

	for i := file.StartChunk; i <= file.EndChunk; i++ {
		task := &downloader.taskList[i]
		if task.needed {
			if !task.isDownloading {
				if downloader.numberOfActiveChunks >= downloader.operator.threads {
					return
				}

				LOG_DEBUG("DOWNLOAD_PREFETCH", "Prefetching %s chunk %s", file.Path,
					downloader.operator.config.GetChunkIDFromHash(task.chunkHash))
				downloader.operator.DownloadAsync(task.chunkHash, i, false, func (chunk *Chunk, chunkIndex int) {
					downloader.completionChannel <- ChunkDownloadCompletion { chunk: chunk, chunkIndex: chunkIndex }
				})
				task.isDownloading = true
				downloader.numberOfDownloadingChunks++
				downloader.numberOfActiveChunks++
			}
		} else {
			LOG_DEBUG("DOWNLOAD_PREFETCH", "%s chunk %s is not needed", file.Path,
				downloader.operator.config.GetChunkIDFromHash(task.chunkHash))
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
			downloader.operator.config.PutChunk(downloader.taskList[i].chunk)
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
			downloader.operator.config.GetChunkIDFromHash(downloader.taskList[chunkIndex].chunkHash))
		downloader.operator.DownloadAsync(downloader.taskList[chunkIndex].chunkHash, chunkIndex, false, func (chunk *Chunk, chunkIndex int) {
			downloader.completionChannel <- ChunkDownloadCompletion { chunk: chunk, chunkIndex: chunkIndex }
		})
		downloader.taskList[chunkIndex].isDownloading = true
		downloader.numberOfDownloadingChunks++
		downloader.numberOfActiveChunks++
	}

	// We also need to look ahead and prefetch other chunks as many as permitted by the number of threads
	for i := chunkIndex + 1; i < len(downloader.taskList); i++ {
		if downloader.numberOfActiveChunks >= downloader.operator.threads {
			break
		}
		task := &downloader.taskList[i]
		if !task.needed {
			break
		}

		if !task.isDownloading {
			LOG_DEBUG("DOWNLOAD_PREFETCH", "Prefetching chunk %s", downloader.operator.config.GetChunkIDFromHash(task.chunkHash))
			downloader.operator.DownloadAsync(task.chunkHash, task.chunkIndex, false, func (chunk *Chunk, chunkIndex int) {
				downloader.completionChannel <- ChunkDownloadCompletion { chunk: chunk, chunkIndex: chunkIndex }
			})
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
			downloader.operator.config.PutChunk(completion.chunk)
			downloader.numberOfActiveChunks--
			downloader.numberOfDownloadedChunks++
			downloader.numberOfDownloadingChunks--
		}

		// Pass the tasks one by one to the download queue
		if downloader.lastChunkIndex + 1 < len(downloader.taskList) {
			task := &downloader.taskList[downloader.lastChunkIndex + 1]
			if task.isDownloading {
				downloader.lastChunkIndex++
				continue
			}
			downloader.operator.DownloadAsync(task.chunkHash, task.chunkIndex, false, func (chunk *Chunk, chunkIndex int) {
				downloader.completionChannel <- ChunkDownloadCompletion { chunk: chunk, chunkIndex: chunkIndex }
			})
			task.isDownloading = true
			downloader.numberOfDownloadingChunks++
			downloader.numberOfActiveChunks++
			downloader.lastChunkIndex++
		}
	}
}

