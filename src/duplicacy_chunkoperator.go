// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"sync"
	"sync/atomic"
	"time"
)

// These are operations that ChunkOperator will perform.
const (
	ChunkOperationFind      = 0
	ChunkOperationDelete    = 1
	ChunkOperationFossilize = 2
	ChunkOperationResurrect = 3
)

// ChunkOperatorTask is used to pass paramaters for different kinds of chunk operations.
type ChunkOperatorTask struct {
	operation int    // The type of operation
	chunkID   string // The chunk id
	filePath  string // The path of the chunk file; it may be empty
}

// ChunkOperator is capable of performing multi-threaded operations on chunks.
type ChunkOperator struct {
	storage             Storage                // This storage
	threads             int                    // Number of threads
	taskQueue           chan ChunkOperatorTask // Operating goroutines are waiting on this channel for input
	stopChannel         chan bool              // Used to stop all the goroutines
	numberOfActiveTasks int64                  // The number of chunks that are being operated on

	fossils     []string    // For fossilize operation, the paths of the fossils are stored in this slice
	fossilsLock *sync.Mutex // The lock for 'fossils'
}

// CreateChunkOperator creates a new ChunkOperator.
func CreateChunkOperator(storage Storage, threads int) *ChunkOperator {
	operator := &ChunkOperator{
		storage: storage,
		threads: threads,

		taskQueue:   make(chan ChunkOperatorTask, threads*4),
		stopChannel: make(chan bool),

		fossils:     make([]string, 0),
		fossilsLock: &sync.Mutex{},
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
	for atomic.LoadInt64(&operator.numberOfActiveTasks) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < operator.threads; i++ {
		operator.stopChannel <- false
	}
}

func (operator *ChunkOperator) AddTask(operation int, chunkID string, filePath string) {

	task := ChunkOperatorTask{
		operation: operation,
		chunkID:   chunkID,
		filePath:  filePath,
	}
	operator.taskQueue <- task
	atomic.AddInt64(&operator.numberOfActiveTasks, int64(1))
}

func (operator *ChunkOperator) Find(chunkID string) {
	operator.AddTask(ChunkOperationFind, chunkID, "")
}

func (operator *ChunkOperator) Delete(chunkID string, filePath string) {
	operator.AddTask(ChunkOperationDelete, chunkID, filePath)
}

func (operator *ChunkOperator) Fossilize(chunkID string, filePath string) {
	operator.AddTask(ChunkOperationFossilize, chunkID, filePath)
}

func (operator *ChunkOperator) Resurrect(chunkID string, filePath string) {
	operator.AddTask(ChunkOperationResurrect, chunkID, filePath)
}

func (operator *ChunkOperator) Run(threadIndex int, task ChunkOperatorTask) {
	defer func() {
		atomic.AddInt64(&operator.numberOfActiveTasks, int64(-1))
	}()

	// task.filePath may be empty.  If so, find the chunk first.
	if task.operation == ChunkOperationDelete || task.operation == ChunkOperationFossilize {
		if task.filePath == "" {
			filePath, exist, _, err := operator.storage.FindChunk(threadIndex, task.chunkID, false)
			if err != nil {
				LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", task.chunkID, err)
				return
			} else if !exist {
				LOG_ERROR("CHUNK_FIND", "Chunk %s does not exist in the storage", task.chunkID)
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
		// In exclusive mode, we assume no other restore operation is running concurrently.
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
			} else {
				LOG_ERROR("CHUNK_DELETE", "Failed to fossilize the chunk %s: %v", task.chunkID, err)
			}
		} else {
			LOG_TRACE("CHUNK_FOSSILIZE", "Fossilized chunk %s", task.chunkID)
			operator.fossilsLock.Lock()
			operator.fossils = append(operator.fossils, fossilPath)
			operator.fossilsLock.Unlock()
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
