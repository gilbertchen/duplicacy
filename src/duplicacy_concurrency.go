// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"sync"
	"sync/atomic"
)

// runConcurrently calls job(threadIndex, index) for each index in [0, count) on at most 'threads' goroutines, and
// re-raises in the calling goroutine whatever the first worker panicked with.  Each worker claims the next index until
// the range is exhausted, so a slow index doesn't hold up the others, and the thread index is handed to the job because
// some storages index a per-thread client or nested directory with it.
//
// A worker that hits an error can't report it by itself, since the panic raised by LOG_ERROR would unwind its own
// goroutine only.  The first one is captured here and re-raised after every worker has finished, so the caller and the
// top-level exception handler see exactly what they see in the single-threaded case.
//
// The work runs on the calling goroutine when at most one worker is needed, so a serial call is the plain loop it was
// before and the job may touch state the caller owns without a synchronization barrier.
func runConcurrently(threads, count int, job func(threadIndex, index int)) {

	if threads > count {
		threads = count
	}

	if threads <= 1 {
		for index := 0; index < count; index++ {
			job(0, index)
		}
		return
	}

	var waitGroup sync.WaitGroup

	var failure interface{}
	var failureLock sync.Mutex

	nextIndex := int64(0)

	waitGroup.Add(threads)
	for i := 0; i < threads; i++ {
		go func(threadIndex int) {
			defer waitGroup.Done()
			defer func() {
				if r := recover(); r != nil {
					failureLock.Lock()
					if failure == nil {
						failure = r
					}
					failureLock.Unlock()
				}
			}()

			for {
				index := int(atomic.AddInt64(&nextIndex, 1)) - 1
				if index >= count {
					return
				}
				job(threadIndex, index)
			}
		}(i)
	}

	waitGroup.Wait()

	if failure != nil {
		panic(failure)
	}
}
