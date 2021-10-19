// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
)

// MaskArray is a mask list which u can choose satisfied mask for different avg chunk size

var MaskArray = [...]uint64{
	0, 0, 0, 0, 0, 0,
	0x00001803110,      // 64B
	0x000018035100,     // 128B
	0x00001800035300,   // 256B
	0x000019000353000,  // 512B
	0x0000590003530000, // 1KB
	0x0000d90003530000, // 2KB
	0x0000d90103530000, // 4KB
	0x0000d90303530000, // 8KB
	0x0000d90313530000, // 16KB
	0x0000d90f03530000, // 32KB
	0x0000d90303537000, // 64KB
	0x0000d90703537000, // 128KB
	0x0000d90707537000, // 256KB
	0x0000d91707537000, // 512KB
	0x0000d91747537000, // 1MB
	0x0000d91767537000, // 2MB
	0x0000d93767537000, // 4MB
	0x0000d93777537000, // 8MB
	0x0000d93777577000, // 16MB
}

// ChunkMaker breaks data into chunks using fastcdc.  To save memory, the chunk maker only use a circular buffer
// whose size is double the minimum chunk size.
type ChunkMaker struct {
	maximumChunkSize int
	minimumChunkSize int
	bufferCapacity   int

	hashMask    uint64
	hashMask2   uint64
	randomTable [256]uint64

	buffer      []byte
	bufferSize  int
	bufferStart int

	config *Config

	hashOnly      bool
	hashOnlyChunk *Chunk
}

// CreateChunkMaker creates a chunk maker.  'randomSeed' is used to generate the character-to-integer table needed by
// fastcdc.
func CreateChunkMaker(config *Config, hashOnly bool) *ChunkMaker {
	size := 1
	count := 0
	for size*2 <= config.AverageChunkSize {
		size *= 2
		count += 1
	}

	if size != config.AverageChunkSize {
		LOG_FATAL("CHUNK_SIZE", "Invalid average chunk size: %d is not a power of 2", config.AverageChunkSize)
		return nil
	}

	maker := &ChunkMaker{
		hashMask:         MaskArray[count+2],
		hashMask2:        MaskArray[count-2],
		maximumChunkSize: config.MaximumChunkSize,
		minimumChunkSize: config.MinimumChunkSize,
		bufferCapacity:   2 * config.MinimumChunkSize,
		config:           config,
		hashOnly:         hashOnly,
	}

	if hashOnly {
		maker.hashOnlyChunk = CreateChunk(config, false)
	}

	randomData := sha256.Sum256(config.ChunkSeed)

	for i := 0; i < 64; i++ {
		for j := 0; j < 4; j++ {
			maker.randomTable[4*i+j] = binary.LittleEndian.Uint64(randomData[8*j : 8*j+8])
		}
		randomData = sha256.Sum256(randomData[:])
	}

	maker.buffer = make([]byte, 2*config.MinimumChunkSize)

	return maker
}

// ForEachChunk reads data from 'reader'.  If EOF is encountered, it will call 'nextReader' to ask for next file.  If
// 'nextReader' returns false, it will process remaining data in the buffer and then quit.  When a chunk is identified,
// it will call 'endOfChunk' to return the chunk size and a boolean flag indicating if it is the last chunk.
func (maker *ChunkMaker) ForEachChunk(reader io.Reader, endOfChunk func(chunk *Chunk, final bool),
	nextReader func(size int64, hash string) (io.Reader, bool)) {

	maker.bufferStart = 0
	maker.bufferSize = 0

	//var minimumReached bool
	var hashSum uint64
	var chunk *Chunk

	fileSize := int64(0)
	fileHasher := maker.config.NewFileHasher()

	// Start a new chunk.
	startNewChunk := func() {
		hashSum = 0
		//minimumReached = false
		if maker.hashOnly {
			chunk = maker.hashOnlyChunk
			chunk.Reset(true)
		} else {
			chunk = maker.config.GetChunk()
			chunk.Reset(true)
		}
	}

	// Move data from the buffer to the chunk.
	fill := func(count int) {
		if maker.bufferStart+count < maker.bufferCapacity {
			chunk.Write(maker.buffer[maker.bufferStart : maker.bufferStart+count])
			maker.bufferStart += count
			maker.bufferSize -= count
		} else {
			chunk.Write(maker.buffer[maker.bufferStart:])
			chunk.Write(maker.buffer[:count-(maker.bufferCapacity-maker.bufferStart)])
			maker.bufferStart = count - (maker.bufferCapacity - maker.bufferStart)
			maker.bufferSize -= count
		}
	}

	startNewChunk()

	var err error

	isEOF := false

	if maker.minimumChunkSize == maker.maximumChunkSize {

		if maker.bufferCapacity < maker.minimumChunkSize {
			maker.buffer = make([]byte, maker.minimumChunkSize)
		}

		for {
			maker.bufferStart = 0
			for maker.bufferStart < maker.minimumChunkSize && !isEOF {
				count, err := reader.Read(maker.buffer[maker.bufferStart:maker.minimumChunkSize])

				if err != nil {
					if err != io.EOF {
						LOG_ERROR("CHUNK_MAKER", "Failed to read %d bytes: %s", count, err.Error())
						return
					} else {
						isEOF = true
					}
				}
				maker.bufferStart += count
			}

			fileHasher.Write(maker.buffer[:maker.bufferStart])
			fileSize += int64(maker.bufferStart)
			chunk.Write(maker.buffer[:maker.bufferStart])

			if isEOF {
				var ok bool
				reader, ok = nextReader(fileSize, hex.EncodeToString(fileHasher.Sum(nil)))
				if !ok {
					endOfChunk(chunk, true)
					return
				} else {
					endOfChunk(chunk, false)
					startNewChunk()
					fileSize = 0
					fileHasher = maker.config.NewFileHasher()
					isEOF = false
				}
			} else {
				endOfChunk(chunk, false)
				startNewChunk()
			}
		}

	}

	for {

		// If the buffer still has some space left and EOF is not seen, read more data.
		for maker.bufferSize < maker.bufferCapacity && !isEOF {
			start := maker.bufferStart + maker.bufferSize
			count := maker.bufferCapacity - start
			if start >= maker.bufferCapacity {
				start -= maker.bufferCapacity
				count = maker.bufferStart - start
			}

			count, err = reader.Read(maker.buffer[start : start+count])

			if err != nil && err != io.EOF {
				LOG_ERROR("CHUNK_MAKER", "Failed to read %d bytes: %s", count, err.Error())
				return
			}

			maker.bufferSize += count
			fileHasher.Write(maker.buffer[start : start+count])
			fileSize += int64(count)

			// if EOF is seen, try to switch to next file and continue
			if err == io.EOF {
				var ok bool
				reader, ok = nextReader(fileSize, hex.EncodeToString(fileHasher.Sum(nil)))
				if !ok {
					isEOF = true
				} else {
					fileSize = 0
					fileHasher = maker.config.NewFileHasher()
					isEOF = false
				}
			}
		}

		// No eough data to meet the minimum chunk size requirement, so just return as a chunk.
		if maker.bufferSize < maker.minimumChunkSize {
			fill(maker.bufferSize)
			endOfChunk(chunk, true)
			return
		}

		begin := maker.minimumChunkSize
		mid := maker.minimumChunkSize + maker.config.AverageChunkSize
		end := maker.bufferSize
		isEOC := false
		bytes := maker.minimumChunkSize

		if end > maker.maximumChunkSize {
			end = maker.maximumChunkSize
		} else if end < mid {
			mid = end
		}

		for begin < mid {
			hashSum = (hashSum << 1) + maker.randomTable[maker.buffer[begin]]
			if hashSum&maker.hashMask == 0 {
				bytes = begin
				isEOC = true
				break
			}
			begin++
		}

		for !isEOC && begin < end {
			hashSum = (hashSum << 1) + maker.randomTable[maker.buffer[begin]]
			if hashSum&maker.hashMask2 == 0 {
				bytes = begin
				isEOC = true
				break
			}
			begin++
		}

		/**
		// Minimum chunk size has been reached.  Calculate the buzhash for the minimum size chunk.
		if !minimumReached {

			bytes := maker.minimumChunkSize

			if maker.bufferStart+bytes < maker.bufferCapacity {
				hashSum = maker.buzhashSum(0, maker.buffer[maker.bufferStart:maker.bufferStart+bytes])
			} else {
				hashSum = maker.buzhashSum(0, maker.buffer[maker.bufferStart:])
				hashSum = maker.buzhashSum(hashSum,
					maker.buffer[:bytes-(maker.bufferCapacity-maker.bufferStart)])
			}

			if (hashSum & maker.hashMask) == 0 {
				// This is a minimum size chunk
				fill(bytes)
				endOfChunk(chunk, false)
				startNewChunk()
				continue
			}

			minimumReached = true
		}

		// Now check the buzhash of the data in the buffer, shifting one byte at a time.
		bytes := maker.bufferSize - maker.minimumChunkSize
		isEOC := false
		maxSize := maker.maximumChunkSize - chunk.GetLength()
		for i := 0; i < maker.bufferSize-maker.minimumChunkSize; i++ {
			out := maker.bufferStart + i
			if out >= maker.bufferCapacity {
				out -= maker.bufferCapacity
			}
			in := maker.bufferStart + i + maker.minimumChunkSize
			if in >= maker.bufferCapacity {
				in -= maker.bufferCapacity
			}

			hashSum = maker.buzhashUpdate(hashSum, maker.buffer[out], maker.buffer[in], maker.minimumChunkSize)
			if (hashSum&maker.hashMask) == 0 || i == maxSize-maker.minimumChunkSize-1 {
				// A chunk is completed.
				bytes = i + 1 + maker.minimumChunkSize
				isEOC = true
				break
			}
		}

		*/

		fill(bytes)

		if isEOC {
			if isEOF && maker.bufferSize == 0 {
				endOfChunk(chunk, true)
				return
			}
			endOfChunk(chunk, false)
			startNewChunk()
			continue
		}

		if isEOF {
			fill(maker.bufferSize)
			endOfChunk(chunk, true)
			return
		}
	}
}
