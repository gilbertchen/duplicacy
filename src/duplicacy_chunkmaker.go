// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"runtime"
	"strings"
)

// ChunkMaker breaks data into chunks using buzhash.  To save memory, the chunk maker only use a circular buffer
// whose size is double the minimum chunk size.
type ChunkMaker struct {
	maximumChunkSize int
	minimumChunkSize int
	bufferCapacity   int

	hashMask    uint64
	randomTable [256]uint64

	buffer      []byte
	bufferSize  int
	bufferStart int

	minimumReached bool
	hashSum uint64
	chunk *Chunk

	config *Config

	hashOnly      bool
	hashOnlyChunk *Chunk

}

// CreateChunkMaker creates a chunk maker.  'randomSeed' is used to generate the character-to-integer table needed by
// buzhash.
func CreateFileChunkMaker(config *Config, hashOnly bool) *ChunkMaker {
	size := 1
	for size*2 <= config.AverageChunkSize {
		size *= 2
	}

	if size != config.AverageChunkSize {
		LOG_FATAL("CHUNK_SIZE", "Invalid average chunk size: %d is not a power of 2", config.AverageChunkSize)
		return nil
	}

	maker := &ChunkMaker{
		hashMask:         uint64(config.AverageChunkSize - 1),
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
	maker.bufferStart = 0
	maker.bufferSize = 0

	maker.startNewChunk()

	return maker
}

// CreateMetaDataChunkMaker creates a chunk maker that always uses the variable-sized chunking algorithm
func CreateMetaDataChunkMaker(config *Config, chunkSize int) *ChunkMaker {

	size := 1
	for size*2 <= chunkSize {
		size *= 2
	}

	if size != chunkSize {
		LOG_FATAL("CHUNK_SIZE", "Invalid metadata chunk size: %d is not a power of 2", chunkSize)
		return nil
	}

	maker := CreateFileChunkMaker(config, false)
	maker.hashMask = uint64(chunkSize - 1)
	maker.maximumChunkSize = chunkSize * 4
	maker.minimumChunkSize = chunkSize / 4
	maker.bufferCapacity = 2 * maker.minimumChunkSize
	maker.buffer = make([]byte, maker.bufferCapacity)

	return maker
}

func rotateLeft(value uint64, bits uint) uint64 {
	return (value << (bits & 0x3f)) | (value >> (64 - (bits & 0x3f)))
}

func rotateLeftByOne(value uint64) uint64 {
	return (value << 1) | (value >> 63)
}

func (maker *ChunkMaker) buzhashSum(sum uint64, data []byte) uint64 {
	for i := 0; i < len(data); i++ {
		sum = rotateLeftByOne(sum) ^ maker.randomTable[data[i]]
	}
	return sum
}

func (maker *ChunkMaker) buzhashUpdate(sum uint64, out byte, in byte, length int) uint64 {
	return rotateLeftByOne(sum) ^ rotateLeft(maker.randomTable[out], uint(length)) ^ maker.randomTable[in]
}

func (maker *ChunkMaker) startNewChunk() (chunk *Chunk) {
	maker.hashSum = 0
	maker.minimumReached = false
	if maker.hashOnly {
		maker.chunk = maker.hashOnlyChunk
		maker.chunk.Reset(true)
	} else {
		maker.chunk = maker.config.GetChunk()
		maker.chunk.Reset(true)
	}
	return
}

func (maker *ChunkMaker) AddData(reader io.Reader, sendChunk func(*Chunk)) (int64, string, string) {

	isEOF := false
	fileSize := int64(0)
	fileHasher := maker.config.NewFileHasher()

	// Move data from the buffer to the chunk.
	fill := func(count int) {

		if maker.bufferStart+count < maker.bufferCapacity {
			maker.chunk.Write(maker.buffer[maker.bufferStart : maker.bufferStart+count])
			maker.bufferStart += count
			maker.bufferSize -= count
		} else {
			maker.chunk.Write(maker.buffer[maker.bufferStart:])
			maker.chunk.Write(maker.buffer[:count-(maker.bufferCapacity-maker.bufferStart)])
			maker.bufferStart = count - (maker.bufferCapacity - maker.bufferStart)
			maker.bufferSize -= count
		}
	}

	var err error

	if maker.minimumChunkSize == maker.maximumChunkSize {

		if reader == nil {
			return 0, "", ""
		}

		for {
			maker.startNewChunk()
			maker.bufferStart = 0
			for maker.bufferStart < maker.minimumChunkSize && !isEOF {
				count, err := reader.Read(maker.buffer[maker.bufferStart:maker.minimumChunkSize])

				if err != nil {
					if err != io.EOF {
						// handle OneDrive 'cloud files' errors (sometimes these are caught by os.OpenFile, sometimes
						// not)
						isWarning := runtime.GOOS == "windows" && strings.HasSuffix(err.Error(), " Access to the cloud file is denied.")
						LOG_WERROR(isWarning, "CHUNK_MAKER", "Failed to read %d bytes: %s", count, err.Error())
						return -1, "", "CLOUD_FILE" // we'd only hit this if it was a cloud file warning, LOG_ERROR panic exits
					} else {
						isEOF = true
					}
				}
				maker.bufferStart += count
			}

			if maker.bufferStart > 0 {
				fileHasher.Write(maker.buffer[:maker.bufferStart])
				fileSize += int64(maker.bufferStart)
				maker.chunk.Write(maker.buffer[:maker.bufferStart])
				sendChunk(maker.chunk)
			}

			if isEOF {
				return fileSize, hex.EncodeToString(fileHasher.Sum(nil)), ""
			}
		}

	}

	for {

		// If the buffer still has some space left and EOF is not seen, read more data.
		for maker.bufferSize < maker.bufferCapacity && !isEOF && reader != nil {
			start := maker.bufferStart + maker.bufferSize
			count := maker.bufferCapacity - start
			if start >= maker.bufferCapacity {
				start -= maker.bufferCapacity
				count = maker.bufferStart - start
			}

			count, err = reader.Read(maker.buffer[start : start+count])

			if err != nil && err != io.EOF {
				// handle OneDrive 'cloud files' errors (sometimes these are caught by os.OpenFile, sometimes not)
				isWarning := runtime.GOOS == "windows" && strings.HasSuffix(err.Error(), " Access to the cloud file is denied.")
				LOG_WERROR(isWarning, "CHUNK_MAKER", "Failed to read %d bytes: %s", count, err.Error())
				return -1, "", "CLOUD_FILE_FAILURE" // we'd only hit this if it was a cloud file warning, LOG_ERROR panic exits
			}

			maker.bufferSize += count
			fileHasher.Write(maker.buffer[start : start+count])
			fileSize += int64(count)

			// if EOF is seen, try to switch to next file and continue
			if err == io.EOF {
				isEOF = true
				break
			}
		}

		// No eough data to meet the minimum chunk size requirement, so just return as a chunk.
		if maker.bufferSize < maker.minimumChunkSize {
			if reader == nil {
				fill(maker.bufferSize)
				if maker.chunk.GetLength() > 0 {
					sendChunk(maker.chunk)
				}
				return 0, "", ""
			} else if isEOF {
				return fileSize, hex.EncodeToString(fileHasher.Sum(nil)), ""
			} else {
				continue
			}
		}

		// Minimum chunk size has been reached.  Calculate the buzhash for the minimum size chunk.
		if !maker.minimumReached {

			bytes := maker.minimumChunkSize

			if maker.bufferStart+bytes < maker.bufferCapacity {
				maker.hashSum = maker.buzhashSum(0, maker.buffer[maker.bufferStart:maker.bufferStart+bytes])
			} else {
				maker.hashSum = maker.buzhashSum(0, maker.buffer[maker.bufferStart:])
				maker.hashSum = maker.buzhashSum(maker.hashSum,
					maker.buffer[:bytes-(maker.bufferCapacity-maker.bufferStart)])
			}

			if (maker.hashSum & maker.hashMask) == 0 {
				// This is a minimum size chunk
				fill(bytes)
				sendChunk(maker.chunk)
				maker.startNewChunk()
				continue
			}

			maker.minimumReached = true
		}

		// Now check the buzhash of the data in the buffer, shifting one byte at a time.
		bytes := maker.bufferSize - maker.minimumChunkSize
		isEOC := false // chunk boundary found
		maxSize := maker.maximumChunkSize - maker.chunk.GetLength()
		for i := 0; i < bytes; i++ {
			out := maker.bufferStart + i
			if out >= maker.bufferCapacity {
				out -= maker.bufferCapacity
			}
			in := maker.bufferStart + i + maker.minimumChunkSize
			if in >= maker.bufferCapacity {
				in -= maker.bufferCapacity
			}

			maker.hashSum = maker.buzhashUpdate(maker.hashSum, maker.buffer[out], maker.buffer[in], maker.minimumChunkSize)
			if (maker.hashSum&maker.hashMask) == 0 || i == maxSize-maker.minimumChunkSize-1 {
				// A chunk is completed.
				bytes = i + 1 + maker.minimumChunkSize
				isEOC = true
				break
			}
		}

		fill(bytes)
		if isEOC {
			sendChunk(maker.chunk)
			maker.startNewChunk()
		} else {
			if reader == nil {
				fill(maker.minimumChunkSize)
				sendChunk(maker.chunk)
				maker.startNewChunk()
				return 0, "", ""
			}
		}

		if isEOF {
			return fileSize, hex.EncodeToString(fileHasher.Sum(nil)), ""
		}
	}
}
