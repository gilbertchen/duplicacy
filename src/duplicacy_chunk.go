// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/rsa"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"runtime"

	"github.com/bkaradzic/go-lz4"
	"github.com/minio/highwayhash"
	"github.com/klauspost/reedsolomon"
	"github.com/klauspost/compress/zstd"

	// This is a fork of github.com/minio/highwayhash at 1.0.1 that computes incorrect hash on
	// arm64 machines.  We need this fork to be able to read the chunks created by Duplicacy
	// CLI 3.0.1 which unfortunately relies on incorrect hashes to determine if each shard is valid.
	wronghighwayhash "github.com/gilbertchen/highwayhash"

)

// A chunk needs to acquire a new buffer and return the old one for every encrypt/decrypt operation, therefore
// we maintain a pool of previously used buffers.
var chunkBufferPool chan *bytes.Buffer = make(chan *bytes.Buffer, runtime.NumCPU()*16)

func AllocateChunkBuffer() (buffer *bytes.Buffer) {
	select {
	case buffer = <-chunkBufferPool:
	default:
		buffer = new(bytes.Buffer)
	}
	return buffer
}

func ReleaseChunkBuffer(buffer *bytes.Buffer) {
	select {
	case chunkBufferPool <- buffer:
	default:
		LOG_INFO("CHUNK_BUFFER", "Discarding a free chunk buffer due to a full pool")
	}
}

// Chunk is the object being passed between the chunk maker, the chunk uploader, and chunk downloader.  It can be
// read and written like a bytes.Buffer, and provides convenient functions to calculate the hash and id of the chunk.
type Chunk struct {
	buffer *bytes.Buffer // Where the actual data is stored.  It may be nil for hash-only chunks, where chunks
	// are only used to compute the hashes

	size int // The size of data stored.  This field is needed if buffer is nil

	hasher hash.Hash // Keeps track of the hash of data stored in the buffer.  It may be nil, since sometimes
	// it isn't necessary to compute the hash, for instance, when the encrypted data is being
	// read into the primary buffer

	hash []byte // The hash of the chunk data.  It is always in the binary format
	id   string // The id of the chunk data (used as the file name for saving the chunk); always in hex format

	config *Config // Every chunk is associated with a Config object.  Which hashing algorithm to use is determined
	// by the config

	isMetadata bool // Indicates if the chunk is a metadata chunk (instead of a file chunk).  This is primarily used by RSA
	                // encryption, where a metadata chunk is not encrypted by RSA
	
	isBroken bool // Indicates the chunk did not download correctly. This is only used for -persist (allowFailures) mode
}

// Magic word to identify a duplicacy format encrypted file, plus a version number.
var ENCRYPTION_BANNER = "duplicacy\000"

// RSA encrypted chunks start with "duplicacy\002"
var ENCRYPTION_VERSION_RSA byte = 2

var ERASURE_CODING_BANNER = "duplicacy\003"

// CreateChunk creates a new chunk.
func CreateChunk(config *Config, bufferNeeded bool) *Chunk {

	var buffer *bytes.Buffer

	if bufferNeeded {
		buffer = AllocateChunkBuffer()
		buffer.Reset()
		if buffer.Cap() < config.MaximumChunkSize {
			buffer.Grow(config.MaximumChunkSize - buffer.Cap())
		}
	}

	return &Chunk{
		buffer: buffer,
		config: config,
	}
}

// GetLength returns the length of available data
func (chunk *Chunk) GetLength() int {
	if chunk.buffer != nil {
		return len(chunk.buffer.Bytes())
	} else {
		return chunk.size
	}
}

// GetBytes returns data available in this chunk
func (chunk *Chunk) GetBytes() []byte {
	return chunk.buffer.Bytes()
}

// Reset makes the chunk reusable by clearing the existing data in the buffers.  'hashNeeded' indicates whether the
// hash of the new data to be read is needed.  If the data to be read in is encrypted, there is no need to
// calculate the hash so hashNeeded should be 'false'.
func (chunk *Chunk) Reset(hashNeeded bool) {
	if chunk.buffer != nil {
		chunk.buffer.Reset()
	}
	if hashNeeded {
		chunk.hasher = chunk.config.NewKeyedHasher(chunk.config.HashKey)
	} else {
		chunk.hasher = nil
	}
	chunk.hash = nil
	chunk.id = ""
	chunk.size = 0
	chunk.isMetadata = false
	chunk.isBroken = false
}

// Write implements the Writer interface.
func (chunk *Chunk) Write(p []byte) (int, error) {

	// buffer may be nil, when the chunk is used for computing the hash only.
	if chunk.buffer == nil {
		chunk.size += len(p)
	} else {
		chunk.buffer.Write(p)
	}

	// hasher may be nil, when the chunk is used to stored encrypted content
	if chunk.hasher != nil {
		chunk.hasher.Write(p)
	}
	return len(p), nil
}

// GetHash returns the chunk hash.
func (chunk *Chunk) GetHash() string {
	if len(chunk.hash) == 0 {
		chunk.hash = chunk.hasher.Sum(nil)
	}

	return string(chunk.hash)
}

// GetID returns the chunk id.
func (chunk *Chunk) GetID() string {
	if len(chunk.id) == 0 {
		if len(chunk.hash) == 0 {
			chunk.hash = chunk.hasher.Sum(nil)
		}

		hasher := chunk.config.NewKeyedHasher(chunk.config.IDKey)
		hasher.Write([]byte(chunk.hash))
		chunk.id = hex.EncodeToString(hasher.Sum(nil))
	}

	return chunk.id
}

func (chunk *Chunk) VerifyID() {
	hasher := chunk.config.NewKeyedHasher(chunk.config.HashKey)
	hasher.Write(chunk.buffer.Bytes())
	hash := hasher.Sum(nil)
	hasher = chunk.config.NewKeyedHasher(chunk.config.IDKey)
	hasher.Write([]byte(hash))
	chunkID := hex.EncodeToString(hasher.Sum(nil))
	if chunkID != chunk.GetID() {
		LOG_ERROR("CHUNK_ID", "The chunk id should be %s instead of %s, length: %d", chunkID, chunk.GetID(), len(chunk.buffer.Bytes()))
	}
}

// Encrypt encrypts the plain data stored in the chunk buffer.  If derivationKey is not nil, the actual
// encryption key will be HMAC-SHA256(encryptionKey, derivationKey).
func (chunk *Chunk) Encrypt(encryptionKey []byte, derivationKey string, isMetadata bool) (err error) {

	var aesBlock cipher.Block
	var gcm cipher.AEAD
	var nonce []byte
	var offset int

	encryptedBuffer := AllocateChunkBuffer()
	encryptedBuffer.Reset()
	defer func() {
		ReleaseChunkBuffer(encryptedBuffer)
	}()

	if len(encryptionKey) > 0 {

		key := encryptionKey
		usingRSA := false
		// Enable RSA encryption only when the chunk is not a metadata chunk
		if chunk.config.rsaPublicKey != nil && !isMetadata && !chunk.isMetadata {
			randomKey := make([]byte, 32)
			_, err := rand.Read(randomKey)
			if err != nil {
				return err
			}
			key = randomKey
			usingRSA = true
		} else if len(derivationKey) > 0 {
			hasher := chunk.config.NewKeyedHasher([]byte(derivationKey))
			hasher.Write(encryptionKey)
			key = hasher.Sum(nil)
		}

		aesBlock, err = aes.NewCipher(key)
		if err != nil {
			return err
		}

		gcm, err = cipher.NewGCM(aesBlock)
		if err != nil {
			return err
		}

		// Start with the magic number and the version number.
		if usingRSA {
			// RSA encryption starts "duplicacy\002"
			encryptedBuffer.Write([]byte(ENCRYPTION_BANNER)[:len(ENCRYPTION_BANNER) - 1])
			encryptedBuffer.Write([]byte{ENCRYPTION_VERSION_RSA})

			// Then the encrypted key
			encryptedKey, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, chunk.config.rsaPublicKey, key, nil)
			if err != nil {
				return err
			}
			binary.Write(encryptedBuffer, binary.LittleEndian, uint16(len(encryptedKey)))
			encryptedBuffer.Write(encryptedKey)
		} else {
			encryptedBuffer.Write([]byte(ENCRYPTION_BANNER))
		}

		// Followed by the nonce
		nonce = make([]byte, gcm.NonceSize())
		_, err := rand.Read(nonce)
		if err != nil {
			return err
		}
		encryptedBuffer.Write(nonce)
		offset = encryptedBuffer.Len()
	}

	// offset is either 0 or the length of banner + nonce

	if chunk.config.CompressionLevel >= -1 && chunk.config.CompressionLevel <= 9 {
		deflater, _ := zlib.NewWriterLevel(encryptedBuffer, chunk.config.CompressionLevel)
		deflater.Write(chunk.buffer.Bytes())
		deflater.Close()
	} else if chunk.config.CompressionLevel >= ZSTD_COMPRESSION_LEVEL_FASTEST && chunk.config.CompressionLevel <= ZSTD_COMPRESSION_LEVEL_BEST  {
		encryptedBuffer.Write([]byte("ZSTD"))

		compressionLevel := zstd.SpeedDefault
		if chunk.config.CompressionLevel == ZSTD_COMPRESSION_LEVEL_FASTEST {
			compressionLevel = zstd.SpeedFastest
		} else if chunk.config.CompressionLevel == ZSTD_COMPRESSION_LEVEL_BETTER {
			compressionLevel = zstd.SpeedBetterCompression
		} else if chunk.config.CompressionLevel == ZSTD_COMPRESSION_LEVEL_BEST {
			compressionLevel = zstd.SpeedBestCompression
		}

		deflater, err := zstd.NewWriter(encryptedBuffer, zstd.WithEncoderLevel(compressionLevel))
		if err != nil {
			return err
		}

		// Make sure we have enough space in encryptedBuffer
		availableLength := encryptedBuffer.Cap() - len(encryptedBuffer.Bytes())
		maximumLength := deflater.MaxEncodedSize(chunk.buffer.Len())
		if availableLength < maximumLength {
			encryptedBuffer.Grow(maximumLength - availableLength)
		}
		_, err = deflater.Write(chunk.buffer.Bytes())
		if err != nil {
			return fmt.Errorf("ZSTD compression error: %v", err)
		}

		err = deflater.Close()
		if err != nil {
			return fmt.Errorf("ZSTD compression error: %v", err)
		}
	} else if chunk.config.CompressionLevel == DEFAULT_COMPRESSION_LEVEL {
		encryptedBuffer.Write([]byte("LZ4 "))
		// Make sure we have enough space in encryptedBuffer
		availableLength := encryptedBuffer.Cap() - len(encryptedBuffer.Bytes())
		maximumLength := lz4.CompressBound(len(chunk.buffer.Bytes()))
		if availableLength < maximumLength {
			encryptedBuffer.Grow(maximumLength - availableLength)
		}
		written, err := lz4.Encode(encryptedBuffer.Bytes()[offset+4:], chunk.buffer.Bytes())
		if err != nil {
			return fmt.Errorf("LZ4 compression error: %v", err)
		}
		// written is actually encryptedBuffer[offset + 4:], but we need to move the write pointer
		// and this seems to be the only way
		encryptedBuffer.Write(written)
	} else {
		return fmt.Errorf("Invalid compression level: %d", chunk.config.CompressionLevel)
	}

	if len(encryptionKey) > 0 {

		// PKCS7 is used.  The sizes of compressed chunks leak information about the original chunks so we want the padding sizes
		// to be the maximum allowed by PKCS7
		dataLength := encryptedBuffer.Len() - offset
		paddingLength := 256 - dataLength%256

		encryptedBuffer.Write(bytes.Repeat([]byte{byte(paddingLength)}, paddingLength))
		encryptedBuffer.Write(bytes.Repeat([]byte{0}, gcm.Overhead()))

		// The encrypted data will be appended to the duplicacy banner and the once.
		encryptedBytes := gcm.Seal(encryptedBuffer.Bytes()[:offset], nonce,
			encryptedBuffer.Bytes()[offset:offset+dataLength+paddingLength], nil)

		encryptedBuffer.Truncate(len(encryptedBytes))
	}

	if chunk.config.DataShards == 0 || chunk.config.ParityShards == 0 {
		chunk.buffer, encryptedBuffer = encryptedBuffer, chunk.buffer
		return
	}

	// Start erasure coding
	encoder, err := reedsolomon.New(chunk.config.DataShards, chunk.config.ParityShards)
	if err != nil {
		return err
	}
	chunkSize := len(encryptedBuffer.Bytes())
	shardSize := (chunkSize + chunk.config.DataShards - 1) / chunk.config.DataShards
	// Append zeros to make the last shard to have the same size as other
	encryptedBuffer.Write(make([]byte, shardSize * chunk.config.DataShards - chunkSize))
	// Grow the buffer for parity shards
	encryptedBuffer.Grow(shardSize * chunk.config.ParityShards)
	// Now create one slice for each shard, reusing the data in the buffer
	data := make([][]byte, chunk.config.DataShards + chunk.config.ParityShards)
	for i := 0; i < chunk.config.DataShards + chunk.config.ParityShards; i++ {
		data[i] = encryptedBuffer.Bytes()[i * shardSize: (i + 1) * shardSize]
	}
	// This populates the parity shard
	encoder.Encode(data)

	// Prepare the chunk to be uploaded
	chunk.buffer.Reset()
	// First the banner
	chunk.buffer.Write([]byte(ERASURE_CODING_BANNER))
	// Then the header which includes the chunk size, data/parity and a 2-byte checksum
	header := make([]byte, 14)
	binary.LittleEndian.PutUint64(header[0:], uint64(chunkSize))
	binary.LittleEndian.PutUint16(header[8:], uint16(chunk.config.DataShards))
	binary.LittleEndian.PutUint16(header[10:], uint16(chunk.config.ParityShards))
	header[12] = header[0] ^ header[2] ^ header[4] ^ header[6] ^ header[8] ^ header[10]
	header[13] = header[1] ^ header[3] ^ header[5] ^ header[7] ^ header[9] ^ header[11]
	chunk.buffer.Write(header)
	// Calculate the highway hash for each shard
	hashKey := make([]byte, 32)
	for _, part := range data {
		hasher, err := highwayhash.New(hashKey)
		if err != nil {
			return err
		}
		_, err = hasher.Write(part)
		if err != nil {
			return err
		}
		chunk.buffer.Write(hasher.Sum(nil))
	}

	// Copy the data
	for _, part := range data {
		chunk.buffer.Write(part)
	}
	// Append the header again for redundancy
	chunk.buffer.Write(header)

	return nil
}

// This is to ensure compatibility with Vertical Backup, which still uses HMAC-SHA256 (instead of HMAC-BLAKE2) to
// derive the key used to encrypt/decrypt files and chunks.

var DecryptWithHMACSHA256 = false

func init() {
	if value, found := os.LookupEnv("DUPLICACY_DECRYPT_WITH_HMACSHA256"); found && value != "0" {
		DecryptWithHMACSHA256 = true
	}
}

// Decrypt decrypts the encrypted data stored in the chunk buffer.  If derivationKey is not nil, the actual
// encryption key will be HMAC-SHA256(encryptionKey, derivationKey).
func (chunk *Chunk) Decrypt(encryptionKey []byte, derivationKey string) (err error, rewriteNeeded bool) {

	rewriteNeeded = false
	var offset int

	encryptedBuffer := AllocateChunkBuffer()
	encryptedBuffer.Reset()
	defer func() {
		ReleaseChunkBuffer(encryptedBuffer)
	}()

	chunk.buffer, encryptedBuffer = encryptedBuffer, chunk.buffer
	bannerLength := len(ENCRYPTION_BANNER)

	if len(encryptedBuffer.Bytes()) > bannerLength && string(encryptedBuffer.Bytes()[:bannerLength]) == ERASURE_CODING_BANNER {

		// The chunk was encoded with erasure coding
		if len(encryptedBuffer.Bytes()) < bannerLength + 14 {
			return fmt.Errorf("Erasure coding header truncated (%d bytes)", len(encryptedBuffer.Bytes())), false
		}
		// Check the header checksum
		header := encryptedBuffer.Bytes()[bannerLength: bannerLength + 14]
		if header[12] != header[0] ^ header[2] ^ header[4] ^ header[6] ^ header[8] ^ header[10] ||
		   header[13] != header[1] ^ header[3] ^ header[5] ^ header[7] ^ header[9] ^ header[11] {
			return fmt.Errorf("Erasure coding header corrupted (%x)", header), false
		}

		// Read the parameters
		chunkSize := int(binary.LittleEndian.Uint64(header[0:8]))
		dataShards := int(binary.LittleEndian.Uint16(header[8:10]))
		parityShards := int(binary.LittleEndian.Uint16(header[10:12]))
		shardSize := (chunkSize + chunk.config.DataShards - 1) / chunk.config.DataShards
		// This is the length the chunk file should have
		expectedLength := bannerLength + 2 * len(header) + (dataShards + parityShards) * (shardSize + 32)
		// The minimum length that can be recovered from
		minimumLength := bannerLength + len(header) + (dataShards + parityShards) * 32 + dataShards * shardSize
		LOG_DEBUG("CHUNK_ERASURECODE", "Chunk size: %d bytes, data size: %d, parity: %d/%d", chunkSize, len(encryptedBuffer.Bytes()), dataShards, parityShards)
		if len(encryptedBuffer.Bytes()) > expectedLength {
			LOG_WARN("CHUNK_ERASURECODE", "Chunk has %d bytes (instead of %d)", len(encryptedBuffer.Bytes()), expectedLength)
		} else if len(encryptedBuffer.Bytes()) == expectedLength {
			// Correct size; fall through
		} else if len(encryptedBuffer.Bytes()) > minimumLength {
			LOG_WARN("CHUNK_ERASURECODE", "Chunk is truncated (%d out of %d bytes)", len(encryptedBuffer.Bytes()), expectedLength)
		} else {
			return fmt.Errorf("Not enough chunk data for recovery; chunk size: %d bytes, data size: %d, parity: %d/%d", chunkSize, len(encryptedBuffer.Bytes()), dataShards, parityShards), false
		}

		// Where the hashes start
		hashOffset := bannerLength + len(header)
		// Where the data start
		dataOffset := hashOffset + (dataShards + parityShards) * 32

		data := make([][]byte, dataShards + parityShards)
		recoveryNeeded := false
		hashKey := make([]byte, 32)
		availableShards := 0
		wrongHashDetected := false

		for i := 0; i < dataShards + parityShards; i++ {
			start := dataOffset + i * shardSize
			if start + shardSize > len(encryptedBuffer.Bytes()) {
				// the current shard is incomplete
				break
			}
			// Now verify the hash
			hasher, err := highwayhash.New(hashKey)
			if err != nil {
				return err, false
			}
			_, err = hasher.Write(encryptedBuffer.Bytes()[start: start + shardSize])
			if err != nil {
				return err, false
			}

			matched := bytes.Compare(hasher.Sum(nil), encryptedBuffer.Bytes()[hashOffset + i * 32: hashOffset + (i + 1) * 32]) == 0

			if !matched && runtime.GOARCH == "arm64" {
				hasher, err := wronghighwayhash.New(hashKey)
				if err == nil {
					_, err = hasher.Write(encryptedBuffer.Bytes()[start: start + shardSize])
					if err == nil {
						matched = bytes.Compare(hasher.Sum(nil), encryptedBuffer.Bytes()[hashOffset + i * 32: hashOffset + (i + 1) * 32]) == 0
						if matched && !wrongHashDetected {
							LOG_WARN("CHUNK_ERASURECODE", "Hash for shard %d was calculated with a wrong version of highwayhash", i)
							wrongHashDetected = true
							rewriteNeeded = true
						}
					}
				}
			}

			if !matched {
				if i < dataShards {
					recoveryNeeded = true
					rewriteNeeded = true
				}
			} else {
				// The shard is good
				data[i] = encryptedBuffer.Bytes()[start: start + shardSize]
				availableShards++
				if availableShards >= dataShards {
					// We have enough shards to recover; skip the remaining shards
					break
				}
			}
		}

		if !recoveryNeeded {
			// Remove the padding zeros from the last shard
			encryptedBuffer.Truncate(dataOffset + chunkSize)
			// Skip the header and hashes
			encryptedBuffer.Read(encryptedBuffer.Bytes()[:dataOffset])
		} else {
			if availableShards < dataShards {
				return fmt.Errorf("Not enough chunk data for recover; only %d out of %d shards are complete", availableShards, dataShards + parityShards), false
			}

			// Show the validity of shards using a string of * and -
			slots := ""
			for _, part := range data {
				if len(part) != 0 {
					slots += "*"
				} else {
					slots += "-"
				}
			}

			LOG_WARN("CHUNK_ERASURECODE", "Recovering a %d byte chunk from %d byte shards: %s", chunkSize, shardSize, slots)
			encoder, err := reedsolomon.New(dataShards, parityShards)
			if err != nil {
				return err, false
			}
			err = encoder.Reconstruct(data)
			if err != nil {
				return err, false
			}
			LOG_DEBUG("CHUNK_ERASURECODE", "Chunk data successfully recovered")
			buffer := AllocateChunkBuffer()
			buffer.Reset()
			for i := 0; i < dataShards; i++ {
				buffer.Write(data[i])
			}
			buffer.Truncate(chunkSize)

			ReleaseChunkBuffer(encryptedBuffer)
			encryptedBuffer = buffer
		}

	}

	if len(encryptionKey) > 0 {

		key := encryptionKey

		if len(derivationKey) > 0 {
			var hasher hash.Hash
			if DecryptWithHMACSHA256 {
				hasher = hmac.New(sha256.New, []byte(derivationKey))
			} else {
				hasher = chunk.config.NewKeyedHasher([]byte(derivationKey))
			}

			hasher.Write(encryptionKey)
			key = hasher.Sum(nil)
		}

		if len(encryptedBuffer.Bytes()) < bannerLength + 12 {
			return fmt.Errorf("No enough encrypted data (%d bytes) provided", len(encryptedBuffer.Bytes())), false
		}

		if string(encryptedBuffer.Bytes()[:bannerLength-1]) != ENCRYPTION_BANNER[:bannerLength-1] {
			return fmt.Errorf("The storage doesn't seem to be encrypted"), false
		}

		encryptionVersion := encryptedBuffer.Bytes()[bannerLength-1]
		if encryptionVersion != 0 && encryptionVersion != ENCRYPTION_VERSION_RSA {
			return fmt.Errorf("Unsupported encryption version %d", encryptionVersion), false
		}

		if encryptionVersion == ENCRYPTION_VERSION_RSA {
			if chunk.config.rsaPrivateKey == nil {
				LOG_ERROR("CHUNK_DECRYPT", "An RSA private key is required to decrypt the chunk")
				return fmt.Errorf("An RSA private key is required to decrypt the chunk"), false
			}

			encryptedKeyLength := binary.LittleEndian.Uint16(encryptedBuffer.Bytes()[bannerLength:bannerLength+2])

			if len(encryptedBuffer.Bytes()) < bannerLength + 14 + int(encryptedKeyLength) {
				return fmt.Errorf("No enough encrypted data (%d bytes) provided", len(encryptedBuffer.Bytes())), false
			}

			encryptedKey := encryptedBuffer.Bytes()[bannerLength + 2:bannerLength + 2 + int(encryptedKeyLength)]
			bannerLength += 2 + int(encryptedKeyLength)

			decryptedKey, err := rsa.DecryptOAEP(sha256.New(), rand.Reader, chunk.config.rsaPrivateKey, encryptedKey, nil)
			if err != nil {
				return err, false
			}
			key = decryptedKey
		}

		aesBlock, err := aes.NewCipher(key)
		if err != nil {
			return err, false
		}

		gcm, err := cipher.NewGCM(aesBlock)
		if err != nil {
			return err, false
		}

		offset = bannerLength + gcm.NonceSize()
		nonce := encryptedBuffer.Bytes()[bannerLength:offset]

		decryptedBytes, err := gcm.Open(encryptedBuffer.Bytes()[:offset], nonce,
			encryptedBuffer.Bytes()[offset:], nil)

		if err != nil {
			return err, false
		}

		paddingLength := int(decryptedBytes[len(decryptedBytes)-1])
		if paddingLength == 0 {
			paddingLength = 256
		}
		if len(decryptedBytes) <= paddingLength {
			return fmt.Errorf("Incorrect padding length %d out of %d bytes", paddingLength, len(decryptedBytes)), false
		}

		for i := 0; i < paddingLength; i++ {
			padding := decryptedBytes[len(decryptedBytes)-1-i]
			if padding != byte(paddingLength) {
				return fmt.Errorf("Incorrect padding of length %d: %x", paddingLength,
					decryptedBytes[len(decryptedBytes)-paddingLength:]), false
			}
		}

		encryptedBuffer.Truncate(len(decryptedBytes) - paddingLength)
	}

	encryptedBuffer.Read(encryptedBuffer.Bytes()[:offset])

	compressed := encryptedBuffer.Bytes()
	if len(compressed) > 4 && string(compressed[:4]) == "LZ4 " {
		chunk.buffer.Reset()
		decompressed, err := lz4.Decode(chunk.buffer.Bytes(), encryptedBuffer.Bytes()[4:])
		if err != nil {
			return err, false
		}

		chunk.buffer.Write(decompressed)
		chunk.hasher = chunk.config.NewKeyedHasher(chunk.config.HashKey)
		chunk.hasher.Write(decompressed)
		chunk.hash = nil
		return nil, rewriteNeeded
	}

	if len(compressed) > 4 && string(compressed[:4]) == "ZSTD" {
		chunk.buffer.Reset()
		chunk.hasher = chunk.config.NewKeyedHasher(chunk.config.HashKey)
		chunk.hash = nil

		encryptedBuffer.Read(encryptedBuffer.Bytes()[:4])
		inflater, err := zstd.NewReader(encryptedBuffer)
		if err != nil {
			return err, false
		}
		defer inflater.Close()
		if _, err = io.Copy(chunk, inflater); err != nil {
			return err, false
		}
		return nil, rewriteNeeded
	}

	inflater, err := zlib.NewReader(encryptedBuffer)
	if err != nil {
		return err, false
	}

	defer inflater.Close()

	chunk.buffer.Reset()
	chunk.hasher = chunk.config.NewKeyedHasher(chunk.config.HashKey)
	chunk.hash = nil

	if _, err = io.Copy(chunk, inflater); err != nil {
		return err, false
	}

	return nil, rewriteNeeded

}
