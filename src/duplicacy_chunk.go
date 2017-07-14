// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
    "io"
    "fmt"
    "hash"
    "bytes"
    "runtime"
    "crypto/cipher"
    "crypto/aes"
    "crypto/rand"
    "encoding/hex"
    "compress/zlib"

    "github.com/bkaradzic/go-lz4"

)

// A chunk needs to acquire a new buffer and return the old one for every encrypt/decrypt operation, therefore
// we maintain a pool of previously used buffers.
var chunkBufferPool chan *bytes.Buffer = make(chan *bytes.Buffer, runtime.NumCPU() * 16)

func AllocateChunkBuffer() (buffer *bytes.Buffer) {
    select {
    case buffer = <- chunkBufferPool:
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
    buffer  *bytes.Buffer  // Where the actual data is stored.  It may be nil for hash-only chunks, where chunks
                           // are only used to compute the hashes

    size    int            // The size of data stored.  This field is needed if buffer is nil

    hasher hash.Hash       // Keeps track of the hash of data stored in the buffer.  It may be nil, since sometimes
                           // it isn't necessary to compute the hash, for instance, when the encrypted data is being
                           // read into the primary buffer

    hash   []byte          // The hash of the chunk data.  It is always in the binary format
    id     string          // The id of the chunk data (used as the file name for saving the chunk); always in hex format

    config *Config         // Every chunk is associated with a Config object.  Which hashing algorithm to use is determined
                           // by the config
}

// Magic word to identify a duplicacy format encrypted file, plus a version number.
var ENCRYPTION_HEADER = "duplicacy\000"

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

    return &Chunk {
        buffer     : buffer,
        config     : config,
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
func (chunk *Chunk) GetBytes() [] byte {
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
}

// Write implements the Writer interface.
func (chunk *Chunk) Write(p []byte) (int, error){

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
    if (len(chunk.hash) == 0) {
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
func (chunk *Chunk) Encrypt(encryptionKey []byte, derivationKey string) (err error) {

    var aesBlock cipher.Block
    var gcm cipher.AEAD
    var nonce []byte
    var offset int

    encryptedBuffer := AllocateChunkBuffer()
    encryptedBuffer.Reset()
    defer func() {
        ReleaseChunkBuffer(encryptedBuffer)
    } ()

    if len(encryptionKey) > 0 {

        key := encryptionKey

        if len(derivationKey) > 0 {
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
        encryptedBuffer.Write([]byte(ENCRYPTION_HEADER))

        // Followed by the nonce
        nonce = make([]byte, gcm.NonceSize())
        _, err := rand.Read(nonce)
        if err != nil {
            return err
        }
        encryptedBuffer.Write(nonce)
        offset = encryptedBuffer.Len()

    }

    // offset is either 0 or the length of header + nonce

    if chunk.config.CompressionLevel >= -1 && chunk.config.CompressionLevel <= 9 {
        deflater, _ := zlib.NewWriterLevel(encryptedBuffer, chunk.config.CompressionLevel)
        deflater.Write(chunk.buffer.Bytes())
        deflater.Close()
    } else if chunk.config.CompressionLevel == DEFAULT_COMPRESSION_LEVEL {
        encryptedBuffer.Write([]byte("LZ4 "))
        // Make sure we have enough space in encryptedBuffer
        availableLength := encryptedBuffer.Cap() - len(encryptedBuffer.Bytes())
        maximumLength := lz4.CompressBound(len(chunk.buffer.Bytes()))
        if availableLength < maximumLength {
            encryptedBuffer.Grow(maximumLength - availableLength)
        }
        written, err := lz4.Encode(encryptedBuffer.Bytes()[offset + 4:], chunk.buffer.Bytes())
        if err != nil {
            return fmt.Errorf("LZ4 compression error: %v", err)
        }
        // written is actually encryptedBuffer[offset + 4:], but we need to move the write pointer
        // and this seems to be the only way
        encryptedBuffer.Write(written)
    } else {
        return fmt.Errorf("Invalid compression level: %d", chunk.config.CompressionLevel)
    }

    if len(encryptionKey) == 0 {
        chunk.buffer, encryptedBuffer = encryptedBuffer, chunk.buffer
        return nil
    }

    // PKCS7 is used.  Compressed chunk sizes leaks information about the original chunks so we want the padding sizes
    // to be the maximum allowed by PKCS7
    dataLength := encryptedBuffer.Len() - offset
    paddingLength := dataLength % 256
    if paddingLength == 0 {
        paddingLength = 256
    }

    encryptedBuffer.Write(bytes.Repeat([]byte{byte(paddingLength)}, paddingLength))
    encryptedBuffer.Write(bytes.Repeat([]byte{0}, gcm.Overhead()))

    // The encrypted data will be appended to the duplicacy header and the once.
    encryptedBytes := gcm.Seal(encryptedBuffer.Bytes()[:offset], nonce,
                               encryptedBuffer.Bytes()[offset: offset + dataLength + paddingLength], nil)

    encryptedBuffer.Truncate(len(encryptedBytes))

    chunk.buffer, encryptedBuffer = encryptedBuffer, chunk.buffer

    return nil

}

// Decrypt decrypts the encrypted data stored in the chunk buffer.  If derivationKey is not nil, the actual
// encryption key will be HMAC-SHA256(encryptionKey, derivationKey).
func (chunk *Chunk) Decrypt(encryptionKey []byte, derivationKey string) (err error) {

    var offset int

    encryptedBuffer := AllocateChunkBuffer()
    encryptedBuffer.Reset()
    defer func() {
        ReleaseChunkBuffer(encryptedBuffer)
    } ()

    chunk.buffer, encryptedBuffer = encryptedBuffer, chunk.buffer

    if len(encryptionKey) > 0 {

        key := encryptionKey

        if len(derivationKey) > 0 {
            hasher := chunk.config.NewKeyedHasher([]byte(derivationKey))
            hasher.Write(encryptionKey)
            key = hasher.Sum(nil)
        }

        aesBlock, err := aes.NewCipher(key)
        if err != nil {
            return err
        }

        gcm, err := cipher.NewGCM(aesBlock)
        if err != nil {
            return err
        }

        headerLength := len(ENCRYPTION_HEADER)
        offset = headerLength + gcm.NonceSize()

        if len(encryptedBuffer.Bytes()) < offset {
            return fmt.Errorf("No enough encrypted data (%d bytes) provided", len(encryptedBuffer.Bytes()))
        }

        if string(encryptedBuffer.Bytes()[:headerLength - 1]) != ENCRYPTION_HEADER[:headerLength - 1] {
            return fmt.Errorf("The storage doesn't seem to be encrypted")
        }

        if encryptedBuffer.Bytes()[headerLength - 1] != 0 {
            return fmt.Errorf("Unsupported encryption version %d", encryptedBuffer.Bytes()[headerLength - 1])
        }

        nonce := encryptedBuffer.Bytes()[headerLength: offset]

        decryptedBytes, err := gcm.Open(encryptedBuffer.Bytes()[:offset], nonce,
                                        encryptedBuffer.Bytes()[offset:], nil)

        if err != nil {
            return err
        }

        paddingLength := int(decryptedBytes[len(decryptedBytes) - 1])
        if paddingLength == 0 {
            paddingLength = 256
        }
        if len(decryptedBytes) <= paddingLength {
            return fmt.Errorf("Incorrect padding length %d out of %d bytes", paddingLength, len(decryptedBytes))
        }

        for i := 0; i < paddingLength; i++ {
            padding := decryptedBytes[len(decryptedBytes) - 1 - i]
            if padding != byte(paddingLength) {
                return fmt.Errorf("Incorrect padding of length %d: %x", paddingLength,
                                  decryptedBytes[len(decryptedBytes) - paddingLength:])
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
            return err
        }

        chunk.buffer.Write(decompressed)
        chunk.hasher = chunk.config.NewKeyedHasher(chunk.config.HashKey)
        chunk.hasher.Write(decompressed)
        chunk.hash = nil
        return nil
    }
    inflater, err := zlib.NewReader(encryptedBuffer)
    if err != nil {
        return err
    }

    defer inflater.Close()

    chunk.buffer.Reset()
    chunk.hasher = chunk.config.NewKeyedHasher(chunk.config.HashKey)
    chunk.hash = nil

    if _, err = io.Copy(chunk, inflater); err != nil {
        return err
    }

    return nil

}

