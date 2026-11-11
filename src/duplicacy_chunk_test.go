// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	crypto_rand "crypto/rand"
	"crypto/rsa"
	"math/rand"
	"testing"
)

func TestErasureCoding(t *testing.T) {
	key := []byte("duplicacydefault")

	config := CreateConfig()
	config.HashKey = key
	config.IDKey = key
	config.MinimumChunkSize = 100
	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
	config.DataShards = 5
    config.ParityShards = 2

	chunk := CreateChunk(config, true)
	chunk.Reset(true)
	data := make([]byte, 100)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i)
	}
	chunk.Write(data)
	err := chunk.Encrypt([]byte(""), "", false)
	if err != nil {
		t.Errorf("Failed to encrypt the test data: %v", err)
		return
	}

	encryptedData := make([]byte, chunk.GetLength())
	copy(encryptedData, chunk.GetBytes())

	crypto_rand.Read(encryptedData[280:300])

	chunk.Reset(false)
	chunk.Write(encryptedData)
	err, _ = chunk.Decrypt([]byte(""), "")
	if err != nil {
		t.Errorf("Failed to decrypt the data: %v", err)
		return
	}
	return
}

// verifyChunk calls VerifyChecksum and reports whether it rejected the chunk, which is signaled by LOG_ERROR
// raising an Exception.
func verifyChunk(chunk *Chunk) (rejected bool) {
	// A previous test may have left testingT set, which would turn the expected error into a test failure
	savedTestingT := testingT
	testingT = nil

	defer func() {
		testingT = savedTestingT
		if r := recover(); r != nil {
			if exception, ok := r.(Exception); ok && exception.LogID == "CHUNK_ID" {
				rejected = true
				return
			}
			panic(r)
		}
	}()

	chunk.VerifyChecksum()
	return false
}

// encryptChunk calls Encrypt and reports whether the checksum verification it performs rejected the chunk
func encryptChunk(chunk *Chunk, key []byte) (rejected bool, err error) {
	savedTestingT := testingT
	testingT = nil

	defer func() {
		testingT = savedTestingT
		if r := recover(); r != nil {
			if exception, ok := r.(Exception); ok && exception.LogID == "CHUNK_ID" {
				rejected = true
				return
			}
			panic(r)
		}
	}()

	err = chunk.Encrypt(key, "", false)
	return false, err
}

func TestChunkChecksum(t *testing.T) {

	key := []byte("duplicacydefault")

	config := CreateConfig()
	config.HashKey = key
	config.IDKey = key
	config.MinimumChunkSize = 100
	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL

	plainData := make([]byte, 100000)
	crypto_rand.Read(plainData)

	// An intact chunk must pass
	chunk := CreateChunk(config, true)
	chunk.Reset(true)
	chunk.Write(plainData)
	chunk.GetID()
	if verifyChunk(chunk) {
		t.Errorf("An intact chunk was rejected")
	}

	// Every single bit flipped in the buffer after the chunk was hashed must be caught
	for _, offset := range []int{0, len(plainData) / 2, len(plainData) - 1} {
		chunk = CreateChunk(config, true)
		chunk.Reset(true)
		chunk.Write(plainData)
		chunk.GetID()
		chunk.GetBytes()[offset] ^= 1
		if !verifyChunk(chunk) {
			t.Errorf("A chunk corrupted at offset %d was not rejected", offset)
		}
	}

	// Data written in multiple calls must be checksummed the same as data written in one call
	chunk = CreateChunk(config, true)
	chunk.Reset(true)
	for i := 0; i < len(plainData); i += 7777 {
		end := i + 7777
		if end > len(plainData) {
			end = len(plainData)
		}
		chunk.Write(plainData[i:end])
	}
	chunk.GetID()
	if verifyChunk(chunk) {
		t.Errorf("A chunk written in multiple calls was rejected")
	}

	// A chunk with no hash, such as a snapshot file or the config file, has nothing to verify against
	chunk = CreateChunk(config, true)
	chunk.Reset(false)
	chunk.Write(plainData)
	chunk.GetBytes()[0] ^= 1
	if verifyChunk(chunk) {
		t.Errorf("A chunk with no hash was verified")
	}

	// A hash-only chunk has no buffer to verify
	chunk = CreateChunk(config, false)
	chunk.Reset(true)
	chunk.Write(plainData)
	if verifyChunk(chunk) {
		t.Errorf("A hash-only chunk was verified")
	}

	// Encrypt verifies the chunk itself, which is what covers every path that stores a chunk
	for _, compressionLevel := range []int{DEFAULT_COMPRESSION_LEVEL, 3, ZSTD_COMPRESSION_LEVEL_DEFAULT} {
		config.CompressionLevel = compressionLevel

		chunk = CreateChunk(config, true)
		chunk.Reset(true)
		chunk.Write(plainData)
		chunk.GetID()
		if rejected, err := encryptChunk(chunk, key); err != nil {
			t.Errorf("[%d] Failed to encrypt the data: %v", compressionLevel, err)
		} else if rejected {
			t.Errorf("[%d] Encrypt rejected an intact chunk", compressionLevel)
		}

		chunk = CreateChunk(config, true)
		chunk.Reset(true)
		chunk.Write(plainData)
		chunk.GetID()
		chunk.GetBytes()[len(plainData)/2] ^= 1
		if rejected, err := encryptChunk(chunk, key); err != nil {
			t.Errorf("[%d] Failed to encrypt the corrupted data: %v", compressionLevel, err)
		} else if !rejected {
			t.Errorf("[%d] Encrypt accepted a corrupted chunk", compressionLevel)
		}

		// A chunk with no hash, such as a snapshot file or the config file, must still encrypt
		chunk = CreateChunk(config, true)
		chunk.Reset(false)
		chunk.Write(plainData)
		if rejected, err := encryptChunk(chunk, key); err != nil {
			t.Errorf("[%d] Failed to encrypt a chunk with no hash: %v", compressionLevel, err)
		} else if rejected {
			t.Errorf("[%d] Encrypt verified a chunk with no hash", compressionLevel)
		}
	}
	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL

	// Once the chunk has been encrypted the buffer no longer holds the data the checksum was computed from
	chunk = CreateChunk(config, true)
	chunk.Reset(true)
	chunk.Write(plainData)
	chunk.GetID()
	if err := chunk.Encrypt(key, "", false); err != nil {
		t.Errorf("Failed to encrypt the data: %v", err)
	} else if verifyChunk(chunk) {
		t.Errorf("An encrypted chunk was verified against the checksum of its plain data")
	}
}

// TestWriteRawData covers the constructor that turns a chunk copied verbatim into one that can be uploaded: the bytes
// are kept as they came from the storage, the identity is taken from the given hash rather than recomputed, and the
// checksum still guards the buffer.
func TestWriteRawData(t *testing.T) {

	setTestingT(t)

	config := CreateConfig()
	config.HashKey = []byte("duplicacydefault")
	config.IDKey = []byte("duplicacydefault")

	hash := string(bytes.Repeat([]byte{0xab}, 32))

	storedData := make([]byte, 4096)
	crypto_rand.Read(storedData)

	chunk := CreateChunk(config, true)
	chunk.WriteRawData(storedData, hash)

	if !chunk.isRawData {
		t.Errorf("WriteRawData did not mark the chunk as holding stored data")
	}
	if !bytes.Equal([]byte(chunk.GetHash()), []byte(hash)) {
		t.Errorf("WriteRawData stored the chunk under a different hash: %x vs %x", chunk.GetHash(), hash)
	}
	if chunk.GetID() != config.GetChunkIDFromHash(hash) {
		t.Errorf("WriteRawData derived the id %s instead of %s", chunk.GetID(), config.GetChunkIDFromHash(hash))
	}
	if !bytes.Equal(chunk.GetBytes(), storedData) {
		t.Errorf("WriteRawData changed the bytes of the chunk")
	}

	// The checksum covers the stored bytes, so corruption in memory is still caught before the chunk is uploaded
	chunk.GetBytes()[0] ^= 1
	if !verifyChunk(chunk) {
		t.Errorf("A stored chunk corrupted in memory was not rejected")
	}
}

func TestChunkBasic(t *testing.T) {

	key := []byte("duplicacydefault")

	config := CreateConfig()
	config.HashKey = key
	config.IDKey = key
	config.MinimumChunkSize = 100
	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
	maxSize := 1000000

	if *testRSAEncryption {
		privateKey, err := rsa.GenerateKey(crypto_rand.Reader, 2048)
		if err != nil {
			t.Errorf("Failed to generate a random private key: %v", err)
		}
		config.rsaPrivateKey = privateKey
		config.rsaPublicKey = privateKey.Public().(*rsa.PublicKey)
	}

	if *testErasureCoding {
		config.DataShards = 5
		config.ParityShards = 2
	}

	for i := 0; i < 500; i++ {

		size := rand.Int() % maxSize

		plainData := make([]byte, size)
		crypto_rand.Read(plainData)
		chunk := CreateChunk(config, true)
		chunk.Reset(true)
		chunk.Write(plainData)

		hash := chunk.GetHash()
		id := chunk.GetID()

		err := chunk.Encrypt(key, "", false)
		if err != nil {
			t.Errorf("Failed to encrypt the data: %v", err)
			continue
		}

		encryptedData := make([]byte, chunk.GetLength())
		copy(encryptedData, chunk.GetBytes())

		if *testErasureCoding {
			offset := 24 + 32 * 7
			start := rand.Int() % (len(encryptedData) - offset) + offset
			length := (len(encryptedData) - offset) / 7
			if start + length > len(encryptedData) {
				length = len(encryptedData) - start
			}
			crypto_rand.Read(encryptedData[start: start+length])
		}

		chunk.Reset(false)
		chunk.Write(encryptedData)
		err, _ = chunk.Decrypt(key, "")
		if err != nil {
			t.Errorf("Failed to decrypt the data: %v", err)
			continue
		}

		decryptedData := chunk.GetBytes()

		if hash != chunk.GetHash() {
			t.Errorf("Original hash: %x, decrypted hash: %x", hash, chunk.GetHash())
		}

		if id != chunk.GetID() {
			t.Errorf("Original id: %s, decrypted hash: %s", id, chunk.GetID())
		}

		if bytes.Compare(plainData, decryptedData) != 0 {
			t.Logf("Original length: %d, decrypted length: %d", len(plainData), len(decryptedData))
			t.Errorf("Original data:\n%x\nDecrypted data:\n%x\n", plainData, decryptedData)
		}

	}

}
