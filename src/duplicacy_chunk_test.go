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
