// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"flag"
	"bytes"
	crypto_rand "crypto/rand"
	"crypto/rsa"
	"math/rand"
	"testing"
)

var testRSAEncryption bool

func init() {
	flag.BoolVar(&testRSAEncryption, "rsa", false, "enable RSA encryption")
	flag.Parse()
}


func TestChunk(t *testing.T) {

	key := []byte("duplicacydefault")

	config := CreateConfig()
	config.HashKey = key
	config.IDKey = key
	config.MinimumChunkSize = 100
	config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
	maxSize := 1000000

	if testRSAEncryption {
		privateKey, err := rsa.GenerateKey(crypto_rand.Reader, 2048)
		if err != nil {
			t.Errorf("Failed to generate a random private key: %v", err)
		}
		config.rsaPrivateKey = privateKey
		config.rsaPublicKey = privateKey.Public().(*rsa.PublicKey)
	}

	remainderLength := -1

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

		if remainderLength == -1 {
			remainderLength = len(encryptedData) % 256
		} else if len(encryptedData)%256 != remainderLength {
			t.Errorf("Incorrect padding size")
		}

		chunk.Reset(false)
		chunk.Write(encryptedData)
		err = chunk.Decrypt(key, "")
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
