// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "testing"
    "bytes"
    crypto_rand "crypto/rand"
    "math/rand"
)

func TestChunk(t *testing.T) {

    key := []byte("duplicacydefault")

    config := CreateConfig()
    config.HashKey = key
    config.IDKey = key
    config.MinimumChunkSize = 100
    config.CompressionLevel = DEFAULT_COMPRESSION_LEVEL
    maxSize := 1000000

    for i := 0; i < 500; i++ {

        size := rand.Int() % maxSize

        plainData := make([]byte, size)
        crypto_rand.Read(plainData)
        chunk := CreateChunk(config, true)
        chunk.Reset(true)
        chunk.Write(plainData)

        hash := chunk.GetHash()
        id := chunk.GetID()

        err := chunk.Encrypt(key, "")
        if err != nil {
            t.Errorf("Failed to encrypt the data: %v", err)
            continue
        }

        encryptedData := make([]byte, chunk.GetLength())
        copy(encryptedData, chunk.GetBytes())

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
            t.Logf("orginal length: %d, decrypted length: %d", len(plainData), len(decryptedData))
            t.Errorf("Original data:\n%x\nDecrypted data:\n%x\n", plainData, decryptedData)
        }


    }

}
