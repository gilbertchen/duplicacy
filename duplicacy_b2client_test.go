// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "testing"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"

    crypto_rand "crypto/rand"
    "math/rand"
    "io"
    "io/ioutil"
)

func createB2ClientForTest(t *testing.T) (*B2Client, string) {
    config, err := ioutil.ReadFile("test_storage.conf")
    if err != nil {
        t.Errorf("Failed to read config file: %v", err)
        return nil, ""
    }

    storages := make(map[string]map[string]string)

    err = json.Unmarshal(config, &storages)
    if err != nil {
        t.Errorf("Failed to parse config file: %v", err)
        return nil, ""
    }

    b2, found := storages["b2"]
    if !found {
        t.Errorf("Failed to find b2 config")
        return nil, ""
    }

    return NewB2Client(b2["account"], b2["key"]), b2["bucket"]

}

func TestB2Client(t *testing.T) {

    b2Client, bucket := createB2ClientForTest(t)
    if b2Client == nil {
        return
    }

    b2Client.TestMode = true

    err := b2Client.AuthorizeAccount()
    if err != nil {
        t.Errorf("Failed to authorize the b2 account: %v", err)
        return
    }

    err = b2Client.FindBucket(bucket)
    if err != nil {
        t.Errorf("Failed to find bucket '%s': %v", bucket, err)
        return
    }

    testDirectory := "b2client_test/"

    files, err := b2Client.ListFileNames(testDirectory, false, false)
    if err != nil {
        t.Errorf("Failed to list files: %v", err)
        return
    }

    for _, file := range files {
        err = b2Client.DeleteFile(file.FileName, file.FileID)
        if err != nil {
            t.Errorf("Failed to delete file '%s': %v", file.FileName, err)
        }
    }

    maxSize := 10000
    for i := 0; i < 20; i++ {
        size := rand.Int() % maxSize + 1
        content := make([]byte, size)
        _, err := crypto_rand.Read(content)
        if err != nil {
            t.Errorf("Error generating random content: %v", err)
            return
        }

        hash := sha256.Sum256(content)
        name := hex.EncodeToString(hash[:])

        err = b2Client.UploadFile(testDirectory + name, content, 100)
        if err != nil {
            t.Errorf("Error uploading file '%s': %v", name, err)
            return
        }
    }

    files, err = b2Client.ListFileNames(testDirectory, false, false)
    if err != nil {
        t.Errorf("Failed to list files: %v", err)
        return
    }

    for _, file := range files {

        readCloser, _, err := b2Client.DownloadFile(file.FileName)
        if err != nil {
            t.Errorf("Error downloading file '%s': %v", file.FileName, err)
            return
        }

        defer readCloser.Close()

        hasher := sha256.New()
        _, err = io.Copy(hasher, readCloser)

        hash := hex.EncodeToString(hasher.Sum(nil))

        if testDirectory + hash != file.FileName {
            t.Errorf("File %s has hash %s", file.FileName, hash)
        }

    }

    for _, file := range files {
        err = b2Client.DeleteFile(file.FileName, file.FileID)
        if err != nil {
            t.Errorf("Failed to delete file '%s': %v", file.FileName, err)
        }
    }
}
