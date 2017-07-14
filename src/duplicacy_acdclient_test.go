// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
    "io"
    "fmt"
    "testing"
    "crypto/sha256"
    "encoding/hex"

    crypto_rand "crypto/rand"
    "math/rand"
)

func TestACDClient(t *testing.T) {

    acdClient, err := NewACDClient("acd-token.json")
    if err != nil {
        t.Errorf("Failed to create the ACD client: %v", err)
        return
    }

    acdClient.TestMode = true

    rootID, _, _, err := acdClient.ListByName("", "")
    if err != nil {
        t.Errorf("Failed to get the root node: %v", err)
        return
    }

    if rootID == "" {
        t.Errorf("No root node")
        return
    }

    testID, _, _, err := acdClient.ListByName(rootID, "test")
    if err != nil {
        t.Errorf("Failed to list the test directory: %v", err)
        return
    }
    if testID == "" {
        testID, err = acdClient.CreateDirectory(rootID, "test")
        if err != nil {
            t.Errorf("Failed to create the test directory: %v", err)
            return
        }
    }

    test1ID, _, _, err := acdClient.ListByName(testID, "test1")
    if err != nil {
        t.Errorf("Failed to list the test1 directory: %v", err)
        return
    }
    if test1ID == "" {
        test1ID, err = acdClient.CreateDirectory(testID, "test1")
        if err != nil {
            t.Errorf("Failed to create the test1 directory: %v", err)
            return
        }
    }

    test2ID, _, _, err := acdClient.ListByName(testID, "test2")
    if err != nil {
        t.Errorf("Failed to list the test2 directory: %v", err)
        return
    }
    if test2ID == "" {
        test2ID, err = acdClient.CreateDirectory(testID, "test2")
        if err != nil {
            t.Errorf("Failed to create the test2 directory: %v", err)
            return
        }
    }

    fmt.Printf("test1: %s, test2: %s\n", test1ID, test2ID)

    numberOfFiles := 20
    maxFileSize := 64 * 1024

    for i := 0; i < numberOfFiles; i++ {
        content := make([]byte, rand.Int() % maxFileSize + 1)
        _, err = crypto_rand.Read(content)
        if err != nil {
            t.Errorf("Error generating random content: %v", err)
            return
        }

        hasher := sha256.New()
        hasher.Write(content)
        filename := hex.EncodeToString(hasher.Sum(nil))

        fmt.Printf("file: %s\n", filename)

        _, err = acdClient.UploadFile(test1ID, filename, content, 100)
        if err != nil {
            /*if e, ok := err.(ACDError); !ok || e.Status != 409 */ {
                t.Errorf("Failed to upload the file %s: %v", filename, err)
                return
            }
        }
    }

    entries, err := acdClient.ListEntries(test1ID, true)
    if err != nil {
        t.Errorf("Error list randomly generated files: %v", err)
        return
    }

    for _, entry := range entries {
        err = acdClient.MoveFile(entry.ID, test1ID, test2ID)
        if err != nil {
            t.Errorf("Failed to move %s: %v", entry.Name, err)
            return
        }
    }

    entries, err = acdClient.ListEntries(test2ID, true)
    if err != nil {
        t.Errorf("Error list randomly generated files: %v", err)
        return
    }

    for _, entry := range entries {
        readCloser, _, err := acdClient.DownloadFile(entry.ID)
        if err != nil {
            t.Errorf("Error downloading file %s: %v", entry.Name, err)
            return
        }

        hasher := sha256.New()
        io.Copy(hasher, readCloser)
        hash := hex.EncodeToString(hasher.Sum(nil))

        if hash != entry.Name {
            t.Errorf("File %s, hash %s", entry.Name, hash)
        }

        readCloser.Close()
    }

    for _, entry := range entries {

        err = acdClient.DeleteFile(entry.ID)
        if err != nil {
            t.Errorf("Failed to delete the file %s: %v", entry.Name, err)
            return
        }
    }

}
