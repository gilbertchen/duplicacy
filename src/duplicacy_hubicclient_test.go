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

func TestHubicClient(t *testing.T) {

    hubicClient, err := NewHubicClient("hubic-token.json")
    if err != nil {
        t.Errorf("Failed to create the Hubic client: %v", err)
        return
    }

    hubicClient.TestMode = true

    existingFiles, err := hubicClient.ListEntries("")
    for _, file := range existingFiles {
        fmt.Printf("name: %s, isDir: %t\n", file.Name, file.Type == "application/directory")
    }

    testExists, _, _, err := hubicClient.GetFileInfo("test")
    if err != nil {
        t.Errorf("Failed to list the test directory: %v", err)
        return
    }
    if !testExists {
        err = hubicClient.CreateDirectory("test")
        if err != nil {
            t.Errorf("Failed to create the test directory: %v", err)
            return
        }
    }

    test1Exists, _, _, err := hubicClient.GetFileInfo("test/test1")
    if err != nil {
        t.Errorf("Failed to list the test1 directory: %v", err)
        return
    }
    if !test1Exists {
        err = hubicClient.CreateDirectory("test/test1")
        if err != nil {
            t.Errorf("Failed to create the test1 directory: %v", err)
            return
        }
    }

    test2Exists, _, _, err := hubicClient.GetFileInfo("test/test2")
    if err != nil {
        t.Errorf("Failed to list the test2 directory: %v", err)
        return
    }
    if !test2Exists {
        err = hubicClient.CreateDirectory("test/test2")
        if err != nil {
            t.Errorf("Failed to create the test2 directory: %v", err)
            return
        }
    }

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

        err = hubicClient.UploadFile("test/test1/" + filename, content, 100)
        if err != nil {
            /*if e, ok := err.(ACDError); !ok || e.Status != 409 */ {
                t.Errorf("Failed to upload the file %s: %v", filename, err)
                return
            }
        }
    }

    entries, err := hubicClient.ListEntries("test/test1")
    if err != nil {
        t.Errorf("Error list randomly generated files: %v", err)
        return
    }

    for _, entry := range entries {

        exists, isDir, size, err := hubicClient.GetFileInfo("test/test1/" + entry.Name)
        fmt.Printf("%s exists: %t, isDir: %t, size: %d, err: %v\n", "test/test1/" + entry.Name, exists, isDir, size, err)

        err = hubicClient.MoveFile("test/test1/" + entry.Name, "test/test2/" + entry.Name)
        if err != nil {
            t.Errorf("Failed to move %s: %v", entry.Name, err)
            return
        }
    }

    entries, err = hubicClient.ListEntries("test/test2")
    if err != nil {
        t.Errorf("Error list randomly generated files: %v", err)
        return
    }

    for _, entry := range entries {
        readCloser, _, err := hubicClient.DownloadFile("test/test2/" + entry.Name)
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

        err = hubicClient.DeleteFile("test/test2/" + entry.Name)
        if err != nil {
            t.Errorf("Failed to delete the file %s: %v", entry.Name, err)
            return
        }
    }

}
