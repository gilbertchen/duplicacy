// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "os"
    "io"
    "path"
    "testing"
    "math/rand"
    "encoding/hex"
    "time"
    "crypto/sha256"
    crypto_rand "crypto/rand"

    "runtime/debug"
)

func createRandomFile(path string, maxSize int) {
    file, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0644)
    if err != nil {
        LOG_ERROR("RANDOM_FILE", "Can't open %s for writing: %v", path, err)
        return
    }

    defer file.Close()

    size := maxSize / 2 + rand.Int() % (maxSize / 2)

    buffer := make([]byte, 32 * 1024)
    for size > 0 {
        bytes := size
        if bytes > cap(buffer) {
            bytes = cap(buffer)
        }
        crypto_rand.Read(buffer[:bytes])
        bytes, err = file.Write(buffer[:bytes])
        if err != nil {
            LOG_ERROR("RANDOM_FILE", "Failed to write to %s: %v", path, err)
            return
        }
        size -= bytes
    }
}

func modifyFile(path string, portion float32) {

    stat, err := os.Stat(path)
    if err != nil {
        LOG_ERROR("MODIFY_FILE", "Can't stat the file %s: %v", path, err)
        return
    }

    modifiedTime := stat.ModTime()

    file, err := os.OpenFile(path, os.O_WRONLY, 0644)
    if err != nil {
        LOG_ERROR("MODIFY_FILE", "Can't open %s for writing: %v", path, err)
        return
    }

    defer func() {
        if file != nil {
            file.Close()
        }
    } ()

    size, err := file.Seek(0, 2)
    if err != nil {
        LOG_ERROR("MODIFY_FILE", "Can't seek to the end of the file %s: %v", path, err)
        return
    }

    length := int (float32(size) * portion)
    start := rand.Int() % (int(size) - length)

    _, err = file.Seek(int64(start), 0)
    if err != nil {
        LOG_ERROR("MODIFY_FILE", "Can't seek to the offset %d: %v", start, err)
        return
    }

    buffer := make([]byte, length)
    crypto_rand.Read(buffer)

    _, err = file.Write(buffer)
    if err != nil {
        LOG_ERROR("MODIFY_FILE", "Failed to write to %s: %v", path, err)
        return
    }

    file.Close()
    file = nil

    // Add 2 seconds to the modified time for the changes to be detectable in quick mode.
    modifiedTime = modifiedTime.Add(time.Second * 2)
    err = os.Chtimes(path, modifiedTime, modifiedTime)

    if err != nil {
        LOG_ERROR("MODIFY_FILE", "Failed to change the modification time of %s: %v", path, err)
        return
    }
}

func truncateFile(path string) {
    file, err := os.OpenFile(path, os.O_WRONLY, 0644)
    if err != nil {
        LOG_ERROR("TRUNCATE_FILE", "Can't open %s for writing: %v", path, err)
        return
    }

    defer file.Close()

    oldSize, err := file.Seek(0, 2)
    if err != nil {
        LOG_ERROR("TRUNCATE_FILE", "Can't seek to the end of the file %s: %v", path, err)
        return
    }

    newSize := rand.Int63() % oldSize

    err = file.Truncate(newSize)
    if err != nil {
        LOG_ERROR("TRUNCATE_FILE", "Can't truncate the file %s to size %d: %v", path, newSize, err)
        return
    }
}

func getFileHash(path string) (hash string) {

    file, err := os.Open(path)
      if err != nil {
        LOG_ERROR("FILE_HASH", "Can't open %s for reading: %v", path, err)
        return ""
      }

    defer file.Close()

    hasher := sha256.New()
    _, err = io.Copy(hasher, file)
      if err != nil {
        LOG_ERROR("FILE_HASH", "Can't read file %s: %v", path, err)
        return ""
      }

      return hex.EncodeToString(hasher.Sum(nil))
}

func TestBackupManager(t *testing.T) {

    rand.Seed(time.Now().UnixNano())
    setTestingT(t)
    SetLoggingLevel(INFO)

    defer func() {
        if r := recover(); r != nil {
            switch e := r.(type) {
            case Exception:
                t.Errorf("%s %s", e.LogID, e.Message)
                debug.PrintStack()
            default:
                t.Errorf("%v", e)
                debug.PrintStack()
            }
        }
    } ()

    testDir := path.Join(os.TempDir(), "duplicacy_test")
    os.RemoveAll(testDir)
    os.MkdirAll(testDir, 0700)

    os.Mkdir(testDir + "/repository1", 0700)
    os.Mkdir(testDir + "/repository1/dir1", 0700)

    maxFileSize := 1000000
    //maxFileSize := 200000

    createRandomFile(testDir + "/repository1/file1", maxFileSize)
    createRandomFile(testDir + "/repository1/file2", maxFileSize)
    createRandomFile(testDir + "/repository1/dir1/file3", maxFileSize)

    threads := 1

    storage, err := loadStorage(testDir + "/storage", threads)
    if err != nil {
        t.Errorf("Failed to create storage: %v", err)
        return
    }

    delay := 0
    if _, ok := storage.(*ACDStorage); ok {
        delay = 1
    }
    if _, ok := storage.(*OneDriveStorage); ok {
        delay = 5 
    }

    password := "duplicacy"

    cleanStorage(storage)

    time.Sleep(time.Duration(delay) * time.Second)
    if testFixedChunkSize {
        if !ConfigStorage(storage, 100, 64 * 1024, 64 * 1024, 64 * 1024, password, nil) {
            t.Errorf("Failed to initialize the storage")
        }
    } else {
        if !ConfigStorage(storage, 100, 64 * 1024, 256 * 1024, 16 * 1024, password, nil) {
            t.Errorf("Failed to initialize the storage")
        }
    }


    time.Sleep(time.Duration(delay) * time.Second)

    backupManager := CreateBackupManager("host1", storage, testDir, password)
    backupManager.SetupSnapshotCache(testDir + "/repository1", "default")

    backupManager.Backup(testDir + "/repository1", /*quickMode=*/true, threads, "first", false, false)
    time.Sleep(time.Duration(delay) * time.Second)
    backupManager.Restore(testDir + "/repository2", threads, /*inPlace=*/false, /*quickMode=*/false, threads, /*overwrite=*/true,
                          /*deleteMode=*/false, /*showStatistics=*/false, /*patterns=*/nil)

    for _, f := range []string{ "file1", "file2", "dir1/file3" } {
        if _, err := os.Stat(testDir + "/repository2/" + f); os.IsNotExist(err) {
            t.Errorf("File %s does not exist", f)
            continue
        }

        hash1 := getFileHash(testDir + "/repository1/" + f)
        hash2 := getFileHash(testDir + "/repository2/" + f)
        if hash1 != hash2 {
            t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
        }
    }

    modifyFile(testDir + "/repository1/file1", 0.1)
    modifyFile(testDir + "/repository1/file2", 0.2)
    modifyFile(testDir + "/repository1/dir1/file3", 0.3)

    backupManager.Backup(testDir + "/repository1", /*quickMode=*/true, threads, "second", false, false)
    time.Sleep(time.Duration(delay) * time.Second)
    backupManager.Restore(testDir + "/repository2", 2, /*inPlace=*/true, /*quickMode=*/true, threads, /*overwrite=*/true,
                          /*deleteMode=*/false, /*showStatistics=*/false, /*patterns=*/nil)

    for _, f := range []string{ "file1", "file2", "dir1/file3" } {
        hash1 := getFileHash(testDir + "/repository1/" + f)
        hash2 := getFileHash(testDir + "/repository2/" + f)
        if hash1 != hash2 {
            t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
        }
    }

    truncateFile(testDir + "/repository1/file2")
    backupManager.Backup(testDir + "/repository1", /*quickMode=*/false, threads, "third", false, false)
    time.Sleep(time.Duration(delay) * time.Second)
    backupManager.Restore(testDir + "/repository2", 3, /*inPlace=*/true, /*quickMode=*/false, threads, /*overwrite=*/true,
                          /*deleteMode=*/false, /*showStatistics=*/false, /*patterns=*/nil)

    for _, f := range []string{ "file1", "file2", "dir1/file3" } {
        hash1 := getFileHash(testDir + "/repository1/" + f)
        hash2 := getFileHash(testDir + "/repository2/" + f)
        if hash1 != hash2 {
            t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
        }
    }

    // Remove file2 and dir1/file3 and restore them from revision 3
    os.Remove(testDir + "/repository1/file2")
    os.Remove(testDir + "/repository1/dir1/file3")
    backupManager.Restore(testDir + "/repository1", 3, /*inPlace=*/true, /*quickMode=*/false, threads, /*overwrite=*/true,
                          /*deleteMode=*/false, /*showStatistics=*/false, /*patterns=*/[]string{"+file2", "+dir1/file3", "-*"})

    for _, f := range []string{ "file1", "file2", "dir1/file3" } {
        hash1 := getFileHash(testDir + "/repository1/" + f)
        hash2 := getFileHash(testDir + "/repository2/" + f)
        if hash1 != hash2 {
            t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
        }
    }

    /*buf := make([]byte, 1<<16)
    runtime.Stack(buf, true)
    fmt.Printf("%s", buf)*/
}
