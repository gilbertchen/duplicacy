// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "fmt"
    "path"
    "strings"
    "sync"
)

type ACDStorage struct {
    RateLimitedStorage

    client *ACDClient
    idCache map[string]string
    idCacheLock    *sync.Mutex
    numberOfThreads int
}

// CreateACDStorage creates an ACD storage object.
func CreateACDStorage(tokenFile string, storagePath string, threads int) (storage *ACDStorage, err error) {

    client, err := NewACDClient(tokenFile)
    if err != nil {
        return nil, err
    }

    storage = &ACDStorage {
        client: client,
        idCache: make(map[string]string),
        idCacheLock: &sync.Mutex{},
        numberOfThreads: threads,
    }

    storagePathID, _, _, err := storage.getIDFromPath(0, storagePath)
    if err != nil {
        return nil, err
    }

    storage.idCache[""] = storagePathID

    for _, dir := range []string { "chunks", "fossils", "snapshots" } {
        dirID, isDir, _, err := client.ListByName(storagePathID, dir)
        if err != nil {
            return nil, err
        }
        if dirID == "" {
            dirID, err = client.CreateDirectory(storagePathID, dir)
            if err != nil {
                return nil, err
            }
        } else if !isDir {
            return nil, fmt.Errorf("%s/%s is not a directory", storagePath + "/" + dir)
        }
        storage.idCache[dir] = dirID
    }

    return storage, nil

}

func (storage *ACDStorage) getPathID(path string) string {
    storage.idCacheLock.Lock()
    pathID := storage.idCache[path]
    storage.idCacheLock.Unlock()
    return pathID
}

func (storage *ACDStorage) findPathID(path string) (string, bool) {
    storage.idCacheLock.Lock()
    pathID, ok := storage.idCache[path]
    storage.idCacheLock.Unlock()
    return pathID, ok
}

func (storage *ACDStorage) savePathID(path string, pathID string) {
    storage.idCacheLock.Lock()
    storage.idCache[path] = pathID
    storage.idCacheLock.Unlock()
}

func (storage *ACDStorage) deletePathID(path string) {
    storage.idCacheLock.Lock()
    delete(storage.idCache, path)
    storage.idCacheLock.Unlock()
}


func (storage *ACDStorage) convertFilePath(filePath string) (string) {
    if strings.HasPrefix(filePath, "chunks/") && strings.HasSuffix(filePath, ".fsl") {
        return "fossils/" + filePath[len("chunks/"):len(filePath) - len(".fsl")]
    }
    return filePath
}

func (storage *ACDStorage) getIDFromPath(threadIndex int, path string) (fileID string, isDir bool, size int64, err error) {

    parentID, ok := storage.findPathID("")
    if !ok {
        parentID, isDir, size, err = storage.client.ListByName("", "")
        if err != nil {
            return "", false, 0, err
        }
    }

    names := strings.Split(path, "/")
    for i, name := range names {
        parentID, isDir, _, err = storage.client.ListByName(parentID, name)
        if err != nil {
            return "", false, 0, err
        }
        if parentID == "" {
            if i == len(names) - 1 {
                return "", false, 0, nil
            } else {
                return "", false, 0, fmt.Errorf("File path '%s' does not exist", path)
            }
        }
        if i != len(names) - 1 && !isDir {
            return "", false, 0, fmt.Errorf("Invalid path %s", path)
        }
    }

    return parentID, isDir, size, err
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *ACDStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
    var err error

    for len(dir) > 0 && dir[len(dir) - 1] == '/' {
        dir = dir[:len(dir) - 1]
    }

    if dir == "snapshots" {

        entries, err := storage.client.ListEntries(storage.getPathID(dir), false)
        if err != nil {
            return nil, nil, err
        }

        subDirs := []string{}

        for _, entry := range entries {
            storage.savePathID(entry.Name, entry.ID)
            subDirs = append(subDirs, entry.Name + "/")
        }
        return subDirs, nil, nil
    } else if strings.HasPrefix(dir, "snapshots/") {
        name := dir[len("snapshots/"):]
        pathID, ok := storage.findPathID(dir)
        if !ok {
            pathID, _, _, err = storage.client.ListByName(storage.getPathID("snapshots"), name)
            if err != nil {
                return nil, nil, err
            }
            if pathID == "" {
                return nil, nil, nil
            }
        }

        entries, err := storage.client.ListEntries(pathID, true)
        if err != nil {
            return nil, nil, err
        }

        files := []string{}

        for _, entry := range entries {
            storage.savePathID(dir + "/" + entry.Name, entry.ID)
            files = append(files, entry.Name)
        }
        return files, nil, nil
    } else {
        files := []string{}
        sizes := []int64{}
        for _, parent := range []string {"chunks", "fossils" } {
            entries, err := storage.client.ListEntries(storage.getPathID(parent), true)
            if err != nil {
                return nil, nil, err
            }


            for _, entry := range entries {
                name := entry.Name
                if parent == "fossils" {
                    name += ".fsl"
                }

                storage.savePathID(parent + "/" + entry.Name, entry.ID)
                files = append(files, name)
                sizes = append(sizes, entry.Size)
            }
        }
        return files, sizes, nil
    }

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *ACDStorage) DeleteFile(threadIndex int, filePath string) (err error) {
    filePath = storage.convertFilePath(filePath)
    fileID, ok := storage.findPathID(filePath)
    if !ok {
        fileID, _, _, err = storage.getIDFromPath(threadIndex, filePath)
        if err != nil {
            return err
        }
        if fileID == "" {
            LOG_TRACE("ACD_STORAGE", "File %s has disappeared before deletion", filePath)
            return nil
        }
        storage.savePathID(filePath, fileID)
    }

    err = storage.client.DeleteFile(fileID)
    if e, ok := err.(ACDError); ok && e.Status == 409 {
        LOG_DEBUG("ACD_DELETE", "Ignore 409 conflict error")
        return nil
    }
    return err
}

// MoveFile renames the file.
func (storage *ACDStorage) MoveFile(threadIndex int, from string, to string) (err error) {
    from = storage.convertFilePath(from)
    to = storage.convertFilePath(to)

    fileID, ok := storage.findPathID(from)
    if !ok {
        return fmt.Errorf("Attempting to rename file %s with unknown id", from)
    }

    fromParentID := storage.getPathID("chunks")
    toParentID := storage.getPathID("fossils")

    if strings.HasPrefix(from, "fossils") {
        fromParentID, toParentID = toParentID, fromParentID
    }

    err = storage.client.MoveFile(fileID, fromParentID, toParentID)
    if err != nil {
        if e, ok := err.(ACDError); ok && e.Status == 409 {
            LOG_DEBUG("ACD_MOVE", "Ignore 409 conflict error")
        } else {
            return err
        }
    }

    storage.savePathID(to, storage.getPathID(from))
    storage.deletePathID(from)

    return nil
}

// CreateDirectory creates a new directory.
func (storage *ACDStorage) CreateDirectory(threadIndex int, dir string) (err error) {

    for len(dir) > 0 && dir[len(dir) - 1] == '/' {
        dir = dir[:len(dir) - 1]
    }

    if dir == "chunks" || dir == "snapshots" {
        return nil
    }

    if strings.HasPrefix(dir, "snapshots/") {
        name := dir[len("snapshots/"):]
        dirID, err := storage.client.CreateDirectory(storage.getPathID("snapshots"), name)
        if err != nil {
            if e, ok := err.(ACDError); ok && e.Status == 409 {
                return nil
            } else {
                return err
            }
        }
        storage.savePathID(dir, dirID)
        return nil

    }

    return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *ACDStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

    for len(filePath) > 0 && filePath[len(filePath) - 1] == '/' {
        filePath = filePath[:len(filePath) - 1]
    }

    filePath = storage.convertFilePath(filePath)
    fileID := ""
    fileID, isDir, size, err = storage.getIDFromPath(threadIndex, filePath)
    if err != nil {
        return false, false, 0, err
    }
    if fileID == "" {
        return false, false, 0, nil
    }

    return true, isDir, size, nil
}

// FindChunk finds the chunk with the specified id.  If 'isFossil' is true, it will search for chunk files with
// the suffix '.fsl'.
func (storage *ACDStorage) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {
    parentID := ""
    filePath = "chunks/" + chunkID
    realPath := filePath
    if isFossil {
        parentID = storage.getPathID("fossils")
        filePath += ".fsl"
        realPath = "fossils/" + chunkID + ".fsl"
    } else {
        parentID = storage.getPathID("chunks")
    }

    fileID := ""
    fileID, _, size, err = storage.client.ListByName(parentID, chunkID)
    if fileID != "" {
        storage.savePathID(realPath, fileID)
    }
    return filePath, fileID != "", size, err
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *ACDStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
    fileID, ok := storage.findPathID(filePath)
    if !ok {
        fileID, _, _, err = storage.getIDFromPath(threadIndex, filePath)
        if err != nil {
            return err
        }
        if fileID == "" {
            return fmt.Errorf("File path '%s' does not exist", filePath)
        }
        storage.savePathID(filePath, fileID)
    }

    readCloser, _, err := storage.client.DownloadFile(fileID)
    if err != nil {
        return err
    }

    defer readCloser.Close()

    _, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit / storage.numberOfThreads)
    return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *ACDStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
    parent := path.Dir(filePath)

    if parent == "." {
        parent = ""
    }

    parentID, ok := storage.findPathID(parent)

    if !ok {
        parentID, _, _, err = storage.getIDFromPath(threadIndex, parent)
        if err != nil {
            return err
        }
        if parentID == "" {
            return fmt.Errorf("File path '%s' does not exist", parent)
        }
        storage.savePathID(parent, parentID)
    }

    fileID, err := storage.client.UploadFile(parentID, path.Base(filePath), content, storage.UploadRateLimit / storage.numberOfThreads)
    if err == nil {
        storage.savePathID(filePath, fileID)
        return nil
    }

    if e, ok := err.(ACDError); ok && e.Status == 409 {
        LOG_TRACE("ACD_UPLOAD", "File %s already exists", filePath)
        return nil
    } else {
        return err
    }
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *ACDStorage) IsCacheNeeded() (bool) { return true }

// If the 'MoveFile' method is implemented.
func (storage *ACDStorage) IsMoveFileImplemented() (bool) { return true }

// If the storage can guarantee strong consistency.
func (storage *ACDStorage) IsStrongConsistent() (bool) { return true }

// If the storage supports fast listing of files names.
func (storage *ACDStorage) IsFastListing() (bool) { return true }

// Enable the test mode.
func (storage *ACDStorage) EnableTestMode() {}
