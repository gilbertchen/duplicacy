// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
    "fmt"
    "io"
    "context"

    "storj.io/uplink"
)

// StorjStorage is a storage backend for Storj.
type StorjStorage struct {
    StorageBase

    project         *uplink.Project
    bucket          string
    storageDir      string
    numberOfThreads int
}

// CreateStorjStorage creates a Storj storage.
func CreateStorjStorage(satellite string, apiKey string, passphrase string,
                        bucket string, storageDir string, threads int) (storage *StorjStorage, err error) {

    ctx := context.Background()
    access, err := uplink.RequestAccessWithPassphrase(ctx, satellite, apiKey, passphrase)
    if err != nil {
        return nil, fmt.Errorf("cannot request the access grant: %v", err)
    }

    project, err := uplink.OpenProject(ctx, access)
    if err != nil {
        return nil, fmt.Errorf("cannot open the project: %v", err)
    }

    _, err = project.StatBucket(ctx, bucket)
    if err != nil {
        return nil, fmt.Errorf("cannot found the bucket: %v", err)
    }

    if storageDir != "" && storageDir[len(storageDir) - 1] != '/' {
        storageDir += "/"
    }

    storage = &StorjStorage {
        project: project,
        bucket: bucket,
        storageDir: storageDir,
        numberOfThreads: threads,
    }

    storage.DerivedStorage = storage
    storage.SetDefaultNestingLevels([]int{2, 3}, 2)
    return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively).
func (storage *StorjStorage) ListFiles(threadIndex int, dir string) (
                                         files []string, sizes []int64, err error) {

    fullPath := storage.storageDir + dir
    if fullPath != "" && fullPath[len(fullPath) - 1] != '/' {
        fullPath += "/"
    }

    options := uplink.ListObjectsOptions {
        Prefix: fullPath,
        System: true, // request SystemMetadata which includes ContentLength 
    }
    objects := storage.project.ListObjects(context.Background(), storage.bucket, &options)
    for objects.Next() {
        if objects.Err() != nil {
            return nil, nil, objects.Err()
        }
        item := objects.Item()
        name := item.Key[len(fullPath):]
        size := item.System.ContentLength
        if item.IsPrefix {
            if name != "" && name[len(name) - 1] != '/' {
                name += "/"
                size = 0
            }
        }
        files = append(files, name)
        sizes = append(sizes, size)
    }

    return files, sizes, nil
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *StorjStorage) DeleteFile(threadIndex int, filePath string) (err error) {

    _, err = storage.project.DeleteObject(context.Background(), storage.bucket,
                                          storage.storageDir + filePath)
    return err
}

// MoveFile renames the file.
func (storage *StorjStorage) MoveFile(threadIndex int, from string, to string) (err error) {
    err = storage.project.MoveObject(context.Background(), storage.bucket,
                                     storage.storageDir + from, storage.bucket, storage.storageDir + to, nil)

    return err
}

// CreateDirectory creates a new directory.
func (storage *StorjStorage) CreateDirectory(threadIndex int, dir string) (err error) {
    return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *StorjStorage) GetFileInfo(threadIndex int, filePath string) (
                                         exist bool, isDir bool, size int64, err error) {
    info, err := storage.project.StatObject(context.Background(), storage.bucket,
                                            storage.storageDir + filePath)

    if info == nil {
        return false, false, 0, nil
    } else if err != nil {
        return false, false, 0, err
    } else {
        return true, info.IsPrefix, info.System.ContentLength, nil
    }
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *StorjStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
    file, err := storage.project.DownloadObject(context.Background(), storage.bucket,
                                                storage.storageDir + filePath, nil)
    if err != nil {
        return err
    }
    defer file.Close()
    
    if _, err = RateLimitedCopy(chunk, file, storage.DownloadRateLimit/storage.numberOfThreads); err != nil {
        return err
    }

    return nil    
}

// UploadFile writes 'content' to the file at 'filePath'
func (storage *StorjStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

    file, err := storage.project.UploadObject(context.Background(), storage.bucket,
                                              storage.storageDir + filePath, nil)
    if err != nil {
        return err
    }
                                
    reader := CreateRateLimitedReader(content, storage.UploadRateLimit/storage.numberOfThreads)
    _, err = io.Copy(file, reader)
    if err != nil {
        return err
    }

    err = file.Commit()
    if err != nil {
        return err
    }

    return nil
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *StorjStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *StorjStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *StorjStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *StorjStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *StorjStorage) EnableTestMode() {}