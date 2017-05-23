// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "fmt"
    "strings"

    "github.com/gilbertchen/azure-sdk-for-go/storage"
)

type AzureStorage struct {
    RateLimitedStorage

    clients []*storage.BlobStorageClient
    container string
}

func CreateAzureStorage(accountName string, accountKey string,
                        container string, threads int) (azureStorage *AzureStorage, err error) {

    var clients []*storage.BlobStorageClient
    for i := 0; i < threads; i++ {

        client, err := storage.NewBasicClient(accountName, accountKey)

        if err != nil {
            return nil, err
        }

        blobService := client.GetBlobService()
        clients = append(clients, &blobService)
    }

    exist, err := clients[0].ContainerExists(container)
    if err != nil {
        return nil, err
    }

    if !exist {
        return nil, fmt.Errorf("container %s does not exist", container)
    }

    azureStorage = &AzureStorage {
        clients: clients,
        container: container,
    }

    return
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (azureStorage *AzureStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {

    type ListBlobsParameters struct {
        Prefix     string
        Delimiter  string
        Marker     string
        Include    string
        MaxResults uint
        Timeout    uint
    }

    if len(dir) > 0 && dir[len(dir) - 1] != '/' {
        dir += "/"
    }
    dirLength := len(dir)

    parameters := storage.ListBlobsParameters {
        Prefix: dir,
        Delimiter: "",
    }

    subDirs := make(map[string]bool)

    for {

        results, err := azureStorage.clients[threadIndex].ListBlobs(azureStorage.container, parameters)
        if err != nil {
            return nil, nil, err
        }

        if dir == "snapshots/" {
            for _, blob := range results.Blobs {
                 name := strings.Split(blob.Name[dirLength:], "/")[0]
                 subDirs[name + "/"] = true
            }
        } else {
            for _, blob := range results.Blobs {
                files = append(files, blob.Name[dirLength:])
                sizes = append(sizes, blob.Properties.ContentLength)
            }
        }

        if results.NextMarker == "" {
            break
        }

        parameters.Marker = results.NextMarker
    }

    if dir == "snapshots/" {

        for subDir, _ := range subDirs {
            files = append(files, subDir)
        }

    }

    return files, sizes, nil

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *AzureStorage) DeleteFile(threadIndex int, filePath string) (err error) {
    _, err = storage.clients[threadIndex].DeleteBlobIfExists(storage.container, filePath)
    return err
}

// MoveFile renames the file.
func (storage *AzureStorage) MoveFile(threadIndex int, from string, to string) (err error) {
    source := storage.clients[threadIndex].GetBlobURL(storage.container, from)
    err = storage.clients[threadIndex].CopyBlob(storage.container, to, source)
    if err != nil {
        return err
    }
    return storage.DeleteFile(threadIndex, from)
}

// CreateDirectory creates a new directory.
func (storage *AzureStorage) CreateDirectory(threadIndex int, dir string) (err error) {
    return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *AzureStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
    properties, err := storage.clients[threadIndex].GetBlobProperties(storage.container, filePath)
    if err != nil {
        if strings.Contains(err.Error(), "404") {
            return false, false, 0, nil
        } else {
            return false, false, 0, err
        }
    }

    return true, false, properties.ContentLength, nil
}

// FindChunk finds the chunk with the specified id.  If 'isFossil' is true, it will search for chunk files with
// the suffix '.fsl'.
func (storage *AzureStorage) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {
    filePath = "chunks/" + chunkID
    if isFossil {
        filePath += ".fsl"
    }

    exist, _, size, err = storage.GetFileInfo(threadIndex, filePath)

    if err != nil {
        return "", false, 0, err
    } else {
        return filePath, exist, size, err
    }
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *AzureStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
    readCloser, err := storage.clients[threadIndex].GetBlob(storage.container, filePath)
    if err != nil {
        return err
    }

    defer readCloser.Close()

    _, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit / len(storage.clients))
    return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *AzureStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
   reader := CreateRateLimitedReader(content, storage.UploadRateLimit / len(storage.clients))
   return storage.clients[threadIndex].CreateBlockBlobFromReader(storage.container, filePath, uint64(len(content)), reader, nil)

}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *AzureStorage) IsCacheNeeded() (bool) { return true }

// If the 'MoveFile' method is implemented.
func (storage *AzureStorage)  IsMoveFileImplemented() (bool) { return true }

// If the storage can guarantee strong consistency.
func (storage *AzureStorage) IsStrongConsistent() (bool) { return true }

// If the storage supports fast listing of files names.
func (storage *AzureStorage) IsFastListing() (bool) { return true }

// Enable the test mode.
func (storage *AzureStorage) EnableTestMode() {}
