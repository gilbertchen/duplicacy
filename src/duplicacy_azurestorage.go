// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"strings"

	"github.com/azure/azure-sdk-for-go/storage"
)

type AzureStorage struct {
	StorageBase

	containers []*storage.Container
}

func CreateAzureStorage(accountName string, accountKey string,
	containerName string, threads int) (azureStorage *AzureStorage, err error) {

	var containers []*storage.Container
	for i := 0; i < threads; i++ {

		client, err := storage.NewBasicClient(accountName, accountKey)

		if err != nil {
			return nil, err
		}

		blobService := client.GetBlobService()
		container := blobService.GetContainerReference(containerName)
		containers = append(containers, container)
	}

	exist, err := containers[0].Exists()
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, fmt.Errorf("container %s does not exist", containerName)
	}

	azureStorage = &AzureStorage{
		containers: containers,
	}

	azureStorage.DerivedStorage = azureStorage
	azureStorage.SetDefaultNestingLevels([]int{0}, 0)
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

	if len(dir) > 0 && dir[len(dir)-1] != '/' {
		dir += "/"
	}
	dirLength := len(dir)

	parameters := storage.ListBlobsParameters{
		Prefix:    dir,
		Delimiter: "",
	}

	subDirs := make(map[string]bool)

	for {

		results, err := azureStorage.containers[threadIndex].ListBlobs(parameters)
		if err != nil {
			return nil, nil, err
		}

		if dir == "snapshots/" {
			for _, blob := range results.Blobs {
				name := strings.Split(blob.Name[dirLength:], "/")[0]
				subDirs[name+"/"] = true
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
	_, err = storage.containers[threadIndex].GetBlobReference(filePath).DeleteIfExists(nil)
	return err
}

// MoveFile renames the file.
func (storage *AzureStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	source := storage.containers[threadIndex].GetBlobReference(from)
	destination := storage.containers[threadIndex].GetBlobReference(to)
	err = destination.Copy(source.GetURL(), nil)
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
	blob := storage.containers[threadIndex].GetBlobReference(filePath)
	err = blob.GetProperties(nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return false, false, 0, nil
		} else {
			return false, false, 0, err
		}
	}

	return true, false, blob.Properties.ContentLength, nil
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *AzureStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	readCloser, err := storage.containers[threadIndex].GetBlobReference(filePath).Get(nil)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/len(storage.containers))
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *AzureStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	reader := CreateRateLimitedReader(content, storage.UploadRateLimit/len(storage.containers))
	blob := storage.containers[threadIndex].GetBlobReference(filePath)
	return blob.CreateBlockBlobFromReader(reader, nil)

}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *AzureStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *AzureStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *AzureStorage) IsStrongConsistent() bool { return true }

// If the storage supports fast listing of files names.
func (storage *AzureStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *AzureStorage) EnableTestMode() {}
