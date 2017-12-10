// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"strings"
)

type HubicStorage struct {
	StorageBase

	client          *HubicClient
	storageDir      string
	numberOfThreads int
}

// CreateHubicStorage creates an Hubic storage object.
func CreateHubicStorage(tokenFile string, storagePath string, threads int) (storage *HubicStorage, err error) {

	for len(storagePath) > 0 && storagePath[len(storagePath)-1] == '/' {
		storagePath = storagePath[:len(storagePath)-1]
	}

	client, err := NewHubicClient(tokenFile)
	if err != nil {
		return nil, err
	}

	exists, isDir, _, err := client.GetFileInfo(storagePath)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("Path '%s' doesn't exist", storagePath)
	}

	if !isDir {
		return nil, fmt.Errorf("Path '%s' is not a directory", storagePath)
	}

	storage = &HubicStorage{
		client:          client,
		storageDir:      storagePath,
		numberOfThreads: threads,
	}

	for _, path := range []string{"chunks", "snapshots"} {
		dir := storagePath + "/" + path
		exists, isDir, _, err := client.GetFileInfo(dir)
		if err != nil {
			return nil, err
		}
		if !exists {
			err = client.CreateDirectory(storagePath + "/" + path)
			if err != nil {
				return nil, err
			}
		} else if !isDir {
			return nil, fmt.Errorf("%s is not a directory", dir)
		}
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *HubicStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	if dir == "snapshots" {
		entries, err := storage.client.ListEntries(storage.storageDir + "/" + dir)
		if err != nil {
			return nil, nil, err
		}

		subDirs := []string{}
		for _, entry := range entries {
			if entry.Type == "application/directory" {
				subDirs = append(subDirs, entry.Name+"/")
			}
		}
		return subDirs, nil, nil
	} else if strings.HasPrefix(dir, "snapshots/") {
		entries, err := storage.client.ListEntries(storage.storageDir + "/" + dir)
		if err != nil {
			return nil, nil, err
		}

		files := []string{}

		for _, entry := range entries {
			if entry.Type == "application/directory" {
				continue
			}
			files = append(files, entry.Name)
		}
		return files, nil, nil
	} else {
		files := []string{}
		sizes := []int64{}
		entries, err := storage.client.ListEntries(storage.storageDir + "/" + dir)
		if err != nil {
			return nil, nil, err
		}

		for _, entry := range entries {
			if entry.Type == "application/directory" {
				files = append(files,  entry.Name + "/")
				sizes = append(sizes, 0)
			} else {
				files = append(files, entry.Name)
				sizes = append(sizes, entry.Size)
			}
		}
		return files, sizes, nil
	}

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *HubicStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	err = storage.client.DeleteFile(storage.storageDir + "/" + filePath)
	if e, ok := err.(HubicError); ok && e.Status == 404 {
		LOG_DEBUG("HUBIC_DELETE", "Ignore 404 error")
		return nil
	}
	return err
}

// MoveFile renames the file.
func (storage *HubicStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	fromPath := storage.storageDir + "/" + from
	toPath := storage.storageDir + "/" + to

	return storage.client.MoveFile(fromPath, toPath)
}

// CreateDirectory creates a new directory.
func (storage *HubicStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	return storage.client.CreateDirectory(storage.storageDir + "/" + dir)
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *HubicStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

	for len(filePath) > 0 && filePath[len(filePath)-1] == '/' {
		filePath = filePath[:len(filePath)-1]
	}
	return storage.client.GetFileInfo(storage.storageDir + "/" + filePath)
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *HubicStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	readCloser, _, err := storage.client.DownloadFile(storage.storageDir + "/" + filePath)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/storage.numberOfThreads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *HubicStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	return storage.client.UploadFile(storage.storageDir+"/"+filePath, content, storage.UploadRateLimit/storage.numberOfThreads)
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *HubicStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *HubicStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *HubicStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *HubicStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *HubicStorage) EnableTestMode() {
	storage.client.TestMode = true
}
