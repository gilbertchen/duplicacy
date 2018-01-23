// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"strings"
)

type B2Storage struct {
	StorageBase

	clients []*B2Client
}

// CreateB2Storage creates a B2 storage object.
func CreateB2Storage(accountID string, applicationKey string, bucket string, threads int) (storage *B2Storage, err error) {

	var clients []*B2Client

	for i := 0; i < threads; i++ {
		client := NewB2Client(accountID, applicationKey)

		err = client.AuthorizeAccount()
		if err != nil {
			return nil, err
		}

		err = client.FindBucket(bucket)
		if err != nil {
			return nil, err
		}

		clients = append(clients, client)
	}

	storage = &B2Storage{
		clients: clients,
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *B2Storage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {
	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}
	length := len(dir) + 1

	includeVersions := false
	if dir == "chunks" {
		includeVersions = true
	}

	entries, err := storage.clients[threadIndex].ListFileNames(dir, false, includeVersions)
	if err != nil {
		return nil, nil, err
	}

	if dir == "snapshots" {

		subDirs := make(map[string]bool)

		for _, entry := range entries {
			name := entry.FileName[length:]
			subDir := strings.Split(name, "/")[0]
			subDirs[subDir+"/"] = true
		}

		for subDir, _ := range subDirs {
			files = append(files, subDir)
		}
	} else if dir == "chunks" {
		lastFile := ""
		for _, entry := range entries {
			if entry.FileName == lastFile {
				continue
			}
			lastFile = entry.FileName
			if entry.Action == "hide" {
				files = append(files, entry.FileName[length:]+".fsl")
			} else {
				files = append(files, entry.FileName[length:])
			}
			sizes = append(sizes, entry.Size)
		}
	} else {
		for _, entry := range entries {
			files = append(files, entry.FileName[length:])
		}
	}

	return files, sizes, nil
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *B2Storage) DeleteFile(threadIndex int, filePath string) (err error) {

	if strings.HasSuffix(filePath, ".fsl") {
		filePath = filePath[:len(filePath)-len(".fsl")]
		entries, err := storage.clients[threadIndex].ListFileNames(filePath, true, true)
		if err != nil {
			return err
		}

		toBeDeleted := false

		for _, entry := range entries {
			if entry.FileName != filePath || (!toBeDeleted && entry.Action != "hide") {
				continue
			}

			toBeDeleted = true

			err = storage.clients[threadIndex].DeleteFile(filePath, entry.FileID)
			if err != nil {
				return err
			}
		}

		return nil

	} else {
		entries, err := storage.clients[threadIndex].ListFileNames(filePath, true, false)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			return nil
		}
		return storage.clients[threadIndex].DeleteFile(filePath, entries[0].FileID)
	}
}

// MoveFile renames the file.
func (storage *B2Storage) MoveFile(threadIndex int, from string, to string) (err error) {

	filePath := ""

	if strings.HasSuffix(from, ".fsl") {
		filePath = to
		if from != to+".fsl" {
			filePath = ""
		}
	} else if strings.HasSuffix(to, ".fsl") {
		filePath = from
		if to != from+".fsl" {
			filePath = ""
		}
	}

	if filePath == "" {
		LOG_FATAL("STORAGE_MOVE", "Moving file '%s' to '%s' is not supported", from, to)
		return nil
	}

	if filePath == from {
		_, err = storage.clients[threadIndex].HideFile(from)
		return err
	} else {
		entries, err := storage.clients[threadIndex].ListFileNames(filePath, true, true)
		if err != nil {
			return err
		}
		if len(entries) == 0 || entries[0].FileName != filePath || entries[0].Action != "hide" {
			return nil
		}

		return storage.clients[threadIndex].DeleteFile(filePath, entries[0].FileID)
	}
}

// CreateDirectory creates a new directory.
func (storage *B2Storage) CreateDirectory(threadIndex int, dir string) (err error) {
	return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *B2Storage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	isFossil := false
	if strings.HasSuffix(filePath, ".fsl") {
		isFossil = true
		filePath = filePath[:len(filePath)-len(".fsl")]
	}

	entries, err := storage.clients[threadIndex].ListFileNames(filePath, true, isFossil)
	if err != nil {
		return false, false, 0, err
	}

	if len(entries) == 0 || entries[0].FileName != filePath {
		return false, false, 0, nil
	}

	if isFossil {
		if entries[0].Action == "hide" {
			return true, false, entries[0].Size, nil
		} else {
			return false, false, 0, nil
		}
	}
	return true, false, entries[0].Size, nil
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *B2Storage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {

	filePath = strings.Replace(filePath, " ", "%20", -1)
	readCloser, _, err := storage.clients[threadIndex].DownloadFile(filePath)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/len(storage.clients))
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *B2Storage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	filePath = strings.Replace(filePath, " ", "%20", -1)
	return storage.clients[threadIndex].UploadFile(filePath, content, storage.UploadRateLimit/len(storage.clients))
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *B2Storage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *B2Storage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *B2Storage) IsStrongConsistent() bool { return true }

// If the storage supports fast listing of files names.
func (storage *B2Storage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *B2Storage) EnableTestMode() {
	for _, client := range storage.clients {
		client.TestMode = true
	}
}
