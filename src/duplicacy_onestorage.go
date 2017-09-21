// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"path"
	"strings"
)

type OneDriveStorage struct {
	RateLimitedStorage

	client         *OneDriveClient
	storageDir     string
	numberOfThread int
}

// CreateOneDriveStorage creates an OneDrive storage object.
func CreateOneDriveStorage(tokenFile string, storagePath string, threads int) (storage *OneDriveStorage, err error) {

	for len(storagePath) > 0 && storagePath[len(storagePath)-1] == '/' {
		storagePath = storagePath[:len(storagePath)-1]
	}

	client, err := NewOneDriveClient(tokenFile)
	if err != nil {
		return nil, err
	}

	fileID, isDir, _, err := client.GetFileInfo(storagePath)
	if err != nil {
		return nil, err
	}

	if fileID == "" {
		return nil, fmt.Errorf("Path '%s' doesn't exist", storagePath)
	}

	if !isDir {
		return nil, fmt.Errorf("Path '%s' is not a directory", storagePath)
	}

	storage = &OneDriveStorage{
		client:         client,
		storageDir:     storagePath,
		numberOfThread: threads,
	}

	for _, path := range []string{"chunks", "fossils", "snapshots"} {
		dir := storagePath + "/" + path
		dirID, isDir, _, err := client.GetFileInfo(dir)
		if err != nil {
			return nil, err
		}
		if dirID == "" {
			err = client.CreateDirectory(storagePath, path)
			if err != nil {
				return nil, err
			}
		} else if !isDir {
			return nil, fmt.Errorf("%s is not a directory", dir)
		}
	}

	return storage, nil

}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *OneDriveStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
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
			if len(entry.Folder) > 0 {
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
			if len(entry.Folder) == 0 {
				files = append(files, entry.Name)
			}
		}
		return files, nil, nil
	} else {
		files := []string{}
		sizes := []int64{}
		for _, parent := range []string{"chunks", "fossils"} {
			entries, err := storage.client.ListEntries(storage.storageDir + "/" + parent)
			if err != nil {
				return nil, nil, err
			}

			for _, entry := range entries {
				name := entry.Name
				if parent == "fossils" {
					name += ".fsl"
				}
				files = append(files, name)
				sizes = append(sizes, entry.Size)
			}
		}
		return files, sizes, nil
	}

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *OneDriveStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	if strings.HasSuffix(filePath, ".fsl") && strings.HasPrefix(filePath, "chunks/") {
		filePath = "fossils/" + filePath[len("chunks/"):len(filePath)-len(".fsl")]
	}

	err = storage.client.DeleteFile(storage.storageDir + "/" + filePath)
	if e, ok := err.(OneDriveError); ok && e.Status == 404 {
		LOG_DEBUG("ONEDRIVE_DELETE", "Ignore 404 error")
		return nil
	}
	return err
}

// MoveFile renames the file.
func (storage *OneDriveStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	fromPath := storage.storageDir + "/" + from
	toParent := storage.storageDir + "/fossils"
	if strings.HasSuffix(from, ".fsl") {
		fromPath = storage.storageDir + "/fossils/" + from[len("chunks/"):len(from)-len(".fsl")]
		toParent = storage.storageDir + "/chunks"
	}

	err = storage.client.MoveFile(fromPath, toParent)
	if err != nil {
		if e, ok := err.(OneDriveError); ok && e.Status == 409 {
			LOG_DEBUG("ONEDRIVE_MOVE", "Ignore 409 conflict error")
		} else {
			return err
		}
	}
	return nil
}

// CreateDirectory creates a new directory.
func (storage *OneDriveStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	parent := path.Dir(dir)

	if parent == "." {
		return storage.client.CreateDirectory(storage.storageDir, dir)
	} else {
		return storage.client.CreateDirectory(storage.storageDir+"/"+parent, path.Base(dir))
	}
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *OneDriveStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

	for len(filePath) > 0 && filePath[len(filePath)-1] == '/' {
		filePath = filePath[:len(filePath)-1]
	}
	fileID, isDir, size, err := storage.client.GetFileInfo(storage.storageDir + "/" + filePath)
	return fileID != "", isDir, size, err
}

// FindChunk finds the chunk with the specified id.  If 'isFossil' is true, it will search for chunk files with
// the suffix '.fsl'.
func (storage *OneDriveStorage) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {
	filePath = "chunks/" + chunkID
	realPath := storage.storageDir + "/" + filePath
	if isFossil {
		filePath += ".fsl"
		realPath = storage.storageDir + "/fossils/" + chunkID
	}

	fileID, _, size, err := storage.client.GetFileInfo(realPath)
	return filePath, fileID != "", size, err
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *OneDriveStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	readCloser, _, err := storage.client.DownloadFile(storage.storageDir + "/" + filePath)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/storage.numberOfThread)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *OneDriveStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	err = storage.client.UploadFile(storage.storageDir+"/"+filePath, content, storage.UploadRateLimit/storage.numberOfThread)

	if e, ok := err.(OneDriveError); ok && e.Status == 409 {
		LOG_TRACE("ONEDRIVE_UPLOAD", "File %s already exists", filePath)
		return nil
	} else {
		return err
	}
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *OneDriveStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *OneDriveStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *OneDriveStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *OneDriveStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *OneDriveStorage) EnableTestMode() {
	storage.client.TestMode = true
}
