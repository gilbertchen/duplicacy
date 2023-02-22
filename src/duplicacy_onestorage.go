// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"path"
	"strings"
	"sync"
)

type OneDriveStorage struct {
	StorageBase

	client          *OneDriveClient
	storageDir      string
	numberOfThreads int
}

// CreateOneDriveStorage creates an OneDrive storage object.
func CreateOneDriveStorage(
	tokenFile string, 
	isBusiness bool, 
	storagePath string, 
	threads int, 
	max_batch_requests int, 
	client_id string, 
	client_secret string, 
	drive_id string,
) (storage *OneDriveStorage, err error) {

	for len(storagePath) > 0 && storagePath[len(storagePath)-1] == '/' {
		storagePath = storagePath[:len(storagePath)-1]
	}

	client, err := NewOneDriveClient(
		tokenFile, 
		isBusiness, 
		max_batch_requests, 
		client_id, 
		client_secret, 
		drive_id,
	)
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
		client:          client,
		storageDir:      storagePath,
		numberOfThreads: threads,
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

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
	return storage, nil

}

func (storage *OneDriveStorage) convertFilePath(filePath string) string {
	if strings.HasPrefix(filePath, "chunks/") && strings.HasSuffix(filePath, ".fsl") {
		return "fossils/" + filePath[len("chunks/"):len(filePath)-len(".fsl")]
	}
	return filePath
}


// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *OneDriveStorage) ListFilesNotThreaded(threadIndex int, dir string) ([]string, []int64, error) {

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
	} else if strings.HasPrefix(dir, "snapshots/") || strings.HasPrefix(dir, "benchmark") {
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
		parents := []string{"chunks", "fossils"}
		for i := 0; i < len(parents); i++ {
			parent := parents[i]
			entries, err := storage.client.ListEntries(storage.storageDir + "/" + parent)
			if err != nil {
				return nil, nil, err
			}

			for _, entry := range entries {
				if len(entry.Folder) == 0 {
					name := entry.Name
					if strings.HasPrefix(parent, "fossils") {
						name = parent + "/" + name + ".fsl"
						name = name[len("fossils/"):]
					} else {
						name = parent + "/" + name
						name = name[len("chunks/"):]
					}
					files = append(files, name)
					sizes = append(sizes, entry.Size)
				} else {
					parents = append(parents, parent+"/"+entry.Name)
				}
			}
		}
		return files, sizes, nil
	}

}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *OneDriveStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {

	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	if dir == "snapshots" {
		// Not threaded
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
	} else if strings.HasPrefix(dir, "snapshots/") || strings.HasPrefix(dir, "benchmark") {
		// Not threaded
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
		// Batched and threaded
		lock := sync.Mutex {}
		allFiles := []string{}
		allSizes := []int64{}

		errorChannel := make(chan error)
		requestChannel := make(chan OneDriveListReqItem)
		activeWorkers := 0

		requests := []OneDriveListReqItem{
			{Path:"chunks", 	URL:""},
			{Path:"fossils", 	URL:""},
		}

		maxRequestsPerThread := 1
		if storage.client.MaxBatchReqs > 1 {
			// OneDrive Business works through Graph API 
			// which supports batch requests (20 is the max)
			maxRequestsPerThread = storage.client.MaxBatchReqs
		}

		for len(requests) > 0 || activeWorkers > 0 {
			if len(requests) > 0 && activeWorkers < storage.numberOfThreads {
				n_batchReqs := len(requests)
				if n_batchReqs > maxRequestsPerThread {
					n_batchReqs = maxRequestsPerThread
				}
				// Dequeue n_batch_reqs from the request queue
				batchReqs := requests[:n_batchReqs]
				requests = requests[n_batchReqs:]
				activeWorkers++

				go func(batchReqs []OneDriveListReqItem) {
					// Will do non-batching if disabled / not supported
					entriesPerReq, newReqs, err := storage.client.ListEntriesBatch(storage.storageDir, batchReqs)
					if err != nil {
						errorChannel <- err
						return
					}

					// send paging requests first
					for _, pageReq := range newReqs {
						requestChannel <- pageReq 
					}

					files := []string {}
					sizes := []int64 {}

					for i, entries := range entriesPerReq {
						LOG_DEBUG("ONE_STORAGE", "Listing %s; %d items returned", batchReqs[i].Path, len(entries.Entries))
						for _, entry := range entries.Entries {
							if len(entry.Folder) == 0 {
								name := entry.Name
								if strings.HasPrefix(batchReqs[i].Path, "fossils") {
									name = batchReqs[i].Path + "/" + name + ".fsl"
									name = name[len("fossils/"):]
								} else {
									name = batchReqs[i].Path + "/" + name
									name = name[len("chunks/"):]
								}
								files = append(files, name)
								sizes = append(sizes, entry.Size)
							} else {
								recurseDirRequest := OneDriveListReqItem{
									Path: batchReqs[i].Path + "/" + entry.Name,
									URL: "",
								}
								requestChannel <- recurseDirRequest
							}
						}
					}
					lock.Lock()
					allFiles = append(allFiles, files...)
					allSizes = append(allSizes, sizes...)
					lock.Unlock()
					requestChannel <- OneDriveListReqItem{Path:"", URL:""}
				} (batchReqs)
			}

			if activeWorkers > 0 {
				select {
				case err := <- errorChannel:
					return nil, nil, err
				case request := <- requestChannel:
					if request.Path == "" {
						activeWorkers--
					} else {
						requests = append(requests, request)
					}
				}
			}
		}

		return allFiles, allSizes, nil
	}
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *OneDriveStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	filePath = storage.convertFilePath(filePath)

	err = storage.client.DeleteFile(storage.storageDir + "/" + filePath)
	if e, ok := err.(OneDriveError); ok && e.Status == 404 {
		LOG_DEBUG("ONEDRIVE_DELETE", "Ignore 404 error")
		return nil
	}
	return err
}

// MoveFile renames the file.
func (storage *OneDriveStorage) MoveFile(threadIndex int, from string, to string) (err error) {

	fromPath := storage.storageDir + "/" + storage.convertFilePath(from)
	toPath := storage.storageDir + "/" + storage.convertFilePath(to)

	err = storage.client.MoveFile(fromPath, path.Dir(toPath))
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

	filePath = storage.convertFilePath(filePath)

	fileID, isDir, size, err := storage.client.GetFileInfo(storage.storageDir + "/" + filePath)
	return fileID != "", isDir, size, err
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *OneDriveStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	readCloser, _, err := storage.client.DownloadFile(storage.storageDir + "/" + filePath)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/storage.numberOfThreads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *OneDriveStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	err = storage.client.UploadFile(storage.storageDir+"/"+filePath, content, storage.UploadRateLimit/storage.numberOfThreads)

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
