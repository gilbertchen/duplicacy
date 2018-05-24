// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"path"
	"strings"
	"sync"
	"time"
)

type ACDStorage struct {
	StorageBase

	client          *ACDClient
	idCache         map[string]string
	idCacheLock     *sync.Mutex
	numberOfThreads int
}

// CreateACDStorage creates an ACD storage object.
func CreateACDStorage(tokenFile string, storagePath string, threads int) (storage *ACDStorage, err error) {

	client, err := NewACDClient(tokenFile)
	if err != nil {
		return nil, err
	}

	storage = &ACDStorage{
		client:          client,
		idCache:         make(map[string]string),
		idCacheLock:     &sync.Mutex{},
		numberOfThreads: threads,
	}

	storagePathID, err := storage.getIDFromPath(0, storagePath, false)
	if err != nil {
		return nil, err
	}

	// Set 'storagePath' as the root of the storage and clean up the id cache accordingly
	storage.idCache = make(map[string]string)
	storage.idCache[""] = storagePathID

	for _, dir := range []string{"chunks", "fossils", "snapshots"} {
		dirID, isDir, _, err := client.ListByName(storagePathID, dir)
		if err != nil {
			return nil, err
		}
		if dirID == "" {
			if err != nil {
				return nil, err
			}
		} else if !isDir {
			return nil, fmt.Errorf("%s is not a directory", storagePath+"/"+dir)
		}
		storage.idCache[dir] = dirID
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
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

// convertFilePath converts the path for a fossil in the form of 'chunks/id.fsl' to 'fossils/id'.  This is because
// ACD doesn't support file renaming. Instead, it only allows one file to be moved from one directory to another.
// By adding a layer of path conversion we're pretending that we can rename between 'chunks/id' and 'chunks/id.fsl'
func (storage *ACDStorage) convertFilePath(filePath string) string {
	if strings.HasPrefix(filePath, "chunks/") && strings.HasSuffix(filePath, ".fsl") {
		return "fossils/" + filePath[len("chunks/"):len(filePath)-len(".fsl")]
	}
	return filePath
}

// getIDFromPath returns the id of the given path.  If 'createDirectories' is true, create the given path and all its
// parent directories if they don't exist.  Note that if 'createDirectories' is false, it may return an empty 'fileID'
// if the file doesn't exist.
func (storage *ACDStorage) getIDFromPath(threadIndex int, filePath string, createDirectories bool) (fileID string, err error) {

	if fileID, ok := storage.findPathID(filePath); ok {
		return fileID, nil
	}

	parentID, ok := storage.findPathID("")
	if !ok {
		parentID, _, _, err = storage.client.ListByName("", "")
		if err != nil {
			return "", err
		}
		storage.savePathID("", parentID)
	}

	names := strings.Split(filePath, "/")
	current := ""
	for i, name := range names {

		current = path.Join(current, name)
		fileID, ok := storage.findPathID(current)
		if ok {
			parentID = fileID
			continue
		}
		isDir := false
		fileID, isDir, _, err = storage.client.ListByName(parentID, name)
		if err != nil {
			return "", err
		}
		if fileID == "" {
			if !createDirectories {
				return "", nil
			}
			// Create the current directory
			fileID, err = storage.client.CreateDirectory(parentID, name)
			if err != nil {
				// Check if the directory has been created by another thread
				if e, ok := err.(ACDError); !ok || e.Status != 409 {
					return "", fmt.Errorf("Failed to create directory '%s': %v", current, err)
				}
				// A 409 means the directory may have already created by another thread.  Wait 10 seconds
				// until we seed the directory.
				for i := 0; i < 10; i++ {
					var createErr error
					fileID, isDir, _, createErr = storage.client.ListByName(parentID, name)
					if createErr != nil {
						return "", createErr
					}
					if fileID == "" {
						time.Sleep(time.Second)
					} else {
						break
					}
				}
				if fileID == "" {
					return "", fmt.Errorf("All attempts to create directory '%s' failed: %v", current, err)
				}
			} else {
				isDir = true
			}
		} else {
			storage.savePathID(current, fileID)
		}
		if i != len(names)-1 && !isDir {
			return "", fmt.Errorf("Path '%s' is not a directory", current)
		}
		parentID = fileID
	}

	return parentID, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *ACDStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
	var err error

	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	if dir == "snapshots" {

		entries, err := storage.client.ListEntries(storage.getPathID(dir), false, true)
		if err != nil {
			return nil, nil, err
		}

		subDirs := []string{}

		for _, entry := range entries {
			storage.savePathID(entry.Name, entry.ID)
			subDirs = append(subDirs, entry.Name+"/")
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
			storage.savePathID(dir, pathID)
		}

		entries, err := storage.client.ListEntries(pathID, true, false)
		if err != nil {
			return nil, nil, err
		}

		files := []string{}

		for _, entry := range entries {
			storage.savePathID(dir+"/"+entry.Name, entry.ID)
			files = append(files, entry.Name)
		}
		return files, nil, nil
	} else {
		files := []string{}
		sizes := []int64{}
		parents := []string{"chunks", "fossils"}
		for i := 0; i < len(parents); i++ {
			parent := parents[i]
			pathID, ok := storage.findPathID(parent)
			if !ok {
				continue
			}
			entries, err := storage.client.ListEntries(pathID, true, true)
			if err != nil {
				return nil, nil, err
			}
			for _, entry := range entries {
				if entry.Kind != "FOLDER" {
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
				storage.savePathID(parent+"/"+entry.Name, entry.ID)
			}
		}
		return files, sizes, nil
	}

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *ACDStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	filePath = storage.convertFilePath(filePath)
	fileID, err := storage.getIDFromPath(threadIndex, filePath, false)
	if err != nil {
		return err
	}
	if fileID == "" {
		LOG_TRACE("ACD_STORAGE", "File '%s' to be deleted does not exist", filePath)
		return nil
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

	fromParent := path.Dir(from)
	fromParentID, err := storage.getIDFromPath(threadIndex, fromParent, false)
	if err != nil {
		return fmt.Errorf("Failed to retrieve the id of the parent directory '%s': %v", fromParent, err)
	}
	if fromParentID == "" {
		return fmt.Errorf("The parent directory '%s' does not exist", fromParent)
	}

	toParent := path.Dir(to)
	toParentID, err := storage.getIDFromPath(threadIndex, toParent, true)
	if err != nil {
		return fmt.Errorf("Failed to retrieve the id of the parent directory '%s': %v", toParent, err)
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

	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	parentPath := path.Dir(dir)
	if parentPath == "." {
		parentPath = ""
	}
	parentID, ok := storage.findPathID(parentPath)
	if !ok {
		return fmt.Errorf("Path directory '%s' has unknown id", parentPath)
	}

	name := path.Base(dir)
	dirID, err := storage.client.CreateDirectory(parentID, name)
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

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *ACDStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

	for len(filePath) > 0 && filePath[len(filePath)-1] == '/' {
		filePath = filePath[:len(filePath)-1]
	}

	filePath = storage.convertFilePath(filePath)

	parentPath := path.Dir(filePath)
	if parentPath == "." {
		parentPath = ""
	}
	parentID, err := storage.getIDFromPath(threadIndex, parentPath, false)
	if err != nil {
		return false, false, 0, err
	}
	if parentID == "" {
		return false, false, 0, nil
	}

	name := path.Base(filePath)
	fileID, isDir, size, err := storage.client.ListByName(parentID, name)
	if err != nil {
		return false, false, 0, err
	}
	if fileID == "" {
		return false, false, 0, nil
	}

	storage.savePathID(filePath, fileID)
	return true, isDir, size, nil
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *ACDStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	fileID, err := storage.getIDFromPath(threadIndex, filePath, false)
	if err != nil {
		return err
	}
	if fileID == "" {
		return fmt.Errorf("File path '%s' does not exist", filePath)
	}

	readCloser, _, err := storage.client.DownloadFile(fileID)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/storage.numberOfThreads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *ACDStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	parent := path.Dir(filePath)
	if parent == "." {
		parent = ""
	}

	parentID, err := storage.getIDFromPath(threadIndex, parent, true)
	if err != nil {
		return err
	}
	if parentID == "" {
		return fmt.Errorf("File path '%s' does not exist", parent)
	}

	fileID, err := storage.client.UploadFile(parentID, path.Base(filePath), content, storage.UploadRateLimit/storage.numberOfThreads)
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
func (storage *ACDStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *ACDStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *ACDStorage) IsStrongConsistent() bool { return true }

// If the storage supports fast listing of files names.
func (storage *ACDStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *ACDStorage) EnableTestMode() {}
