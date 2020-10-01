// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"syscall"
	"time"
)

// FileStorage is a local on-disk file storage implementing the Storage interface.
type FileStorage struct {
	StorageBase

	isCacheNeeded   bool // Network storages require caching
	storageDir      string
	numberOfThreads int
}

// CreateFileStorage creates a file storage.
func CreateFileStorage(storageDir string, isCacheNeeded bool, threads int) (storage *FileStorage, err error) {

	var stat os.FileInfo

	stat, err = os.Stat(storageDir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(storageDir, 0744)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		if !stat.IsDir() {
			return nil, fmt.Errorf("The storage path %s is a file", storageDir)
		}
	}

	for storageDir[len(storageDir)-1] == '/' {
		storageDir = storageDir[:len(storageDir)-1]
	}

	storage = &FileStorage{
		storageDir:      storageDir,
		isCacheNeeded:   isCacheNeeded,
		numberOfThreads: threads,
	}

	// Random number fo generating the temporary chunk file suffix.
	rand.Seed(time.Now().UnixNano())

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{2, 3}, 2)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively).
func (storage *FileStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {

	fullPath := path.Join(storage.storageDir, dir)

	list, err := ioutil.ReadDir(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}

	for _, f := range list {
		name := f.Name()
		if (f.IsDir() || f.Mode() & os.ModeSymlink != 0) && name[len(name)-1] != '/' {
			name += "/"
		}
		files = append(files, name)
		sizes = append(sizes, f.Size())
	}

	return files, sizes, nil
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *FileStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	err = os.Remove(path.Join(storage.storageDir, filePath))
	if err == nil || os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

// MoveFile renames the file.
func (storage *FileStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	return os.Rename(path.Join(storage.storageDir, from), path.Join(storage.storageDir, to))
}

// CreateDirectory creates a new directory.
func (storage *FileStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	err = os.Mkdir(path.Join(storage.storageDir, dir), 0744)
	if err != nil && os.IsExist(err) {
		return nil
	} else {
		return err
	}
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *FileStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	stat, err := os.Stat(path.Join(storage.storageDir, filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return false, false, 0, nil
		} else {
			return false, false, 0, err
		}
	}

	return true, stat.IsDir(), stat.Size(), nil
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *FileStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {

	file, err := os.Open(path.Join(storage.storageDir, filePath))

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
func (storage *FileStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

	fullPath := path.Join(storage.storageDir, filePath)

	if len(strings.Split(filePath, "/")) > 2 {
		dir := path.Dir(fullPath)
		// Use Lstat() instead of Stat() since 1) Stat() doesn't work for deduplicated disks on Windows and 2) there isn't
		// really a need to follow the link if filePath is a link.
		stat, err := os.Lstat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			err = os.MkdirAll(dir, 0744)
			if err != nil {
				return err
			}
		} else {
			if !stat.IsDir() && stat.Mode() & os.ModeSymlink == 0 {
				return fmt.Errorf("The path %s is not a directory or symlink", dir)
			}
		}
	}

	letters := "abcdefghijklmnopqrstuvwxyz"
	suffix := make([]byte, 8)
	for i := range suffix {
		suffix[i] = letters[rand.Intn(len(letters))]
	}

	temporaryFile := fullPath + "." + string(suffix) + ".tmp"

	file, err := os.OpenFile(temporaryFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	reader := CreateRateLimitedReader(content, storage.UploadRateLimit/storage.numberOfThreads)
	_, err = io.Copy(file, reader)
	if err != nil {
		file.Close()
		return err
	}

	if err = file.Sync(); err != nil {
		pathErr, ok := err.(*os.PathError)
		isNotSupported := ok && pathErr.Op == "sync" && pathErr.Err == syscall.ENOTSUP
		if !isNotSupported {
			_ = file.Close()
			return err
		}
	}

	err = file.Close()
	if err != nil {
		return err
	}

	err = os.Rename(temporaryFile, fullPath)
	if err != nil {

		if _, e := os.Stat(fullPath); e == nil {
			os.Remove(temporaryFile)
			return nil
		} else {
			return err
		}
	}

	return nil
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *FileStorage) IsCacheNeeded() bool { return storage.isCacheNeeded }

// If the 'MoveFile' method is implemented.
func (storage *FileStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *FileStorage) IsStrongConsistent() bool { return true }

// If the storage supports fast listing of files names.
func (storage *FileStorage) IsFastListing() bool { return false }

// Enable the test mode.
func (storage *FileStorage) EnableTestMode() {}
