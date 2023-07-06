// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"io"
	"os"
	"fmt"
	"net"
	"path"
	"time"
	"strings"
	"syscall"
	"math/rand"

	"github.com/hirochachacha/go-smb2"
)

// SambaStorage is a local on-disk file storage implementing the Storage interface.
type SambaStorage struct {
	StorageBase

	share           *smb2.Share
	storageDir      string
	numberOfThreads int
}

// CreateSambaStorage creates a file storage.
func CreateSambaStorage(server string, port int, username string, password string, shareName string, storageDir string, threads int) (storage *SambaStorage, err error) {

	connection, err := net.Dial("tcp", fmt.Sprintf("%s:%d", server, port))
	if err != nil {
		return nil, err
	}

	dialer := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     username,
			Password: password,
		},
	}

	client, err := dialer.Dial(connection)
	if err != nil {
		return nil, err
	}

	share, err := client.Mount(shareName)
	if err != nil {
		return nil, err
	}

	// Random number fo generating the temporary chunk file suffix.
	rand.Seed(time.Now().UnixNano())

	storage = &SambaStorage{
		share:           share,
		numberOfThreads: threads,
	}

	exist, isDir, _, err := storage.GetFileInfo(0, storageDir)
	if err != nil {
		return nil, fmt.Errorf("Failed to check the storage path %s: %v", storageDir, err)
	}

	if !exist {
		return nil, fmt.Errorf("The storage path %s does not exist", storageDir)
	}

	if !isDir {
		return nil, fmt.Errorf("The storage path %s is not a directory", storageDir)
	}

	storage.storageDir = storageDir
	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{2, 3}, 2)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively).
func (storage *SambaStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {

	fullPath := path.Join(storage.storageDir, dir)

	list, err := storage.share.ReadDir(fullPath)
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
func (storage *SambaStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	err = storage.share.Remove(path.Join(storage.storageDir, filePath))
	if err == nil || os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

// MoveFile renames the file.
func (storage *SambaStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	return storage.share.Rename(path.Join(storage.storageDir, from), path.Join(storage.storageDir, to))
}

// CreateDirectory creates a new directory.
func (storage *SambaStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	fmt.Printf("Creating directory %s\n", dir)
	err = storage.share.Mkdir(path.Join(storage.storageDir, dir), 0744)
	if err != nil && os.IsExist(err) {
		return nil
	} else {
		return err
	}
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *SambaStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	stat, err := storage.share.Stat(path.Join(storage.storageDir, filePath))
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
func (storage *SambaStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {

	file, err := storage.share.Open(path.Join(storage.storageDir, filePath))

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
func (storage *SambaStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

	fullPath := path.Join(storage.storageDir, filePath)

	if len(strings.Split(filePath, "/")) > 2 {
		dir := path.Dir(fullPath)
		stat, err := storage.share.Stat(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			err = storage.share.MkdirAll(dir, 0744)
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

	file, err := storage.share.Create(temporaryFile)
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

	err = storage.share.Rename(temporaryFile, fullPath)
	if err != nil {

		if _, e := storage.share.Stat(fullPath); e == nil {
			storage.share.Remove(temporaryFile)
			return nil
		} else {
			return err
		}
	}

	return nil
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *SambaStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *SambaStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *SambaStorage) IsStrongConsistent() bool { return true }

// If the storage supports fast listing of files names.
func (storage *SambaStorage) IsFastListing() bool { return false }

// Enable the test mode.
func (storage *SambaStorage) EnableTestMode() {}
