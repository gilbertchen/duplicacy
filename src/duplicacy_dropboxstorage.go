// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/gilbertchen/go-dropbox"
)

type DropboxStorage struct {
	StorageBase

	clients        []*dropbox.Files
	minimumNesting int // The minimum level of directories to dive into before searching for the chunk file.
	storageDir     string
}

// CreateDropboxStorage creates a dropbox storage object.
func CreateDropboxStorage(accessToken string, storageDir string, minimumNesting int, threads int) (storage *DropboxStorage, err error) {

	var clients []*dropbox.Files
	for i := 0; i < threads; i++ {
		client := dropbox.NewFiles(dropbox.NewConfig(accessToken))
		clients = append(clients, client)
	}

	if storageDir == "" || storageDir[0] != '/' {
		storageDir = "/" + storageDir
	}

	if len(storageDir) > 1 && storageDir[len(storageDir)-1] == '/' {
		storageDir = storageDir[:len(storageDir)-1]
	}

	storage = &DropboxStorage{
		clients:        clients,
		storageDir:     storageDir,
		minimumNesting: minimumNesting,
	}

	err = storage.CreateDirectory(0, "")
	if err != nil {
		return nil, fmt.Errorf("Can't create storage directory: %v", err)
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{1}, 1)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *DropboxStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {

	if dir != "" && dir[0] != '/' {
		dir = "/" + dir
	}

	if len(dir) > 1 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	input := &dropbox.ListFolderInput{
		Path:             storage.storageDir + dir,
		Recursive:        false,
		IncludeMediaInfo: false,
		IncludeDeleted:   false,
	}

	output, err := storage.clients[threadIndex].ListFolder(input)

	for {

		if err != nil {
			return nil, nil, err
		}

		for _, entry := range output.Entries {
			name := entry.Name
			if entry.Tag == "folder" {
				name += "/"
			}
			files = append(files, name)
			sizes = append(sizes, int64(entry.Size))
		}

		if output.HasMore {
			output, err = storage.clients[threadIndex].ListFolderContinue(
				&dropbox.ListFolderContinueInput{Cursor: output.Cursor})

		} else {
			break
		}

	}

	return files, sizes, nil
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *DropboxStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	if filePath != "" && filePath[0] != '/' {
		filePath = "/" + filePath
	}

	input := &dropbox.DeleteInput{
		Path: storage.storageDir + filePath,
	}
	_, err = storage.clients[threadIndex].Delete(input)
	if err != nil {
		if e, ok := err.(*dropbox.Error); ok && strings.HasPrefix(e.Summary, "path_lookup/not_found/") {
			return nil
		}
	}

	return err
}

// MoveFile renames the file.
func (storage *DropboxStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	if from != "" && from[0] != '/' {
		from = "/" + from
	}
	if to != "" && to[0] != '/' {
		to = "/" + to
	}
	input := &dropbox.MoveInput{
		FromPath: storage.storageDir + from,
		ToPath:   storage.storageDir + to,
	}
	_, err = storage.clients[threadIndex].Move(input)
	return err
}

// CreateDirectory creates a new directory.
func (storage *DropboxStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	if dir != "" && dir[0] != '/' {
		dir = "/" + dir
	}

	if len(dir) > 1 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	input := &dropbox.CreateFolderInput{
		Path: storage.storageDir + dir,
	}

	_, err = storage.clients[threadIndex].CreateFolder(input)
	if err != nil {
		if e, ok := err.(*dropbox.Error); ok && strings.HasPrefix(e.Summary, "path/conflict/") {
			return nil
		}
	}
	return err
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *DropboxStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

	if filePath != "" && filePath[0] != '/' {
		filePath = "/" + filePath
	}

	input := &dropbox.GetMetadataInput{
		Path:             storage.storageDir + filePath,
		IncludeMediaInfo: false,
	}

	output, err := storage.clients[threadIndex].GetMetadata(input)
	if err != nil {
		if e, ok := err.(*dropbox.Error); ok && strings.HasPrefix(e.Summary, "path/not_found/") {
			return false, false, 0, nil
		} else {
			return false, false, 0, err
		}
	}

	return true, output.Tag == "folder", int64(output.Size), nil
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *DropboxStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {

	if filePath != "" && filePath[0] != '/' {
		filePath = "/" + filePath
	}

	input := &dropbox.DownloadInput{
		Path: storage.storageDir + filePath,
	}

	output, err := storage.clients[threadIndex].Download(input)
	if err != nil {
		return err
	}

	defer output.Body.Close()
	defer ioutil.ReadAll(output.Body)

	_, err = RateLimitedCopy(chunk, output.Body, storage.DownloadRateLimit/len(storage.clients))
	return err

}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *DropboxStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	if filePath != "" && filePath[0] != '/' {
		filePath = "/" + filePath
	}

	input := &dropbox.UploadInput{
		Path:       storage.storageDir + filePath,
		Mode:       dropbox.WriteModeOverwrite,
		AutoRename: false,
		Mute:       true,
		Reader:     CreateRateLimitedReader(content, storage.UploadRateLimit/len(storage.clients)),
	}

	_, err = storage.clients[threadIndex].Upload(input)
	return err
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *DropboxStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *DropboxStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *DropboxStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *DropboxStorage) IsFastListing() bool { return false }

// Enable the test mode.
func (storage *DropboxStorage) EnableTestMode() {}
