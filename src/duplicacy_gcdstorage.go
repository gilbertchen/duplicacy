// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

var (
	GCDFileMimeType = "application/octet-stream"
	GCDDirectoryMimeType = "application/vnd.google-apps.folder"
)

type GCDStorage struct {
	StorageBase

	service     *drive.Service
	idCache     map[string]string  // only directories are saved in this cache
	idCacheLock sync.Mutex
	backoffs    []int // desired backoff time in seconds for each thread
	attempts    []int // number of failed attempts since last success for each thread

	createDirectoryLock sync.Mutex
	isConnected         bool
	numberOfThreads     int
	TestMode            bool
}

type GCDConfig struct {
	ClientID     string          `json:"client_id"`
	ClientSecret string          `json:"client_secret"`
	Endpoint     oauth2.Endpoint `json:"end_point"`
	Token        oauth2.Token    `json:"token"`
}

func (storage *GCDStorage) shouldRetry(threadIndex int, err error) (bool, error) {

	const MAX_ATTEMPTS = 15

	maximumBackoff := 64
	if maximumBackoff < storage.numberOfThreads {
		maximumBackoff = storage.numberOfThreads
	}
	retry := false
	message := ""
	if err == nil {
		storage.backoffs[threadIndex] = 1
		storage.attempts[threadIndex] = 0
		return false, nil
	} else if e, ok := err.(*googleapi.Error); ok {
		if 500 <= e.Code && e.Code < 600 {
			// Retry for 5xx response codes.
			message = fmt.Sprintf("HTTP status code %d", e.Code)
			retry = true
		} else if e.Code == 429 {
			// Too many requests{
			message = "HTTP status code 429"
			retry = true
		} else if e.Code == 403 {
			// User Rate Limit Exceeded
			message = e.Message
			retry = true
		} else if e.Code == 401 {
			// Only retry on authorization error when storage has been connected before
			if storage.isConnected {
				message = "Authorization Error"
				retry = true
			}
		}
	} else if e, ok := err.(*url.Error); ok {
		message = e.Error()
		retry = true
	} else if err == io.ErrUnexpectedEOF {
		// Retry on unexpected EOFs and temporary network errors.
		message = "Unexpected EOF"
		retry = true
	} else if err, ok := err.(net.Error); ok {
		message = "Temporary network error"
		retry = err.Temporary()
	}

	if !retry {
		return false, err
	}

	if storage.attempts[threadIndex] >= MAX_ATTEMPTS {
		LOG_INFO("GCD_RETRY", "[%d] Maximum number of retries reached (backoff: %d, attempts: %d)",
			threadIndex, storage.backoffs[threadIndex], storage.attempts[threadIndex])
		storage.backoffs[threadIndex] = 1
		storage.attempts[threadIndex] = 0
		return false, err
	}

	if storage.backoffs[threadIndex] < maximumBackoff {
		storage.backoffs[threadIndex] *= 2
	}
	if storage.backoffs[threadIndex] > maximumBackoff {
		storage.backoffs[threadIndex] = maximumBackoff
	}
	storage.attempts[threadIndex] += 1
	delay := float64(storage.backoffs[threadIndex]) * rand.Float64() * 2
	LOG_DEBUG("GCD_RETRY", "[%d] %s; retrying after %.2f seconds (backoff: %d, attempts: %d)",
		threadIndex, message, delay, storage.backoffs[threadIndex], storage.attempts[threadIndex])
	time.Sleep(time.Duration(delay * float64(time.Second)))

	return true, nil
}

// convertFilePath converts the path for a fossil in the form of 'chunks/id.fsl' to 'fossils/id'.  This is because
// GCD doesn't support file renaming. Instead, it only allows one file to be moved from one directory to another.
// By adding a layer of path conversion we're pretending that we can rename between 'chunks/id' and 'chunks/id.fsl'
func (storage *GCDStorage) convertFilePath(filePath string) string {
	if strings.HasPrefix(filePath, "chunks/") && strings.HasSuffix(filePath, ".fsl") {
		return "fossils/" + filePath[len("chunks/"):len(filePath)-len(".fsl")]
	}
	return filePath
}

func (storage *GCDStorage) getPathID(path string) string {
	storage.idCacheLock.Lock()
	pathID := storage.idCache[path]
	storage.idCacheLock.Unlock()
	return pathID
}

func (storage *GCDStorage) findPathID(path string) (string, bool) {
	storage.idCacheLock.Lock()
	pathID, ok := storage.idCache[path]
	storage.idCacheLock.Unlock()
	return pathID, ok
}

func (storage *GCDStorage) savePathID(path string, pathID string) {
	storage.idCacheLock.Lock()
	storage.idCache[path] = pathID
	storage.idCacheLock.Unlock()
}

func (storage *GCDStorage) deletePathID(path string) {
	storage.idCacheLock.Lock()
	delete(storage.idCache, path)
	storage.idCacheLock.Unlock()
}

func (storage *GCDStorage) listFiles(threadIndex int, parentID string, listFiles bool, listDirectories bool) ([]*drive.File, error) {

	if parentID == "" {
		return nil, fmt.Errorf("No parent ID provided")
	}

	files := []*drive.File{}

	startToken := ""

	query := "'" + parentID + "' in parents and trashed = false "
	if listFiles && !listDirectories {
		query += "and mimeType != 'application/vnd.google-apps.folder'"
	} else if !listFiles && !listDirectories {
		query += "and mimeType = 'application/vnd.google-apps.folder'"
	}

	maxCount := int64(1000)
	if storage.TestMode {
		maxCount = 8
	}

	for {
		var fileList *drive.FileList
		var err error

		for {
			fileList, err = storage.service.Files.List().Q(query).Fields("nextPageToken", "files(name, mimeType, id, size)").PageToken(startToken).PageSize(maxCount).Do()
			if retry, e := storage.shouldRetry(threadIndex, err); e == nil && !retry {
				break
			} else if retry {
				continue
			} else {
				return nil, err
			}
		}

		files = append(files, fileList.Files...)

		startToken = fileList.NextPageToken
		if startToken == "" {
			break
		}
	}

	return files, nil
}

func (storage *GCDStorage) listByName(threadIndex int, parentID string, name string) (string, bool, int64, error) {

	var fileList *drive.FileList
	var err error

	for {
		query := "name = '" + name + "' and '" + parentID + "' in parents and trashed = false "
		fileList, err = storage.service.Files.List().Q(query).Fields("files(name, mimeType, id, size)").Do()

		if retry, e := storage.shouldRetry(threadIndex, err); e == nil && !retry {
			break
		} else if retry {
			continue
		} else {
			return "", false, 0, err
		}
	}

	if len(fileList.Files) == 0 {
		return "", false, 0, nil
	}

	file := fileList.Files[0]

	return file.Id, file.MimeType == GCDDirectoryMimeType, file.Size, nil
}

// getIDFromPath returns the id of the given path.  If 'createDirectories' is true, create the given path and all its
// parent directories if they don't exist.  Note that if 'createDirectories' is false, it may return an empty 'fileID'
// if the file doesn't exist.
func (storage *GCDStorage) getIDFromPath(threadIndex int, filePath string, createDirectories bool) (string, error) {

	if fileID, ok := storage.findPathID(filePath); ok {
		return fileID, nil
	}

	fileID := "root"

	if rootID, ok := storage.findPathID(""); ok {
		fileID = rootID
	}

	names := strings.Split(filePath, "/")
	current := ""
	for i, name := range names {
		// Find the intermediate directory in the cache first.
		current = path.Join(current, name)
		currentID, ok := storage.findPathID(current)
		if ok {
			fileID = currentID
			continue
		}

		// Check if the directory exists.
		var err error
		var isDir bool
		fileID, isDir, _, err = storage.listByName(threadIndex, fileID, name)
		if err != nil {
			return "", err
		}
		if fileID == "" {
			if !createDirectories {
				return "", nil
			}

			// Only one thread can create the directory at a time -- GCD allows multiple directories
			// to have the same name but different ids.
			storage.createDirectoryLock.Lock()
			err = storage.CreateDirectory(threadIndex, current)
			storage.createDirectoryLock.Unlock()

			if err != nil {
				return "", fmt.Errorf("Failed to create directory '%s': %v", current, err)
			}
			currentID, ok = storage.findPathID(current)
			if !ok {
				return "", fmt.Errorf("Directory '%s' created by id not found", current)
			}
			fileID = currentID
			continue
		} else if isDir {
			storage.savePathID(current, fileID)
		}
		if i != len(names) - 1 && !isDir {
			return "", fmt.Errorf("Path '%s' is not a directory", current)
		}
	}
	return fileID, nil
}

// CreateGCDStorage creates a GCD storage object.
func CreateGCDStorage(tokenFile string, storagePath string, threads int) (storage *GCDStorage, err error) {

	description, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	gcdConfig := &GCDConfig{}
	if err := json.Unmarshal(description, gcdConfig); err != nil {
		return nil, err
	}

	oauth2Config := oauth2.Config{
		ClientID:     gcdConfig.ClientID,
		ClientSecret: gcdConfig.ClientSecret,
		Endpoint:     gcdConfig.Endpoint,
	}

	authClient := oauth2Config.Client(context.Background(), &gcdConfig.Token)

	service, err := drive.New(authClient)
	if err != nil {
		return nil, err
	}

	storage = &GCDStorage{
		service:         service,
		numberOfThreads: threads,
		idCache:         make(map[string]string),
		backoffs:        make([]int, threads),
		attempts:        make([]int, threads),
	}

	for i := range storage.backoffs {
		storage.backoffs[i] = 1
		storage.attempts[i] = 0
	}

	storagePathID, err := storage.getIDFromPath(0, storagePath, true)
	if err != nil {
		return nil, err
	}

	// Reset the id cache and start with 'storagePathID' as the root
	storage.idCache = make(map[string]string)
	storage.idCache[""] = storagePathID

	for _, dir := range []string{"chunks", "snapshots", "fossils"} {
		dirID, isDir, _, err := storage.listByName(0, storagePathID, dir)
		if err != nil {
			return nil, err
		}
		if dirID == "" {
			err = storage.CreateDirectory(0, dir)
			if err != nil {
				return nil, err
			}
		} else if !isDir {
			return nil, fmt.Errorf("%s/%s is not a directory", storagePath, dir)
		} else {
			storage.idCache[dir] = dirID
		}
	}

	storage.isConnected = true

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *GCDStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	if dir == "snapshots" {

		files, err := storage.listFiles(threadIndex, storage.getPathID(dir), false, true)
		if err != nil {
			return nil, nil, err
		}

		subDirs := []string{}

		for _, file := range files {
			storage.savePathID("snapshots/" + file.Name, file.Id)
			subDirs = append(subDirs, file.Name + "/")
		}
		return subDirs, nil, nil
	} else if strings.HasPrefix(dir, "snapshots/") {
		pathID, err := storage.getIDFromPath(threadIndex, dir, false)
		if err != nil {
			return nil, nil, err
		}
		if pathID == "" {
			return nil, nil, fmt.Errorf("Path '%s' does not exist", dir)
		}

		entries, err := storage.listFiles(threadIndex, pathID, true, false)
		if err != nil {
			return nil, nil, err
		}

		files := []string{}

		for _, entry := range entries {
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
			entries, err := storage.listFiles(threadIndex, pathID, true, true)
			if err != nil {
				return nil, nil, err
			}
			for _, entry := range entries {
				if entry.MimeType != GCDDirectoryMimeType {
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
					parents = append(parents, parent+ "/" + entry.Name)
					storage.savePathID(parent + "/" + entry.Name, entry.Id)
				}
			}
		}
		return files, sizes, nil
	}

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *GCDStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	filePath = storage.convertFilePath(filePath)
	fileID, err := storage.getIDFromPath(threadIndex, filePath, false)
	if err != nil {
		LOG_TRACE("GCD_STORAGE", "Ignored file deletion error: %v", err)
		return nil
	}

	for {
		err = storage.service.Files.Delete(fileID).Fields("id").Do()
		if retry, err := storage.shouldRetry(threadIndex, err); err == nil && !retry {
			storage.deletePathID(filePath)
			return nil
		} else if retry {
			continue
		} else {
			if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
				LOG_TRACE("GCD_STORAGE", "File %s has disappeared before deletion", filePath)
				return nil
			}
			return err
		}
	}
}

// MoveFile renames the file.
func (storage *GCDStorage) MoveFile(threadIndex int, from string, to string) (err error) {

	from = storage.convertFilePath(from)
	to = storage.convertFilePath(to)

	fileID, err := storage.getIDFromPath(threadIndex, from, false)
	if err != nil {
		return fmt.Errorf("Failed to retrieve the id of '%s': %v", from, err)
	}
	if fileID == "" {
		return fmt.Errorf("The file '%s' to be moved does not exist", from)
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

	for {
		_, err = storage.service.Files.Update(fileID, nil).AddParents(toParentID).RemoveParents(fromParentID).Do()
		if retry, err := storage.shouldRetry(threadIndex, err); err == nil && !retry {
			break
		} else if retry {
			continue
		} else {
			return err
		}
	}

	return nil
}

// createDirectory creates a new directory.
func (storage *GCDStorage) CreateDirectory(threadIndex int, dir string) (err error) {

	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	exist, isDir, _, err := storage.GetFileInfo(threadIndex, dir)
	if err != nil {
		return err
	}

	if exist {
		if !isDir {
			return fmt.Errorf("%s is a file", dir)
		}
		return nil
	}

	parentDir := path.Dir(dir)
	if parentDir == "." {
		parentDir = ""
	}
	parentID := storage.getPathID(parentDir)
	if parentID == "" {
		return fmt.Errorf("Parent directory '%s' does not exist", parentDir)
	}
	name := path.Base(dir)

	var file *drive.File

	for {
		file = &drive.File{
			Name:     name,
			MimeType: GCDDirectoryMimeType,
			Parents:  []string{parentID},
		}

		file, err = storage.service.Files.Create(file).Fields("id").Do()
		if retry, err := storage.shouldRetry(threadIndex, err); err == nil && !retry {
			break
		} else {

			// Check if the directory has already been created by other thread
			if _, ok := storage.findPathID(dir); ok {
				return nil
			}

			if retry {
				continue
			} else {
				return err
			}
		}
	}

	storage.savePathID(dir, file.Id)
	return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *GCDStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	for len(filePath) > 0 && filePath[len(filePath)-1] == '/' {
		filePath = filePath[:len(filePath)-1]
	}
	filePath = storage.convertFilePath(filePath)

	fileID, ok := storage.findPathID(filePath)
	if ok {
		// Only directories are saved in the case so this must be a directory
		return true, true, 0, nil
	}

	dir := path.Dir(filePath)
	if dir == "." {
		dir = ""
	}
	dirID, err := storage.getIDFromPath(threadIndex, dir, false)
	if err != nil {
		return false, false, 0, err
	}
	if dirID == "" {
		return false, false, 0, nil
	}

	fileID, isDir, size, err = storage.listByName(threadIndex, dirID, path.Base(filePath))
	if fileID != "" && isDir {
		storage.savePathID(filePath, fileID)
	}
	return fileID != "", isDir, size, err

}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *GCDStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	// We never download the fossil so there is no need to convert the path
	fileID, err := storage.getIDFromPath(threadIndex, filePath, false)
	if err != nil {
		return err
	}
	if fileID == "" {
		return fmt.Errorf("%s does not exist", filePath)
	}

	var response *http.Response

	for {
		response, err = storage.service.Files.Get(fileID).Download()
		if retry, err := storage.shouldRetry(threadIndex, err); err == nil && !retry {
			break
		} else if retry {
			continue
		} else {
			return err
		}
	}

	defer response.Body.Close()

	_, err = RateLimitedCopy(chunk, response.Body, storage.DownloadRateLimit/storage.numberOfThreads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *GCDStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

	// We never upload a fossil so there is no need to convert the path
	parent := path.Dir(filePath)

	if parent == "." {
		parent = ""
	}

	parentID, err := storage.getIDFromPath(threadIndex, parent, true)
	if err != nil {
		return err
	}

	file := &drive.File{
		Name:     path.Base(filePath),
		MimeType: GCDFileMimeType,
		Parents:  []string{parentID},
	}

	for {
		reader := CreateRateLimitedReader(content, storage.UploadRateLimit/storage.numberOfThreads)
		_, err = storage.service.Files.Create(file).Media(reader).Fields("id").Do()
		if retry, err := storage.shouldRetry(threadIndex, err); err == nil && !retry {
			break
		} else if retry {
			continue
		} else {
			return err
		}
	}

	return err
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *GCDStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *GCDStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *GCDStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *GCDStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *GCDStorage) EnableTestMode() { storage.TestMode = true }
