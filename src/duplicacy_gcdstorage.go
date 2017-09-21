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
	"runtime"
)

type GCDStorage struct {
	RateLimitedStorage

    service *drive.Service
    idCache map[string]string
    idCacheLock *sync.Mutex
    backoffs []float64
	backoffsRetries []int

	isConnected     bool
	numberOfThreads int
	TestMode        bool
}

type GCDConfig struct {
	ClientID     string          `json:"client_id"`
	ClientSecret string          `json:"client_secret"`
	Endpoint     oauth2.Endpoint `json:"end_point"`
	Token        oauth2.Token    `json:"token"`
}

func (storage *GCDStorage) shouldRetry(threadIndex int, err error) (bool, error) {
	const LIMIT_BACKOFF_TIME = 64
	const MAX_NUMBER_OF_RETRIES = 15
	minimumSleepRatio := 0.1
	maximumSleepRatio := 0.2
	minimumSleep := float64(storage.numberOfThreads) * minimumSleepRatio
	maximumSleep := float64(storage.numberOfThreads) * maximumSleepRatio
	rand.Seed(time.Now().UnixNano()) // unsure if this is needed

	retry := false
	message := ""
	if err == nil {
		storage.backoffs[threadIndex] = computeInitialBackoff(minimumSleep, maximumSleep)
		storage.backoffsRetries[threadIndex] = 0
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
			message = e.Message // "User Rate Limit Exceeded"
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

	if !retry || storage.backoffsRetries[threadIndex] >= MAX_NUMBER_OF_RETRIES {
		LOG_INFO("GCD_RETRY", "Thread: %03d. Maximum number of retries reached. Backoff time: %.2f. Number of retries: %d", threadIndex, storage.backoffs[threadIndex], storage.backoffsRetries[threadIndex])
		storage.backoffs[threadIndex] = computeInitialBackoff(minimumSleep, maximumSleep)
		storage.backoffsRetries[threadIndex] = 0
		return false, err
	}

    if storage.backoffs[threadIndex] < LIMIT_BACKOFF_TIME {
		storage.backoffs[threadIndex] *= 2.0
	} else {
		storage.backoffs[threadIndex] = LIMIT_BACKOFF_TIME
		storage.backoffsRetries[threadIndex] += 1
	}
	delay := storage.backoffs[threadIndex] * rand.Float64() + storage.backoffs[threadIndex]*rand.Float64()
    if storage.backoffs[threadIndex] >= LIMIT_BACKOFF_TIME {
		callerChain:=findCallerChain()
		LOG_INFO("GCD_RETRY", "Thread: %3d. Message:%s. Retrying after %6.2f seconds. Current backoff: %6.2f. Number of retries: %2d. caller chain: %s", threadIndex, message, delay, storage.backoffs[threadIndex], storage.backoffsRetries[threadIndex], callerChain)
	}
    time.Sleep(time.Duration(delay * float64(time.Second)))

    return true, nil
}

func findCallerChain() string {
	callerStack := ""
	pc := make([]uintptr, 15)
	n := runtime.Callers(1, pc)
	frames := runtime.CallersFrames(pc[:n])

	for {
		frame, more := frames.Next()
		if strings.Contains(frame.File, "runtime/") {
			break
		}
		callerStack += "->" + frame.Function
		if !more {
			break
		}
	}
	return callerStack
}

/*
  logic for said calculus is here: https://stackoverflow.com/questions/1527803/generating-random-whole-numbers-in-javascript-in-a-specific-range
  	chose 0.1*thread number as a minimum sleep time
	  and 0.2*thread number as a maximum sleep time
  for the first sleep of the first backoff of the threads.
  This would mean that both when the program is started, and when multiple threads retry, google won't be ddosed :^)
*/
func computeInitialBackoff(minimumSleep float64, maximumSleep float64) float64 {
	return rand.Float64()*(maximumSleep-minimumSleep+1) + minimumSleep
}

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

func (storage *GCDStorage) listFiles(threadIndex int, parentID string, listFiles bool) ([]*drive.File, error) {

	if parentID == "" {
		return nil, fmt.Errorf("No parent ID provided")
	}

	files := []*drive.File{}

	startToken := ""

	query := "'" + parentID + "' in parents and "
	if listFiles {
		query += "mimeType != 'application/vnd.google-apps.folder'"
	} else {
		query += "mimeType = 'application/vnd.google-apps.folder'"
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
		query := "name = '" + name + "' and '" + parentID + "' in parents"
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

	return file.Id, file.MimeType == "application/vnd.google-apps.folder", file.Size, nil
}

func (storage *GCDStorage) getIDFromPath(threadIndex int, path string) (string, error) {

	fileID := "root"

	if rootID, ok := storage.findPathID(""); ok {
		fileID = rootID
	}

	names := strings.Split(path, "/")
	current := ""
	for i, name := range names {

		if len(current) == 0 {
			current = name
		} else {
			current = current + "/" + name
		}

		currentID, ok := storage.findPathID(current)
		if ok {
			fileID = currentID
			continue
		}

		var err error
		var isDir bool
		fileID, isDir, _, err = storage.listByName(threadIndex, fileID, name)
		if err != nil {
			return "", err
		}
		if fileID == "" {
			return "", fmt.Errorf("Path %s doesn't exist", path)
		}
		if i != len(names)-1 && !isDir {
			return "", fmt.Errorf("Invalid path %s", path)
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

	config := oauth2.Config{
		ClientID:     gcdConfig.ClientID,
		ClientSecret: gcdConfig.ClientSecret,
		Endpoint:     gcdConfig.Endpoint,
	}

	authClient := config.Client(context.Background(), &gcdConfig.Token)

	service, err := drive.New(authClient)
	if err != nil {
		return nil, err
	}

    storage = &GCDStorage {
        service: service,
        numberOfThreads: threads,
        idCache: make(map[string]string),
        idCacheLock: &sync.Mutex{},
        backoffs: make([]float64, threads),
		backoffsRetries:make([]int, threads),
    }

    for b := range storage.backoffs {
		storage.backoffs[b] = 0.1 * float64(storage.numberOfThreads) 			// at the first error, we should still sleep some amount
	}

	storagePathID, err := storage.getIDFromPath(0, storagePath)
	if err != nil {
		return nil, err
	}

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
			return nil, fmt.Errorf("%s/%s is not a directory", storagePath+"/"+dir)
		} else {
			storage.idCache[dir] = dirID
		}
	}

	storage.isConnected = true

	return storage, nil

}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *GCDStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	if dir == "snapshots" {

		files, err := storage.listFiles(threadIndex, storage.getPathID(dir), false)
		if err != nil {
			return nil, nil, err
		}

		subDirs := []string{}

		for _, file := range files {
			storage.savePathID("snapshots/"+file.Name, file.Id)
			subDirs = append(subDirs, file.Name+"/")
		}
		return subDirs, nil, nil
	} else if strings.HasPrefix(dir, "snapshots/") {
		pathID, err := storage.getIDFromPath(threadIndex, dir)
		if err != nil {
			return nil, nil, err
		}

		entries, err := storage.listFiles(threadIndex, pathID, true)
		if err != nil {
			return nil, nil, err
		}

		files := []string{}

		for _, entry := range entries {
			storage.savePathID(dir+"/"+entry.Name, entry.Id)
			files = append(files, entry.Name)
		}
		return files, nil, nil
	} else {
		files := []string{}
		sizes := []int64{}

		for _, parent := range []string{"chunks", "fossils"} {
			entries, err := storage.listFiles(threadIndex, storage.getPathID(parent), true)
			if err != nil {
				return nil, nil, err
			}

			for _, entry := range entries {
				name := entry.Name
				if parent == "fossils" {
					name += ".fsl"
				}
				storage.savePathID(parent+"/"+entry.Name, entry.Id)
				files = append(files, name)
				sizes = append(sizes, entry.Size)
			}
		}
		return files, sizes, nil
	}

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *GCDStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	filePath = storage.convertFilePath(filePath)
	fileID, ok := storage.findPathID(filePath)
	if !ok {
		fileID, err = storage.getIDFromPath(threadIndex, filePath)
		if err != nil {
			LOG_TRACE("GCD_STORAGE", "Ignored file deletion error: %v", err)
			return nil
		}
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

	fileID, ok := storage.findPathID(from)
	if !ok {
		return fmt.Errorf("Attempting to rename file %s with unknown id", to)
	}

	fromParentID := storage.getPathID("chunks")
	toParentID := storage.getPathID("fossils")

	if strings.HasPrefix(from, "fossils") {
		fromParentID, toParentID = toParentID, fromParentID
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

	storage.savePathID(to, storage.getPathID(from))
	storage.deletePathID(from)
	return nil
}

// CreateDirectory creates a new directory.
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

	parentID := storage.getPathID("")
	name := dir

	if strings.HasPrefix(dir, "snapshots/") {
		parentID = storage.getPathID("snapshots")
		name = dir[len("snapshots/"):]
	}

	file := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{parentID},
	}

	for {
		file, err = storage.service.Files.Create(file).Fields("id").Do()
		if retry, err := storage.shouldRetry(threadIndex, err); err == nil && !retry {
			break
		} else if retry {
			continue
		} else {
			return err
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

	// GetFileInfo is never called on a fossil
	fileID, ok := storage.findPathID(filePath)
	if !ok {
		dir := path.Dir(filePath)
		if dir == "." {
			dir = ""
		}
		dirID, err := storage.getIDFromPath(threadIndex, dir)
		if err != nil {
			return false, false, 0, err
		}

		fileID, isDir, size, err = storage.listByName(threadIndex, dirID, path.Base(filePath))
		if fileID != "" {
			storage.savePathID(filePath, fileID)
		}
		return fileID != "", isDir, size, err
	}

	for {
		file, err := storage.service.Files.Get(fileID).Fields("id, mimeType").Do()
		if retry, err := storage.shouldRetry(threadIndex, err); err == nil && !retry {
			return true, file.MimeType == "application/vnd.google-apps.folder", file.Size, nil
		} else if retry {
			continue
		} else {
			return false, false, 0, err
		}
	}
}

// FindChunk finds the chunk with the specified id.  If 'isFossil' is true, it will search for chunk files with
// the suffix '.fsl'.
func (storage *GCDStorage) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {
	parentID := ""
	filePath = "chunks/" + chunkID
	realPath := storage.convertFilePath(filePath)
	if isFossil {
		parentID = storage.getPathID("fossils")
		filePath += ".fsl"
	} else {
		parentID = storage.getPathID("chunks")
	}

	fileID := ""
	fileID, _, size, err = storage.listByName(threadIndex, parentID, chunkID)
	if fileID != "" {
		storage.savePathID(realPath, fileID)
	}
	return filePath, fileID != "", size, err
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *GCDStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	// We never download the fossil so there is no need to convert the path
	fileID, ok := storage.findPathID(filePath)
	if !ok {
		fileID, err = storage.getIDFromPath(threadIndex, filePath)
		if err != nil {
			return err
		}
		storage.savePathID(filePath, fileID)
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

	parentID, ok := storage.findPathID(parent)
	if !ok {
		parentID, err = storage.getIDFromPath(threadIndex, parent)
		if err != nil {
			return err
		}
		storage.savePathID(parent, parentID)
	}

	file := &drive.File{
		Name:     path.Base(filePath),
		MimeType: "application/octet-stream",
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
