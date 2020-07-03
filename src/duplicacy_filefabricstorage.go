// Copyright (c) Storage Made Easy. All rights reserved.
// 
// This storage backend is contributed by Storage Made Easy (https://storagemadeeasy.com/) to be used in
// Duplicacy and its derivative works.
//

package duplicacy

import (
	"io"
	"fmt"
	"time"
	"sync"
	"bytes"
	"errors"
	"strings"
	"net/url"
	"net/http"
	"math/rand"
	"io/ioutil"
	"encoding/xml"
	"path/filepath"
	"mime/multipart"
)

// The XML element representing a file returned by the File Fabric server
type FileFabricFile struct {
	XMLName     xml.Name
	ID          string     `xml:"fi_id"`
	Path        string     `xml:"path"`
	Size        int64      `xml:"fi_size"`
	Type        int        `xml:"fi_type"`
}

// The XML element representing a file list returned by the server
type FileFabricFileList struct {
	XMLName xml.Name `xml:"files"`
	Files []FileFabricFile `xml:",any"`
}

type FileFabricStorage struct {
	StorageBase

	endpoint           string            // the server
	authToken          string            // the authentication token
	accessToken        string            // the access token (as returned by getTokenByAuthToken)
	storageDir         string            // the path of the storage directory
	storageDirID       string            // the id of 'storageDir'

	client             *http.Client      // the default http client
	threads            int               // number of threads
	maxRetries         int               // maximum number of tries
	directoryCache     map[string]string // stores ids for directories known to this backend
	directoryCacheLock sync.Mutex        // lock for accessing directoryCache

	isAuthorized       bool
	testMode           bool
}

var (
	errFileFabricAuthorizationFailure = errors.New("Authentication failure")
	errFileFabricDirectoryExists = errors.New("Directory exists")
)

// The general server response
type FileFabricResponse struct {
	Status string `xml:"status"`
	Message string `xml:"statusmessage"`
}

// Check the server response and return an error representing the error message it contains
func checkFileFabricResponse(response FileFabricResponse, actionFormat string, actionArguments ...interface{}) error {

	action := fmt.Sprintf(actionFormat, actionArguments...)
	if response.Status == "ok" && response.Message == "Success" {
		return nil
	} else if response.Status == "error_data" {
		if response.Message == "Folder with same name already exists." {
			return errFileFabricDirectoryExists
		}
	}

	return fmt.Errorf("Failed to %s (status: %s, message: %s)", action, response.Status, response.Message)
}

// Create a File Fabric storage backend
func CreateFileFabricStorage(endpoint string, token string, storageDir string, threads int) (storage *FileFabricStorage, err error) {

	if len(storageDir) > 0 && storageDir[len(storageDir)-1] != '/' {
		storageDir += "/"
	}

	storage = &FileFabricStorage{

		endpoint:       endpoint,
		authToken:      token,
		client:         http.DefaultClient,
		threads:        threads,
		directoryCache: make(map[string]string),
		maxRetries:     12,
	}

	err = storage.getAccessToken()
	if err != nil {
		return nil, err
	}

	storageDirID, isDir, _, err := storage.getFileInfo(0, storageDir)
	if err != nil {
		return nil, err
	}
	if storageDirID == "" {
		return nil, fmt.Errorf("Storage path %s does not exist", storageDir)
	}
	if !isDir {
		return nil, fmt.Errorf("Storage path %s is not a directory", storageDir)
	}
	storage.storageDir = storageDir
	storage.storageDirID = storageDirID

	for _, dir := range []string{"snapshots", "chunks"} {
		storage.CreateDirectory(0, dir)
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
	return storage, nil
}

// Retrieve the access token using an auth token
func (storage *FileFabricStorage) getAccessToken() (error) {

    formData := url.Values { "authtoken": {storage.authToken},}
	readCloser, _, _, err := storage.sendRequest(0, http.MethodPost, storage.getAPIURL("getTokenByAuthToken"), nil, formData)
	if err != nil {
		return err
	}

	defer readCloser.Close()
	defer io.Copy(ioutil.Discard, readCloser)	

    var output struct {
		FileFabricResponse
        Token string `xml:"token"`
    }

    err = xml.NewDecoder(readCloser).Decode(&output)
    if err != nil {
        return err
    }

	err = checkFileFabricResponse(output.FileFabricResponse, "request the access token")
	if err != nil {
		return err
	}

	storage.accessToken = output.Token
    return nil
}

// Determine if we should retry based on the number of retries given by 'retry' and if so calculate the delay with exponential backoff
func (storage *FileFabricStorage) shouldRetry(retry int, messageFormat string, messageArguments ...interface{}) bool {
	message := fmt.Sprintf(messageFormat, messageArguments...)

	if retry >= storage.maxRetries {
		LOG_WARN("FILEFABRIC_REQUEST", "%s", message)
		return false
	}
	backoff := 1 << uint(retry)
	if backoff > 60 {
		backoff = 60
	}
	delay := rand.Intn(backoff*500) + backoff*500
	LOG_INFO("FILEFABRIC_RETRY", "%s; retrying after %.1f seconds", message, float32(delay) / 1000.0)
	time.Sleep(time.Duration(delay) * time.Millisecond)
	return true
}

// Send a request to the server
func (storage *FileFabricStorage) sendRequest(threadIndex int, method string, requestURL string, requestHeaders map[string]string, input interface{}) ( io.ReadCloser, http.Header, int64, error) {

	var response *http.Response

	for retries := 0; ; retries++ {
		var inputReader io.Reader

		switch input.(type) {
		case url.Values:
			values := input.(url.Values)
			inputReader = strings.NewReader(values.Encode())
			if requestHeaders == nil {
				requestHeaders = make(map[string]string)
			}
			requestHeaders["Content-Type"] = "application/x-www-form-urlencoded"
		case *RateLimitedReader:
			rateLimitedReader := input.(*RateLimitedReader)
			rateLimitedReader.Reset()
			inputReader = rateLimitedReader
		default:
			LOG_FATAL("FILEFABRIC_REQUEST", "Input type is not supported")
			return nil, nil, 0, fmt.Errorf("Input type is not supported")
		}

		request, err := http.NewRequest(method, requestURL, inputReader)
		if err != nil {
			return nil, nil, 0, err
		}

		if requestHeaders != nil {
			for key, value := range requestHeaders {
				request.Header.Set(key, value)
			}
		}

		if _, ok := input.(*RateLimitedReader); ok {
			request.ContentLength = input.(*RateLimitedReader).Length()
		}

		response, err = storage.client.Do(request)
		if err != nil {
			if !storage.shouldRetry(retries, "[%d] %s %s returned an error: %v", threadIndex, method, requestURL, err) {
				return nil, nil, 0, err
			}
			continue
		}

		if response.StatusCode < 300 {
			return response.Body, response.Header, response.ContentLength, nil
		}

		defer response.Body.Close()
		defer io.Copy(ioutil.Discard, response.Body)	

		var output struct {
			Status string `xml:"status"`
			Message string `xml:"statusmessage"`
		}
	
		err = xml.NewDecoder(response.Body).Decode(&output)
		if err != nil {
			if !storage.shouldRetry(retries, "[%d] %s %s returned an invalid response: %v", threadIndex, method, requestURL, err) {
				return nil, nil, 0, err
			}
			continue
		}

		if !storage.shouldRetry(retries, "[%d] %s %s returned status: %s, message: %s", threadIndex, method, requestURL, output.Status, output.Message) {
			return nil, nil, 0, err
		}
	}

}

func (storage *FileFabricStorage) getAPIURL(function string) string {
	if storage.accessToken == "" {
		return "https://" + storage.endpoint + "/api/*/" + function + "/"
	} else {
		return "https://" + storage.endpoint + "/api/" + storage.accessToken + "/" + function + "/"
	}
}

// ListFiles return the list of files and subdirectories under 'dir'.  A subdirectories returned must have a trailing '/', with
// a size of 0.  If 'dir' is 'snapshots', only subdirectories will be returned.  If 'dir' is 'snapshots/repository_id', then only
// files will be returned.  If 'dir' is 'chunks', the implementation can return the list either recusively or non-recusively.
func (storage *FileFabricStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {
	if dir != "" && dir[len(dir)-1] != '/' {
		dir += "/"
	}

	dirID, _, _, err := storage.getFileInfo(threadIndex, dir)
	if err != nil {
		return nil, nil, err
	}

	if dirID == "" {
		return nil, nil, nil
	}

	lastID := ""

	for {
		formData := url.Values { "marker": {lastID}, "limit": {"1000"}, "includefolders": {"n"}, "fi_pid" : {dirID}}
		if dir == "snapshots/" {
			formData["includefolders"] = []string{"y"}
		}
		if storage.testMode {
			formData["limit"] = []string{"5"}
		}

		readCloser, _, _, err := storage.sendRequest(threadIndex, http.MethodPost, storage.getAPIURL("getListOfFiles"), nil, formData)
		if err != nil {
			return nil, nil, err
		}

		defer readCloser.Close()
		defer io.Copy(ioutil.Discard, readCloser)	

		var output struct {
			FileFabricResponse
			FileList FileFabricFileList `xml:"files"`
			Truncated int `xml:"truncated"`
		}

		err = xml.NewDecoder(readCloser).Decode(&output)
		if err != nil {
			return nil, nil, err
		}

		err = checkFileFabricResponse(output.FileFabricResponse, "list the storage directory '%s'", dir)
		if err != nil {
			return nil, nil, err
		}

		if dir == "snapshots/" {
			for _, file := range output.FileList.Files {
				if file.Type == 1 {
					files = append(files, file.Path + "/")
				}
				lastID = file.ID
			}
		} else {
			for _, file := range output.FileList.Files {
				if file.Type == 0 {
					files = append(files, file.Path)
					sizes = append(sizes, file.Size)
				}
				lastID = file.ID
			}
		}

		if output.Truncated != 1 {
			break
		}
	}
	return files, sizes, nil
}

// getFileInfo returns the information about the file or directory at 'filePath'.
func (storage *FileFabricStorage) getFileInfo(threadIndex int, filePath string) (fileID string, isDir bool, size int64, err error) {

	formData := url.Values { "path" : {storage.storageDir + filePath}}

	readCloser, _, _, err := storage.sendRequest(threadIndex, http.MethodPost, storage.getAPIURL("checkPathExists"), nil, formData)
	if err != nil {
		return "", false, 0, err
	}

	defer readCloser.Close()
	defer io.Copy(ioutil.Discard, readCloser)	

	var output struct {
		FileFabricResponse
		File FileFabricFile `xml:"file"`
		Exists string `xml:"exists"`
	}

	err = xml.NewDecoder(readCloser).Decode(&output)
	if err != nil {
		return "", false, 0, err
	}

	err = checkFileFabricResponse(output.FileFabricResponse, "get the info on '%s'", filePath)
	if err != nil {
		return "", false, 0, err
	}

	if output.Exists != "y" {
		return "", false, 0, nil
	} else {
		if output.File.Type == 1 {
			for filePath != "" && filePath[len(filePath)-1] == '/' {
				filePath = filePath[:len(filePath)-1]
			}
			
			storage.directoryCacheLock.Lock()
			storage.directoryCache[filePath] = output.File.ID
			storage.directoryCacheLock.Unlock()
		}
		return output.File.ID, output.File.Type == 1, output.File.Size, nil
	}
}

// GetFileInfo returns the information about the file or directory at 'filePath'.  This is a function required by the Storage interface.
func (storage *FileFabricStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

	fileID := ""
	fileID, isDir, size, err = storage.getFileInfo(threadIndex, filePath)
	return fileID != "", isDir, size, err
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *FileFabricStorage) DeleteFile(threadIndex int, filePath string) (err error) {

	fileID, _, _, _ := storage.getFileInfo(threadIndex, filePath)
	if fileID == "" {
		return nil
	}

	formData := url.Values { "fi_id" : {fileID}}

	readCloser, _, _, err := storage.sendRequest(threadIndex, http.MethodPost, storage.getAPIURL("doDeleteFile"), nil, formData)
	if err != nil {
		return err
	}

	defer readCloser.Close()
	defer io.Copy(ioutil.Discard, readCloser)	

	var output FileFabricResponse

	err = xml.NewDecoder(readCloser).Decode(&output)
	if err != nil {
		return err
	}

	err = checkFileFabricResponse(output, "delete file '%s'", filePath)
	if err != nil {
		return err
	}

	return nil
}

// MoveFile renames the file.
func (storage *FileFabricStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	fileID, _, _, _ := storage.getFileInfo(threadIndex, from)
	if fileID == "" {
		return nil
	}

	formData := url.Values { "fi_id" : {fileID}, "fi_name": {filepath.Base(to)},}

	readCloser, _, _, err := storage.sendRequest(threadIndex, http.MethodPost, storage.getAPIURL("doRenameFile"), nil, formData)
	if err != nil {
		return err
	}

	defer readCloser.Close()
	defer io.Copy(ioutil.Discard, readCloser)	

	var output FileFabricResponse

	err = xml.NewDecoder(readCloser).Decode(&output)
	if err != nil {
		return err
	}

	err = checkFileFabricResponse(output, "rename file '%s' to '%s'", from, to)
	if err != nil {
		return err
	}
	
	return nil
}

// createParentDirectory creates the parent directory if it doesn't exist in the cache.
func (storage *FileFabricStorage) createParentDirectory(threadIndex int, dir string) (parentID string, err error) {

	found := strings.LastIndex(dir, "/")
	if found == -1 {
		return storage.storageDirID, nil
	}
	parent := dir[:found]

	storage.directoryCacheLock.Lock()
	parentID = storage.directoryCache[parent]
	storage.directoryCacheLock.Unlock()

	if parentID != "" {
		return parentID, nil
	}

	parentID, err = storage.createDirectory(threadIndex, parent)
	if err != nil {
		if err == errFileFabricDirectoryExists {
		    var isDir bool
			parentID, isDir, _, err = storage.getFileInfo(threadIndex, parent)
			if err != nil {
				return "", err
			}
			if isDir == false {
				return "", fmt.Errorf("'%s' in the storage is a file", parent)
			}
			storage.directoryCacheLock.Lock()
			storage.directoryCache[parent] = parentID
			storage.directoryCacheLock.Unlock()
			return parentID, nil
		} else {
			return "", err
		}
	}
	return parentID, nil
}

// createDirectory creates a new directory.
func (storage *FileFabricStorage) createDirectory(threadIndex int, dir string) (dirID string, err error) {
	for dir != "" && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	parentID, err := storage.createParentDirectory(threadIndex, dir)
	if err != nil {
		return "", err
	}

	formData := url.Values { "fi_name": {filepath.Base(dir)}, "fi_pid" : {parentID}}

	readCloser, _, _, err := storage.sendRequest(threadIndex, http.MethodPost, storage.getAPIURL("doCreateNewFolder"), nil, formData)
	if err != nil {
		return "", err
	}

	defer readCloser.Close()
	defer io.Copy(ioutil.Discard, readCloser)	

	var output struct {
		FileFabricResponse
		File FileFabricFile `xml:"file"`
	}

	err = xml.NewDecoder(readCloser).Decode(&output)
	if err != nil {
		return "", err
	}

	err = checkFileFabricResponse(output.FileFabricResponse, "create directory '%s'", dir)
	if err != nil {
		return "", err
	}

	storage.directoryCacheLock.Lock()
	storage.directoryCache[dir] = output.File.ID
	storage.directoryCacheLock.Unlock()

	return output.File.ID, nil
}

func (storage *FileFabricStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	_, err = storage.createDirectory(threadIndex, dir)
	if err == errFileFabricDirectoryExists {
		return nil
	}
	return err
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *FileFabricStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	formData := url.Values { "fi_id" : {storage.storageDir + filePath}}

	readCloser, _, _, err := storage.sendRequest(threadIndex, http.MethodPost, storage.getAPIURL("getFile"), nil, formData)
	if err != nil {
		return err
	}

	defer readCloser.Close()
	defer io.Copy(ioutil.Discard, readCloser)	
	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/storage.threads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *FileFabricStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

	parentID, err := storage.createParentDirectory(threadIndex, filePath)
	if err != nil {
		return err
	}

	fileName := filepath.Base(filePath)
    requestBody := &bytes.Buffer{}
    writer := multipart.NewWriter(requestBody)
    part, _ := writer.CreateFormFile("file_1", fileName)
    part.Write(content)

    writer.WriteField("file_name1", fileName)
	writer.WriteField("fi_pid", parentID)
	writer.WriteField("fi_structtype", "g")
    writer.Close()

	headers := make(map[string]string)
	headers["Content-Type"] = writer.FormDataContentType()

	rateLimitedReader := CreateRateLimitedReader(requestBody.Bytes(), storage.UploadRateLimit/storage.threads)
	readCloser, _, _, err := storage.sendRequest(threadIndex, http.MethodPost, storage.getAPIURL("doUploadFiles"), headers, rateLimitedReader)

	defer readCloser.Close()
	defer io.Copy(ioutil.Discard, readCloser)	

	var output FileFabricResponse

	err = xml.NewDecoder(readCloser).Decode(&output)
	if err != nil {
		return err
	}

	err = checkFileFabricResponse(output, "upload file '%s'", filePath)
	if err != nil {
		return err
	}

	return nil
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *FileFabricStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *FileFabricStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *FileFabricStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *FileFabricStorage) IsFastListing() bool { return false }

// Enable the test mode.
func (storage *FileFabricStorage) EnableTestMode() { storage.testMode = true }
