// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com
//
//
// This storage backend is based on the work by Yuri Karamani from https://github.com/karamani/webdavclnt,
// released under the MIT license.
//
package duplicacy

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	//"net/http/httputil"
	"path/filepath"
	"strconv"
	"sync"
	"time"
	"strings"
)

type WebDAVStorage struct {
	StorageBase

	host       string
	port       int
	username   string
	password   string
	storageDir string
	useHTTP    bool

	client             *http.Client
	threads            int
	directoryCache     map[string]int // stores directories known to exist by this backend
	directoryCacheLock sync.Mutex     // lock for accessing directoryCache
}

var (
	errWebDAVAuthorizationFailure = errors.New("Authentication failed")
	errWebDAVMovedPermanently = errors.New("Moved permanently")
	errWebDAVNotExist = errors.New("Path does not exist")
	errWebDAVMaximumBackoff = errors.New("Maximum backoff reached")
	errWebDAVMethodNotAllowed = errors.New("Method not allowed")
)

func CreateWebDAVStorage(host string, port int, username string, password string, storageDir string, useHTTP bool, threads int) (storage *WebDAVStorage, err error) {
	if storageDir[len(storageDir)-1] != '/' {
		storageDir += "/"
	}

	storage = &WebDAVStorage{
		host:       host,
		port:       port,
		username:   username,
		password:   password,
		storageDir: "",
		useHTTP:    false,

		client:         http.DefaultClient,
		threads:        threads,
		directoryCache: make(map[string]int),
	}

	// Make sure it doesn't follow redirect
	storage.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
        return http.ErrUseLastResponse
	}

	exist, isDir, _, err := storage.GetFileInfo(0, storageDir)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("Storage path %s does not exist", storageDir)
	}
	if !isDir {
		return nil, fmt.Errorf("Storage path %s is not a directory", storageDir)
	}
	storage.storageDir = storageDir

	for _, dir := range []string{"snapshots", "chunks"} {
		storage.CreateDirectory(0, dir)
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
	return storage, nil
}

func (storage *WebDAVStorage) createConnectionString(uri string) string {

	url := storage.host

	if storage.useHTTP {
		url = "http://" + url
	} else {
		url = "https://" + url
	}

	if storage.port > 0 {
		url += fmt.Sprintf(":%d", storage.port)
	}
	return url + "/" + storage.storageDir + uri
}

func (storage *WebDAVStorage) retry(backoff int) int {
	delay := rand.Intn(backoff*500) + backoff*500
	time.Sleep(time.Duration(delay) * time.Millisecond)
	backoff *= 2
	return backoff
}

func (storage *WebDAVStorage) sendRequest(method string, uri string, depth int, data []byte) (io.ReadCloser, http.Header, error) {

	backoff := 1
	for i := 0; i < 8; i++ {

		var dataReader io.Reader
		headers := make(map[string]string)
		if method == "PROPFIND" {
			headers["Content-Type"] = "application/xml"
			headers["Depth"] = fmt.Sprintf("%d", depth)
			dataReader = bytes.NewReader(data)
		} else if method == "PUT" {
			headers["Content-Type"] = "application/octet-stream"
			dataReader = CreateRateLimitedReader(data, storage.UploadRateLimit/storage.threads)
		} else if method == "MOVE" {
			headers["Destination"] = storage.createConnectionString(string(data))
			headers["Content-Type"] = "application/octet-stream"
			dataReader = bytes.NewReader([]byte(""))
		} else {
			headers["Content-Type"] = "application/octet-stream"
			dataReader = bytes.NewReader(data)
		}

		request, err := http.NewRequest(method, storage.createConnectionString(uri), dataReader)
		if err != nil {
			return nil, nil, err
		}

		if len(storage.username) > 0 {
			request.SetBasicAuth(storage.username, storage.password)
		}

		for key, value := range headers {
			request.Header.Set(key, value)
		}

		//requestDump, err := httputil.DumpRequest(request, true)
		//LOG_INFO("debug", "Request: %s", requestDump)

		response, err := storage.client.Do(request)
		if err != nil {
			LOG_TRACE("WEBDAV_RETRY", "URL request '%s %s' returned an error (%v)", method, uri, err)
			backoff = storage.retry(backoff)
			continue
		}

		if response.StatusCode < 300 {
			return response.Body, response.Header, nil
		}

		if response.StatusCode == 301 {
			return nil, nil, errWebDAVMovedPermanently
		}

		response.Body.Close()
		if response.StatusCode == 404 {
			// Retry if it is UPLOAD, otherwise return immediately
			if method != "PUT" {
				return nil, nil, errWebDAVNotExist
			}
		} else if response.StatusCode == 405 {
			return nil, nil, errWebDAVMethodNotAllowed
		}
		LOG_INFO("WEBDAV_RETRY", "URL request '%s %s' returned status code %d", method, uri, response.StatusCode)
		backoff = storage.retry(backoff)
	}
	return nil, nil, errWebDAVMaximumBackoff
}

type WebDAVProperties map[string]string

type WebDAVPropValue struct {
	XMLName xml.Name `xml:""`
	Value   string   `xml:",innerxml"`
}

type WebDAVProp struct {
	PropList []WebDAVPropValue `xml:",any"`
}

type WebDAVPropStat struct {
	Prop *WebDAVProp `xml:"prop"`
}

type WebDAVResponse struct {
	Href     string          `xml:"href"`
	PropStat *WebDAVPropStat `xml:"propstat"`
}

type WebDAVMultiStatus struct {
	Responses []WebDAVResponse `xml:"response"`
}

func (storage *WebDAVStorage) getProperties(uri string, depth int, properties ...string) (map[string]WebDAVProperties, error) {

	propfind := "<prop>"
	for _, p := range properties {
		propfind += fmt.Sprintf("<%s/>", p)
	}
	propfind += "</prop>"

	body := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8" ?><propfind xmlns="DAV:">%s</propfind>`, propfind)

	readCloser, _, err := storage.sendRequest("PROPFIND", uri, depth, []byte(body))
	if err != nil {
		return nil, err
	}
	defer readCloser.Close()
	content, err := ioutil.ReadAll(readCloser)
	if err != nil {
		return nil, err
	}

	object := WebDAVMultiStatus{}
	err = xml.Unmarshal(content, &object)
	if err != nil {
		return nil, err
	}

	if object.Responses == nil || len(object.Responses) == 0 {
		return nil, errors.New("no WebDAV responses")
	}

	responses := make(map[string]WebDAVProperties)

	for _, responseTag := range object.Responses {

		if responseTag.PropStat == nil || responseTag.PropStat.Prop == nil || responseTag.PropStat.Prop.PropList == nil {
			return nil, errors.New("no WebDAV properties")
		}

		properties := make(WebDAVProperties)
		for _, prop := range responseTag.PropStat.Prop.PropList {
			properties[prop.XMLName.Local] = prop.Value
		}

		responseKey := responseTag.Href
		responses[responseKey] = properties

	}

	return responses, nil
}

// ListFiles return the list of files and subdirectories under 'dir'.  A subdirectories returned must have a trailing '/', with
// a size of 0.  If 'dir' is 'snapshots', only subdirectories will be returned.  If 'dir' is 'snapshots/repository_id', then only
// files will be returned.  If 'dir' is 'chunks', the implementation can return the list either recusively or non-recusively.
func (storage *WebDAVStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {
	if dir[len(dir)-1] != '/' {
		dir += "/"
	}
	properties, err := storage.getProperties(dir, 1, "getcontentlength", "resourcetype")
	if err != nil {
		return nil, nil, err
	}

	prefixLength := len(storage.storageDir) + len(dir) + 1

	for file, m := range properties {
		if len(file) <= prefixLength {
			continue
		}

		isDir := false
		size := 0
		if resourceType, exist := m["resourcetype"]; exist && strings.Contains(resourceType, "collection") {
			isDir = true
		} else if length, exist := m["getcontentlength"]; exist {
			if length == "" {
				isDir = true
			} else {
				size, _ = strconv.Atoi(length)
			}
		} else {
			continue
		}

		if !isDir {
			if dir != "snapshots/" {
				files = append(files, file[prefixLength:])
				sizes = append(sizes, int64(size))
			}
		} else {
			// This is a dir
			file := file[prefixLength:]
			if file[len(file)-1] != '/' {
				file += "/"
			}
			files = append(files, file)
			sizes = append(sizes, int64(0))
		}
	}

	return files, sizes, nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *WebDAVStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	properties, err := storage.getProperties(filePath, 0, "getcontentlength", "resourcetype")
	if err != nil {
		if err == errWebDAVNotExist {
			return false, false, 0, nil
		}
		if err == errWebDAVMovedPermanently {
			// This must be a directory
			return true, true, 0, nil
		}
		return false, false, 0, err
	}

	if m, exist := properties["/" + storage.storageDir + filePath]; !exist {
		return false, false, 0, nil
	} else if resourceType, exist := m["resourcetype"]; exist && strings.Contains(resourceType, "collection") {
		return true, true, 0, nil
	} else if length, exist := m["getcontentlength"]; exist && length != ""{
		value, _ := strconv.Atoi(length)
		return true, false, int64(value), nil
	} else {
		return true, true, 0, nil
	}
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *WebDAVStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	readCloser, _, err := storage.sendRequest("DELETE", filePath, 0, []byte(""))
	if err != nil {
		return err
	}
	readCloser.Close()
	return nil
}

// MoveFile renames the file.
func (storage *WebDAVStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	readCloser, _, err := storage.sendRequest("MOVE", from, 0, []byte(to))
	if err != nil {
		return err
	}
	readCloser.Close()
	return nil
}

// createParentDirectory creates the parent directory if it doesn't exist in the cache
func (storage *WebDAVStorage) createParentDirectory(threadIndex int, dir string) (err error) {
	parent := filepath.Dir(dir)

	if parent == "." {
		return nil
	}

	storage.directoryCacheLock.Lock()
	_, exist := storage.directoryCache[parent]
	storage.directoryCacheLock.Unlock()

	if exist {
		return nil
	}

	err = storage.CreateDirectory(threadIndex, parent)
	if err == nil {
		storage.directoryCacheLock.Lock()
		storage.directoryCache[parent] = 1
		storage.directoryCacheLock.Unlock()
	}
	return err
}

// CreateDirectory creates a new directory.
func (storage *WebDAVStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	for dir != "" && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	if dir == "" {
		return nil
	}

	// If there is an error in creating the parent directory, proceed anyway
	storage.createParentDirectory(threadIndex, dir)

	readCloser, _, err := storage.sendRequest("MKCOL", dir, 0, []byte(""))
	if err != nil {
		if err == errWebDAVMethodNotAllowed || err == errWebDAVMovedPermanently {
			// We simply ignore these errors and assume that the directory already exists
			return nil
		}
		return err
	}
	readCloser.Close()
	return nil
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *WebDAVStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	readCloser, _, err := storage.sendRequest("GET", filePath, 0, nil)
	if err != nil {
		return err
	}

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/storage.threads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *WebDAVStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

	// If there is an error in creating the parent directory, proceed anyway
	storage.createParentDirectory(threadIndex, filePath)

	readCloser, _, err := storage.sendRequest("PUT", filePath, 0, content)
	if err != nil {
		return err
	}
	readCloser.Close()
	return nil
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *WebDAVStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *WebDAVStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *WebDAVStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *WebDAVStorage) IsFastListing() bool { return false }

// Enable the test mode.
func (storage *WebDAVStorage) EnableTestMode() {}
