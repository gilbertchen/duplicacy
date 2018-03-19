// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"strconv"
	"strings"
	"time"

	"github.com/ncw/swift"
)

type SwiftStorage struct {
	StorageBase

	connection *swift.Connection
	container  string
	storageDir string
	threads    int
}

// CreateSwiftStorage creates an OpenStack Swift storage object.  storageURL is in the form of 
// `user@authURL/container/path?arg1=value1&arg2=value2``
func CreateSwiftStorage(storageURL string, key string, threads int) (storage *SwiftStorage, err error) {

	// This is the map to store all arguments
	arguments := make(map[string]string)

	// Check if there are arguments provided as a query string
	if strings.Contains(storageURL, "?") {
		urlAndArguments := strings.SplitN(storageURL, "?", 2)
		storageURL = urlAndArguments[0]
		for _, pair := range strings.Split(urlAndArguments[1], "&") {
			if strings.Contains(pair, "=") {
				keyAndValue := strings.Split(pair, "=")
				arguments[keyAndValue[0]] = keyAndValue[1]
			}
		}
	}

	// Take out the user name if there is one
	if strings.Contains(storageURL, "@") {
		userAndURL := strings.Split(storageURL, "@")
		arguments["user"] = userAndURL[0]
		storageURL = userAndURL[1]
	}

	// The version is used to split authURL and container/path
	versions := []string{"/v1/", "/v1.0/", "/v2/", "/v2.0/", "/v3/", "/v3.0/", "/v4/", "/v4.0/"}
	storageDir := ""
	for _, version := range versions {
		if strings.Contains(storageURL, version) {
			urlAndStorageDir := strings.SplitN(storageURL, version, 2)
			storageURL = urlAndStorageDir[0] + version[0:len(version)-1]
			storageDir = urlAndStorageDir[1]
		}
	}

	// If no container/path is specified, find them from the arguments
	if storageDir == "" {
		storageDir = arguments["storage_dir"]
	}

	// Now separate the container name from the storage path
	container := ""
	if strings.Contains(storageDir, "/") {
		containerAndStorageDir := strings.SplitN(storageDir, "/", 2)
		container = containerAndStorageDir[0]
		storageDir = containerAndStorageDir[1]
		if len(storageDir) > 0 && storageDir[len(storageDir)-1] != '/' {
			storageDir += "/"
		}
	} else {
		container = storageDir
		storageDir = ""
	}

	// Number of retries on err
	retries := 4
	if value, ok := arguments["retries"]; ok {
		retries, _ = strconv.Atoi(value)
	}

	// Connect channel timeout
	connectionTimeout := 10
	if value, ok := arguments["connection_timeout"]; ok {
		connectionTimeout, _ = strconv.Atoi(value)
	}

	// Data channel timeout
	timeout := 60
	if value, ok := arguments["timeout"]; ok {
		timeout, _ = strconv.Atoi(value)
	}

	// Auth version; default to auto-detect
	authVersion := 0
	if value, ok := arguments["auth_version"]; ok {
		authVersion, _ = strconv.Atoi(value)
	}

	// Allow http to be used by setting "protocol=http" in arguments
	if _, ok := arguments["protocol"]; !ok {
		arguments["protocol"] = "https"
	}

	// Please refer to https://godoc.org/github.com/ncw/swift#Connection
	connection := swift.Connection{
		Domain:         arguments["domain"],
		DomainId:       arguments["domain_id"],
		UserName:       arguments["user"],
		UserId:         arguments["user_id"],
		ApiKey:         key,
		AuthUrl:        arguments["protocol"] + "://" + storageURL,
		Retries:        retries,
		UserAgent:      arguments["user_agent"],
		ConnectTimeout: time.Duration(connectionTimeout) * time.Second,
		Timeout:        time.Duration(timeout) * time.Second,
		Region:         arguments["region"],
		AuthVersion:    authVersion,
		Internal:       false,
		Tenant:         arguments["tenant"],
		TenantId:       arguments["tenant_id"],
		EndpointType:   swift.EndpointType(arguments["endpiont_type"]),
		TenantDomain:   arguments["tenant_domain"],
		TenantDomainId: arguments["tenant_domain_id"],
		TrustId:        arguments["trust_id"],
	}

	_, _, err = connection.Container(container)
	if err != nil {
		return nil, err
	}

	storage = &SwiftStorage{
		connection: &connection,
		container:  container,
		storageDir: storageDir,
		threads:    threads,
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{1}, 1)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *SwiftStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {
	if len(dir) > 0 && dir[len(dir)-1] != '/' {
		dir += "/"
	}
	isSnapshotDir := dir == "snapshots/"
	dir = storage.storageDir + dir

	options := swift.ObjectsOpts{
		Prefix: dir,
		Limit:  1000,
	}

	if isSnapshotDir {
		options.Delimiter = '/'
	}

	objects, err := storage.connection.ObjectsAll(storage.container, &options)
	if err != nil {
		return nil, nil, err
	}

	for _, obj := range objects {
		if isSnapshotDir {
			if obj.SubDir != "" {
				files = append(files, obj.SubDir[len(dir):])
				sizes = append(sizes, 0)
			}
		} else {
			files = append(files, obj.Name[len(dir):])
			sizes = append(sizes, obj.Bytes)
		}
	}

	return files, sizes, nil
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *SwiftStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	return storage.connection.ObjectDelete(storage.container, storage.storageDir+filePath)
}

// MoveFile renames the file.
func (storage *SwiftStorage) MoveFile(threadIndex int, from string, to string) (err error) {
	return storage.connection.ObjectMove(storage.container, storage.storageDir+from,
		storage.container, storage.storageDir+to)
}

// CreateDirectory creates a new directory.
func (storage *SwiftStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	// Does nothing as directories do not exist in OpenStack Swift
	return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *SwiftStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	object, _, err := storage.connection.Object(storage.container, storage.storageDir+filePath)

	if err != nil {
		if err == swift.ObjectNotFound {
			return false, false, 0, nil
		} else {
			return false, false, 0, err
		}
	}

	return true, false, object.Bytes, nil
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *SwiftStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {

	file, _, err := storage.connection.ObjectOpen(storage.container, storage.storageDir+filePath, false, nil)
	if err != nil {
		return err
	}
	_, err = RateLimitedCopy(chunk, file, storage.DownloadRateLimit/storage.threads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *SwiftStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {
	reader := CreateRateLimitedReader(content, storage.UploadRateLimit/storage.threads)
	_, err = storage.connection.ObjectPut(storage.container, storage.storageDir+filePath, reader, true, "", "application/duplicacy", nil)
	return err
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *SwiftStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *SwiftStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *SwiftStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *SwiftStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *SwiftStorage) EnableTestMode() {
}
