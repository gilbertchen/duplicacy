// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	crypto_rand "crypto/rand"
	"math/rand"

	azurestorage "github.com/gilbertchen/azure-sdk-for-go/storage"
)

var testStorageName = flag.String("storage", "", "the test storage to use")
var testRateLimit = flag.Int("limit-rate", 0, "maximum transfer speed in kbytes/sec")
var testQuickMode = flag.Bool("quick", false, "quick test")
var testThreads = flag.Int("threads", 1, "number of downloading/uploading threads")
var testFixedChunkSize = flag.Bool("fixed-chunk-size", false, "fixed chunk size")
var testRSAEncryption = flag.Bool("rsa", false, "enable RSA encryption")
var testErasureCoding = flag.Bool("erasure-coding", false, "enable Erasure Coding")

func loadStorage(localStoragePath string, threads int) (Storage, error) {

	if *testStorageName == "" || *testStorageName == "file" {
		storage, err := CreateFileStorage(localStoragePath, false, threads)
		if storage != nil {
			// Use a read level of at least 2 because this will catch more errors than a read level of 1.
			storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		}
		return storage, err
	}

	description, err := ioutil.ReadFile("test_storage.conf")
	if err != nil {
		return nil, err
	}

	configs := make(map[string]map[string]string)

	err = json.Unmarshal(description, &configs)
	if err != nil {
		return nil, err
	}

	config, found := configs[*testStorageName]
	if !found {
		return nil, fmt.Errorf("No storage named '%s' found", *testStorageName)
	}

	if *testStorageName == "flat" {
		storage, err := CreateFileStorage(localStoragePath, false, threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "samba" {
		storage, err := CreateFileStorage(localStoragePath, true, threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "sftp" {
		port, _ := strconv.Atoi(config["port"])
		storage, err := CreateSFTPStorageWithPassword(config["server"], port, config["username"], config["directory"], 2, config["password"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "s3" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, false)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "wasabi" {
		storage, err := CreateWasabiStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "s3c" {
		storage, err := CreateS3CStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "digitalocean" {
		storage, err := CreateS3CStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "minio" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, false, true)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "minios" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, true)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "dropbox" {
		storage, err := CreateDropboxStorage(config["token"], config["directory"], 1, threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "b2" {
		storage, err := CreateB2Storage(config["account"], config["key"], "", config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "gcs-s3" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, false)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "gcs" {
		storage, err := CreateGCSStorage(config["token_file"], config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "gcs-sa" {
		storage, err := CreateGCSStorage(config["token_file"], config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "azure" {
		storage, err := CreateAzureStorage(config["account"], config["key"], config["container"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "acd" {
		storage, err := CreateACDStorage(config["token_file"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "gcd" {
		storage, err := CreateGCDStorage(config["token_file"], "", config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "gcd-shared" {
		storage, err := CreateGCDStorage(config["token_file"], config["drive"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "gcd-impersonate" {
		storage, err := CreateGCDStorage(config["token_file"], config["drive"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "one" {
		storage, err := CreateOneDriveStorage(config["token_file"], false, config["storage_path"], threads, "", "", "")
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "odb" {
		storage, err := CreateOneDriveStorage(config["token_file"], true, config["storage_path"], threads, "", "", "")
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "one" {
		storage, err := CreateOneDriveStorage(config["token_file"], false, config["storage_path"], threads, "", "", "")
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "hubic" {
		storage, err := CreateHubicStorage(config["token_file"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "memset" {
		storage, err := CreateSwiftStorage(config["storage_url"], config["key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "pcloud" || *testStorageName == "box" {
		storage, err := CreateWebDAVStorage(config["host"], 0, config["username"], config["password"], config["storage_path"], false, threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "fabric" {
		storage, err := CreateFileFabricStorage(config["endpoint"], config["token"], config["storage_path"], threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "storj" {
		storage, err := CreateStorjStorage(config["satellite"], config["key"], config["passphrase"], config["bucket"], config["storage_path"], threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "storj" {
		storage, err := CreateStorjStorage(config["satellite"], config["key"], config["passphrase"], config["bucket"], config["storage_path"], threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if *testStorageName == "smb" {
		port, _ := strconv.Atoi(config["port"])
		storage, err := CreateSambaStorage(config["server"], port, config["username"], config["password"], config["share"], config["storage_path"], threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	}

	return nil, fmt.Errorf("Invalid storage named: %s", *testStorageName)
}

func cleanStorage(storage Storage) {

	directories := make([]string, 0, 1024)
	snapshots := make([]string, 0, 1024)

	directories = append(directories, "snapshots/")

	LOG_INFO("STORAGE_LIST", "Listing snapshots in the storage")
	for len(directories) > 0 {

		dir := directories[len(directories)-1]
		directories = directories[:len(directories)-1]

		files, _, err := storage.ListFiles(0, dir)
		if err != nil {
			LOG_ERROR("STORAGE_LIST", "Failed to list the directory %s: %v", dir, err)
			return
		}

		for _, file := range files {
			if len(file) > 0 && file[len(file)-1] == '/' {
				directories = append(directories, dir+file)
			} else {
				snapshots = append(snapshots, dir+file)
			}
		}
	}

	LOG_INFO("STORAGE_DELETE", "Deleting %d snapshots in the storage", len(snapshots))
	for _, snapshot := range snapshots {
		storage.DeleteFile(0, snapshot)
	}

	for _, chunk := range listChunks(storage) {
		storage.DeleteFile(0, "chunks/"+chunk)
	}

	storage.DeleteFile(0, "config")

	return
}

func listChunks(storage Storage) (chunks []string) {

	directories := make([]string, 0, 1024)

	directories = append(directories, "chunks/")

	for len(directories) > 0 {

		dir := directories[len(directories)-1]
		directories = directories[:len(directories)-1]

		files, _, err := storage.ListFiles(0, dir)
		if err != nil {
			LOG_ERROR("CHUNK_LIST", "Failed to list the directory %s: %v", dir, err)
			return nil
		}

		for _, file := range files {
			if len(file) > 0 && file[len(file)-1] == '/' {
				directories = append(directories, dir+file)
			} else {
				chunk := dir + file
				chunk = chunk[len("chunks/"):]
				chunks = append(chunks, chunk)
			}
		}
	}

	return
}

func moveChunk(t *testing.T, storage Storage, chunkID string, isFossil bool, delay int) {

	filePath, exist, _, err := storage.FindChunk(0, chunkID, isFossil)

	if err != nil {
		t.Errorf("Error find chunk %s: %v", chunkID, err)
		return
	}

	to := filePath + ".fsl"
	if isFossil {
		to = filePath[:len(filePath)-len(".fsl")]
	}

	err = storage.MoveFile(0, filePath, to)
	if err != nil {
		t.Errorf("Error renaming file %s to %s: %v", filePath, to, err)
	}

	time.Sleep(time.Duration(delay) * time.Second)

	_, exist, _, err = storage.FindChunk(0, chunkID, isFossil)
	if err != nil {
		t.Errorf("Error get file info for chunk %s: %v", chunkID, err)
	}

	if exist {
		t.Errorf("File %s still exists after renaming", filePath)
	}

	_, exist, _, err = storage.FindChunk(0, chunkID, !isFossil)
	if err != nil {
		t.Errorf("Error get file info for %s: %v", to, err)
	}

	if !exist {
		t.Errorf("File %s doesn't exist", to)
	}

}

func TestStorage(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "storage_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	LOG_INFO("STORAGE_TEST", "storage: %s", *testStorageName)

	threads := 8
	storage, err := loadStorage(testDir, threads)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	storage.EnableTestMode()
	storage.SetRateLimits(*testRateLimit, *testRateLimit)

	delay := 0
	if _, ok := storage.(*ACDStorage); ok {
		delay = 5
	}
	if _, ok := storage.(*HubicStorage); ok {
		delay = 2
	}

	for _, dir := range []string{"chunks", "snapshots"} {
		err = storage.CreateDirectory(0, dir)
		if err != nil {
			t.Errorf("Failed to create directory %s: %v", dir, err)
			return
		}
	}

	storage.CreateDirectory(0, "snapshots/repository1")
	storage.CreateDirectory(0, "snapshots/repository2")

	storage.CreateDirectory(0, "shared")

	// Upload to the same directory by multiple goroutines
	count := threads
	finished := make(chan int, count)
	for i := 0; i < count; i++ {
		go func(threadIndex int, name string) {
			err := storage.UploadFile(threadIndex, name, []byte("this is a test file"))
			if err != nil {
				t.Errorf("Error to upload '%s': %v", name, err)
			}
			finished <- 0
		}(i, fmt.Sprintf("shared/a/b/c/%d", i))
	}

	for i := 0; i < count; i++ {
		<-finished
	}

	for i := 0; i < count; i++ {
		storage.DeleteFile(0, fmt.Sprintf("shared/a/b/c/%d", i))
	}
	storage.DeleteFile(0, "shared/a/b/c")
	storage.DeleteFile(0, "shared/a/b")
	storage.DeleteFile(0, "shared/a")

	time.Sleep(time.Duration(delay) * time.Second)
	{

		// Upload fake snapshot files so that for storages having no concept of directories,
		// ListFiles("snapshots") still returns correct snapshot IDs.

		// Create a random file not a text file to make ACD Storage happy.
		content := make([]byte, 100)
		_, err = crypto_rand.Read(content)
		if err != nil {
			t.Errorf("Error generating random content: %v", err)
			return
		}

		err = storage.UploadFile(0, "snapshots/repository1/1", content)
		if err != nil {
			t.Errorf("Error to upload snapshots/repository1/1: %v", err)
		}

		err = storage.UploadFile(0, "snapshots/repository2/1", content)
		if err != nil {
			t.Errorf("Error to upload snapshots/repository2/1: %v", err)
		}
	}

	time.Sleep(time.Duration(delay) * time.Second)

	snapshotDirs, _, err := storage.ListFiles(0, "snapshots/")
	if err != nil {
		t.Errorf("Failed to list snapshot ids: %v", err)
		return
	}

	snapshotIDs := []string{}
	for _, snapshotDir := range snapshotDirs {
		if len(snapshotDir) > 0 && snapshotDir[len(snapshotDir)-1] == '/' {
			snapshotIDs = append(snapshotIDs, snapshotDir[:len(snapshotDir)-1])
		}
	}

	if len(snapshotIDs) < 2 {
		t.Errorf("Snapshot directories not created")
		return
	}

	for _, snapshotID := range snapshotIDs {
		snapshots, _, err := storage.ListFiles(0, "snapshots/"+snapshotID)
		if err != nil {
			t.Errorf("Failed to list snapshots for %s: %v", snapshotID, err)
			return
		}
		for _, snapshot := range snapshots {
			storage.DeleteFile(0, "snapshots/"+snapshotID+"/"+snapshot)
		}
	}

	time.Sleep(time.Duration(delay) * time.Second)

	storage.DeleteFile(0, "config")

	for _, file := range []string{"snapshots/repository1/1", "snapshots/repository2/1"} {
		exist, _, _, err := storage.GetFileInfo(0, file)
		if err != nil {
			t.Errorf("Failed to get file info for %s: %v", file, err)
			return
		}
		if exist {
			t.Errorf("File %s still exists after deletion", file)
			return
		}
	}

	numberOfFiles := 10
	maxFileSize := 64 * 1024

	if *testQuickMode {
		numberOfFiles = 2
	}

	chunks := []string{}

	for i := 0; i < numberOfFiles; i++ {
		content := make([]byte, rand.Int()%maxFileSize+1)
		_, err = crypto_rand.Read(content)
		if err != nil {
			t.Errorf("Error generating random content: %v", err)
			return
		}

		hasher := sha256.New()
		hasher.Write(content)
		chunkID := hex.EncodeToString(hasher.Sum(nil))
		chunks = append(chunks, chunkID)

		filePath, exist, _, err := storage.FindChunk(0, chunkID, false)
		if err != nil {
			t.Errorf("Failed to list the chunk %s: %v", chunkID, err)
			return
		}
		if exist {
			t.Errorf("Chunk %s already exists", chunkID)
		}

		err = storage.UploadFile(0, filePath, content)
		if err != nil {
			t.Errorf("Failed to upload the file %s: %v", filePath, err)
			return
		}
		LOG_INFO("STORAGE_CHUNK", "Uploaded chunk: %s, size: %d", filePath, len(content))
	}

	LOG_INFO("STORAGE_FOSSIL", "Making %s a fossil", chunks[0])
	moveChunk(t, storage, chunks[0], false, delay)
	LOG_INFO("STORAGE_FOSSIL", "Making %s a chunk", chunks[0])
	moveChunk(t, storage, chunks[0], true, delay)

	config := CreateConfig()
	config.MinimumChunkSize = 100
	config.chunkPool = make(chan *Chunk, numberOfFiles*2)

	chunk := CreateChunk(config, true)

	for _, chunkID := range chunks {

		chunk.Reset(false)
		filePath, exist, _, err := storage.FindChunk(0, chunkID, false)
		if err != nil {
			t.Errorf("Error getting file info for chunk %s: %v", chunkID, err)
			continue
		} else if !exist {
			t.Errorf("Chunk %s does not exist", chunkID)
			continue
		} else {
			err = storage.DownloadFile(0, filePath, chunk)
			if err != nil {
				t.Errorf("Error downloading file %s: %v", filePath, err)
				continue
			}
			LOG_INFO("STORAGE_CHUNK", "Downloaded chunk: %s, size: %d", filePath, chunk.GetLength())
		}

		hasher := sha256.New()
		hasher.Write(chunk.GetBytes())
		hash := hex.EncodeToString(hasher.Sum(nil))

		if hash != chunkID {
			t.Errorf("File %s, hash %s, size %d", chunkID, hash, chunk.GetBytes())
		}
	}

	LOG_INFO("STORAGE_FOSSIL", "Making %s a fossil", chunks[1])
	moveChunk(t, storage, chunks[1], false, delay)

	filePath, exist, _, err := storage.FindChunk(0, chunks[1], true)
	if err != nil {
		t.Errorf("Error getting file info for fossil %s: %v", chunks[1], err)
	} else if !exist {
		t.Errorf("Fossil %s does not exist", chunks[1])
	} else {
		err = storage.DeleteFile(0, filePath)
		if err != nil {
			t.Errorf("Failed to delete file %s: %v", filePath, err)
		} else {
			time.Sleep(time.Duration(delay) * time.Second)
			filePath, exist, _, err = storage.FindChunk(0, chunks[1], true)
			if err != nil {
				t.Errorf("Error get file info for deleted fossil %s: %v", chunks[1], err)
			} else if exist {
				t.Errorf("Fossil %s still exists after deletion", chunks[1])
			}
		}
	}

	allChunks := []string{}
	for _, file := range listChunks(storage) {
		allChunks = append(allChunks, file)
	}

	for _, file := range allChunks {

		err = storage.DeleteFile(0, "chunks/"+file)
		if err != nil {
			t.Errorf("Failed to delete the file %s: %v", file, err)
			return
		}
	}

}

func TestCleanStorage(t *testing.T) {
	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "storage_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	LOG_INFO("STORAGE_TEST", "storage: %s", *testStorageName)

	storage, err := loadStorage(testDir, 1)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}

	directories := make([]string, 0, 1024)
	directories = append(directories, "snapshots/")
	directories = append(directories, "chunks/")

	for len(directories) > 0 {

		dir := directories[len(directories)-1]
		directories = directories[:len(directories)-1]

		LOG_INFO("LIST_FILES", "Listing %s", dir)

		files, _, err := storage.ListFiles(0, dir)
		if err != nil {
			LOG_ERROR("LIST_FILES", "Failed to list the directory %s: %v", dir, err)
			return
		}

		for _, file := range files {
			if len(file) > 0 && file[len(file)-1] == '/' {
				directories = append(directories, dir+file)
			} else {
				storage.DeleteFile(0, dir+file)
				LOG_INFO("DELETE_FILE", "Deleted file %s", file)
			}
		}
	}

	storage.DeleteFile(0, "config")
	LOG_INFO("DELETE_FILE", "Deleted config")

	files, _, err := storage.ListFiles(0, "chunks/")
	for _, file := range files {
		if len(file) > 0 && file[len(file)-1] != '/' {
			LOG_DEBUG("FILE_EXIST", "File %s exists after deletion", file)
		}
	}

}

// prefixNames returns the names in 'files' that start with 'prefix', the way the flat object stores list them: without
// a delimiter every matching name is returned, and with one the names are broken at the delimiter and the folder
// replaces the files it contains.  It is the behaviour that a bucket with a delimiter is expected to provide, so a
// test can tell a subtree scan from a listing of the direct children.
func prefixNames(files []string, prefix string, delimiter string) (names []string) {

	sorted := append([]string{}, files...)
	sort.Strings(sorted)

	seen := make(map[string]bool)
	for _, file := range sorted {
		if !strings.HasPrefix(file, prefix) {
			continue
		}
		name := file
		if delimiter != "" {
			rest := file[len(prefix):]
			if index := strings.Index(rest, delimiter); index >= 0 {
				name = prefix + rest[:index+len(delimiter)]
			}
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}

	return names
}

// b2ListRequest records what a B2 listing asked the service for.
type b2ListRequest struct {
	prefix        string
	delimiter     string
	startFileName string
}

// b2TestServer is a B2 endpoint that implements the part of b2_list_file_names that B2Storage depends on, and records
// what was asked of it.  The names it answers with follow the prefix/delimiter/startFileName rules of the real
// service, so 'returned' shows whether the caller scanned the whole subtree or only the direct children.
type b2TestServer struct {
	*httptest.Server

	files    []string
	requests []b2ListRequest
	returned []string
}

func newB2TestServer(files []string) *b2TestServer {

	server := &b2TestServer{files: files}

	server.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {

		if request.URL.Path != "/b2api/v1/b2_list_file_names" {
			http.Error(writer, "unexpected request "+request.URL.Path, http.StatusNotFound)
			return
		}

		var input struct {
			Prefix        string `json:"prefix"`
			Delimiter     string `json:"delimiter"`
			StartFileName string `json:"startFileName"`
		}
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}

		// b2_list_file_names never returns a name before startFileName.
		candidates := []string{}
		for _, file := range server.files {
			if file >= input.StartFileName {
				candidates = append(candidates, file)
			}
		}

		server.requests = append(server.requests, b2ListRequest{input.Prefix, input.Delimiter, input.StartFileName})
		server.returned = prefixNames(candidates, input.Prefix, input.Delimiter)

		type entry struct {
			FileName string `json:"fileName"`
			Action   string `json:"action"`
		}
		output := struct {
			Files        []entry `json:"files"`
			NextFileName string  `json:"nextFileName"`
		}{}

		for _, name := range server.returned {
			action := "upload"
			if strings.HasSuffix(name, "/") {
				action = "folder"
			}
			output.Files = append(output.Files, entry{name, action})
		}

		if err := json.NewEncoder(writer).Encode(output); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	}))

	return server
}

// createStorage builds a B2Storage whose client talks to this test server.
func (server *b2TestServer) createStorage() *B2Storage {

	client := NewB2Client("test-account", "test-application-key", "", "", 1)
	client.BucketID = "test-bucket"
	client.BucketName = "test-bucket"
	client.APIURL = server.URL
	client.DownloadURL = server.URL
	client.IsAuthorized = true
	client.HTTPClient = server.Client()

	storage := &B2Storage{client: client}
	storage.DerivedStorage = storage
	return storage
}

// Listing the snapshot ids must not walk every snapshot file of every id: B2 is a flat object store, so the prefix scan
// with no delimiter returns every revision of every snapshot id and the ids are then deduplicated client-side.  With a
// delimiter the service returns one folder per id instead, which is all that listing the ids needs.
func TestB2ListSnapshotsListsOnlyDirectChildren(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	server := newB2TestServer([]string{
		"chunks/00/0000000000000000000000000000000000000000000000000000000000000000",
		"snapshots/vm1@host1/1",
		"snapshots/vm1@host1/2",
		"snapshots/vm2@host2/1",
	})
	defer server.Close()

	storage := server.createStorage()

	files, _, err := storage.ListFiles(0, "snapshots")
	if err != nil {
		t.Errorf("Failed to list the snapshot ids: %v", err)
		return
	}

	sort.Strings(files)
	if len(files) != 2 || files[0] != "vm1@host1/" || files[1] != "vm2@host2/" {
		t.Errorf("Listing the snapshot ids returned %v instead of the two snapshot ids", files)
	}

	if len(server.requests) != 1 {
		t.Errorf("Listing the snapshot ids made %d requests instead of 1", len(server.requests))
		return
	}
	request := server.requests[0]
	if request.delimiter != "/" {
		t.Errorf("Listing the snapshot ids should break the names at '/', got delimiter %q", request.delimiter)
	}
	if request.prefix != "snapshots/" {
		t.Errorf("Listing the snapshot ids should be restricted to the snapshots directory, got prefix %q", request.prefix)
	}
	if len(server.returned) != 2 {
		t.Errorf("Listing the snapshot ids should be answered with one name per id, got %d: %v",
			len(server.returned), server.returned)
	}

	// The revisions of a single id are still listed normally.
	files, _, err = storage.ListFiles(0, "snapshots/vm1@host1")
	if err != nil {
		t.Errorf("Failed to list the revisions of vm1@host1: %v", err)
		return
	}

	sort.Strings(files)
	if len(files) != 2 || files[0] != "1" || files[1] != "2" {
		t.Errorf("Listing the revisions of vm1@host1 returned %v instead of revisions 1 and 2", files)
	}
}

// azureListRequest records what an Azure listing asked the service for.
type azureListRequest struct {
	prefix    string
	delimiter string
}

// azureTestTransport answers the Azure list blobs request with the same prefix/delimiter behaviour that the real
// service provides, and records what was asked of it.
type azureTestTransport struct {
	files    []string
	requests []azureListRequest
	returned []string
}

func (transport *azureTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {

	query := request.URL.Query()
	transport.requests = append(transport.requests, azureListRequest{query.Get("prefix"), query.Get("delimiter")})
	transport.returned = prefixNames(transport.files, query.Get("prefix"), query.Get("delimiter"))

	body := `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><NextMarker></NextMarker><Blobs>`
	for _, name := range transport.returned {
		if strings.HasSuffix(name, "/") {
			body += "<BlobPrefix><Name>" + name + "</Name></BlobPrefix>"
		} else {
			body += "<Blob><Name>" + name + "</Name><Properties><Content-Length>7</Content-Length></Properties></Blob>"
		}
	}
	body += "</Blobs></EnumerationResults>"

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       ioutil.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

// Azure behaves like B2 here: the snapshot directory used to be listed as a flat prefix, so every snapshot file of
// every id was returned and the ids were deduplicated client-side.  The delimiter makes the service return the ids
// themselves.
func TestAzureListSnapshotsListsOnlyDirectChildren(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	transport := &azureTestTransport{files: []string{
		"snapshots/vm1@host1/1",
		"snapshots/vm1@host1/2",
		"snapshots/vm2@host2/1",
	}}

	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	client, err := azurestorage.NewClient("testaccount", key, "core.windows.net", azurestorage.DefaultAPIVersion, false)
	if err != nil {
		t.Errorf("Failed to create the azure client: %v", err)
		return
	}
	client.HTTPClient = &http.Client{Transport: transport}

	blobService := client.GetBlobService()
	container := blobService.GetContainerReference("testcontainer")

	storage := &AzureStorage{containers: []*azurestorage.Container{container}}

	files, _, err := storage.ListFiles(0, "snapshots/")
	if err != nil {
		t.Errorf("Failed to list the snapshot ids: %v", err)
		return
	}

	sort.Strings(files)
	if len(files) != 2 || files[0] != "vm1@host1/" || files[1] != "vm2@host2/" {
		t.Errorf("Listing the snapshot ids returned %v instead of the two snapshot ids", files)
	}

	if len(transport.requests) != 1 {
		t.Errorf("Listing the snapshot ids made %d requests instead of 1", len(transport.requests))
		return
	}
	request := transport.requests[0]
	if request.delimiter != "/" {
		t.Errorf("Listing the snapshot ids should break the names at '/', got delimiter %q", request.delimiter)
	}
	if request.prefix != "snapshots/" {
		t.Errorf("Listing the snapshot ids should be restricted to the snapshots directory, got prefix %q", request.prefix)
	}
	if len(transport.returned) != 2 {
		t.Errorf("Listing the snapshot ids should be answered with one name per id, got %d: %v",
			len(transport.returned), transport.returned)
	}

	// The revisions of a single id are still listed normally.
	files, _, err = storage.ListFiles(0, "snapshots/vm1@host1")
	if err != nil {
		t.Errorf("Failed to list the revisions of vm1@host1: %v", err)
		return
	}

	sort.Strings(files)
	if len(files) != 2 || files[0] != "1" || files[1] != "2" {
		t.Errorf("Listing the revisions of vm1@host1 returned %v instead of revisions 1 and 2", files)
	}
}
