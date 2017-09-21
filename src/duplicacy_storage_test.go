// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	crypto_rand "crypto/rand"
	"math/rand"
)

var testStorageName string
var testRateLimit int
var testQuickMode bool
var testThreads int
var testFixedChunkSize bool

func init() {
	flag.StringVar(&testStorageName, "storage", "", "the test storage to use")
	flag.IntVar(&testRateLimit, "limit-rate", 0, "maximum transfer speed in kbytes/sec")
	flag.BoolVar(&testQuickMode, "quick", false, "quick test")
	flag.IntVar(&testThreads, "threads", 1, "number of downloading/uploading threads")
	flag.BoolVar(&testFixedChunkSize, "fixed-chunk-size", false, "fixed chunk size")
	flag.Parse()
}

func loadStorage(localStoragePath string, threads int) (Storage, error) {

	if testStorageName == "" || testStorageName == "file" {
		return CreateFileStorage(localStoragePath, 2, false, threads)
	}

	config, err := ioutil.ReadFile("test_storage.conf")
	if err != nil {
		return nil, err
	}

	storages := make(map[string]map[string]string)

	err = json.Unmarshal(config, &storages)
	if err != nil {
		return nil, err
	}

	storage, found := storages[testStorageName]
	if !found {
		return nil, fmt.Errorf("No storage named '%s' found", testStorageName)
	}

	if testStorageName == "flat" {
		return CreateFileStorage(localStoragePath, 0, false, threads)
	} else if testStorageName == "samba" {
		return CreateFileStorage(localStoragePath, 2, true, threads)
	} else if testStorageName == "sftp" {
		port, _ := strconv.Atoi(storage["port"])
		return CreateSFTPStorageWithPassword(storage["server"], port, storage["username"], storage["directory"], storage["password"], threads)
	} else if testStorageName == "s3" || testStorageName == "wasabi" {
		return CreateS3Storage(storage["region"], storage["endpoint"], storage["bucket"], storage["directory"], storage["access_key"], storage["secret_key"], threads, true, false)
	} else if testStorageName == "s3c" {
		return CreateS3CStorage(storage["region"], storage["endpoint"], storage["bucket"], storage["directory"], storage["access_key"], storage["secret_key"], threads)
	} else if testStorageName == "minio" {
		return CreateS3Storage(storage["region"], storage["endpoint"], storage["bucket"], storage["directory"], storage["access_key"], storage["secret_key"], threads, false, true)
	} else if testStorageName == "minios" {
		return CreateS3Storage(storage["region"], storage["endpoint"], storage["bucket"], storage["directory"], storage["access_key"], storage["secret_key"], threads, true, true)
	} else if testStorageName == "dropbox" {
		return CreateDropboxStorage(storage["token"], storage["directory"], threads)
	} else if testStorageName == "b2" {
		return CreateB2Storage(storage["account"], storage["key"], storage["bucket"], threads)
	} else if testStorageName == "gcs-s3" {
		return CreateS3Storage(storage["region"], storage["endpoint"], storage["bucket"], storage["directory"], storage["access_key"], storage["secret_key"], threads, true, false)
	} else if testStorageName == "gcs" {
		return CreateGCSStorage(storage["token_file"], storage["bucket"], storage["directory"], threads)
	} else if testStorageName == "gcs-sa" {
		return CreateGCSStorage(storage["token_file"], storage["bucket"], storage["directory"], threads)
	} else if testStorageName == "azure" {
		return CreateAzureStorage(storage["account"], storage["key"], storage["container"], threads)
	} else if testStorageName == "acd" {
		return CreateACDStorage(storage["token_file"], storage["storage_path"], threads)
	} else if testStorageName == "gcd" {
		return CreateGCDStorage(storage["token_file"], storage["storage_path"], threads)
	} else if testStorageName == "one" {
		return CreateOneDriveStorage(storage["token_file"], storage["storage_path"], threads)
	} else if testStorageName == "hubic" {
		return CreateHubicStorage(storage["token_file"], storage["storage_path"], threads)
	} else {
		return nil, fmt.Errorf("Invalid storage named: %s", testStorageName)
	}
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

	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case Exception:
				t.Errorf("%s %s", e.LogID, e.Message)
				debug.PrintStack()
			default:
				t.Errorf("%v", e)
				debug.PrintStack()
			}
		}
	}()

	testDir := path.Join(os.TempDir(), "duplicacy_test", "storage_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	LOG_INFO("STORAGE_TEST", "storage: %s", testStorageName)

	storage, err := loadStorage(testDir, 1)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	storage.EnableTestMode()
	storage.SetRateLimits(testRateLimit, testRateLimit)

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

	numberOfFiles := 20
	maxFileSize := 64 * 1024

	if testQuickMode {
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
		LOG_INFO("STORAGE_CHUNK", "Uploaded chunk: %s, size: %d", chunkID, len(content))
	}

	allChunks := []string{}
	for _, file := range listChunks(storage) {
		file = strings.Replace(file, "/", "", -1)
		if len(file) == 64 {
			allChunks = append(allChunks, file)
		}
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
			LOG_INFO("STORAGE_CHUNK", "Downloaded chunk: %s, size: %d", chunkID, chunk.GetLength())
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
			t.Errorf("Failed to delete file %s: %v", filePath)
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

	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case Exception:
				t.Errorf("%s %s", e.LogID, e.Message)
				debug.PrintStack()
			default:
				t.Errorf("%v", e)
				debug.PrintStack()
			}
		}
	}()

	testDir := path.Join(os.TempDir(), "duplicacy_test", "storage_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	LOG_INFO("STORAGE_TEST", "storage: %s", testStorageName)

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

}
