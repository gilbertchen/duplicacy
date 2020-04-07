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
var testRSAEncryption bool

func init() {
	flag.StringVar(&testStorageName, "storage", "", "the test storage to use")
	flag.IntVar(&testRateLimit, "limit-rate", 0, "maximum transfer speed in kbytes/sec")
	flag.BoolVar(&testQuickMode, "quick", false, "quick test")
	flag.IntVar(&testThreads, "threads", 1, "number of downloading/uploading threads")
	flag.BoolVar(&testFixedChunkSize, "fixed-chunk-size", false, "fixed chunk size")
	flag.BoolVar(&testRSAEncryption, "rsa", false, "enable RSA encryption")
	flag.Parse()
}

func loadStorage(localStoragePath string, threads int) (Storage, error) {

	if testStorageName == "" || testStorageName == "file" {
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

	config, found := configs[testStorageName]
	if !found {
		return nil, fmt.Errorf("No storage named '%s' found", testStorageName)
	}

	if testStorageName == "flat" {
		storage, err := CreateFileStorage(localStoragePath, false, threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "samba" {
		storage, err := CreateFileStorage(localStoragePath, true, threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "sftp" {
		port, _ := strconv.Atoi(config["port"])
		storage, err := CreateSFTPStorageWithPassword(config["server"], port, config["username"], config["directory"], 2, config["password"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "s3" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, false)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "wasabi" {
		storage, err := CreateWasabiStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "s3c" {
		storage, err := CreateS3CStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "digitalocean" {
		storage, err := CreateS3CStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "minio" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, false, true)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "minios" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, true)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "dropbox" {
		storage, err := CreateDropboxStorage(config["token"], config["directory"], 1, threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "b2" {
		storage, err := CreateB2Storage(config["account"], config["key"], "", config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "gcs-s3" {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, false)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "gcs" {
		storage, err := CreateGCSStorage(config["token_file"], config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "gcs-sa" {
		storage, err := CreateGCSStorage(config["token_file"], config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "azure" {
		storage, err := CreateAzureStorage(config["account"], config["key"], config["container"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "acd" {
		storage, err := CreateACDStorage(config["token_file"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "gcd" {
		storage, err := CreateGCDStorage(config["token_file"], "", config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "one" {
		storage, err := CreateOneDriveStorage(config["token_file"], false, config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "odb" {
		storage, err := CreateOneDriveStorage(config["token_file"], true, config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "one" {
		storage, err := CreateOneDriveStorage(config["token_file"], false, config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "hubic" {
		storage, err := CreateHubicStorage(config["token_file"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "memset" {
		storage, err := CreateSwiftStorage(config["storage_url"], config["key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	} else if testStorageName == "pcloud" || testStorageName == "box" {
		storage, err := CreateWebDAVStorage(config["host"], 0, config["username"], config["password"], config["storage_path"], false, threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	}
	return nil, fmt.Errorf("Invalid storage named: %s", testStorageName)
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

	threads := 8
	storage, err := loadStorage(testDir, threads)
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

	files, _, err := storage.ListFiles(0, "chunks/")
	for _, file := range files {
		if len(file) > 0 && file[len(file)-1] != '/' {
			LOG_DEBUG("FILE_EXIST", "File %s exists after deletion", file)
		}
	}

}
