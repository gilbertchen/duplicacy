// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"runtime/debug"
)

func createRandomFile(path string, maxSize int) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		LOG_ERROR("RANDOM_FILE", "Can't open %s for writing: %v", path, err)
		return
	}

	defer file.Close()

	size := maxSize/2 + rand.Int()%(maxSize/2)

	buffer := make([]byte, 32*1024)
	for size > 0 {
		bytes := size
		if bytes > cap(buffer) {
			bytes = cap(buffer)
		}
		crypto_rand.Read(buffer[:bytes])
		bytes, err = file.Write(buffer[:bytes])
		if err != nil {
			LOG_ERROR("RANDOM_FILE", "Failed to write to %s: %v", path, err)
			return
		}
		size -= bytes
	}
}

func modifyFile(path string, portion float32) {

	stat, err := os.Stat(path)
	if err != nil {
		LOG_ERROR("MODIFY_FILE", "Can't stat the file %s: %v", path, err)
		return
	}

	modifiedTime := stat.ModTime()

	file, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		LOG_ERROR("MODIFY_FILE", "Can't open %s for writing: %v", path, err)
		return
	}

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	size, err := file.Seek(0, 2)
	if err != nil {
		LOG_ERROR("MODIFY_FILE", "Can't seek to the end of the file %s: %v", path, err)
		return
	}

	length := int(float32(size) * portion)
	start := rand.Int() % (int(size) - length)

	_, err = file.Seek(int64(start), 0)
	if err != nil {
		LOG_ERROR("MODIFY_FILE", "Can't seek to the offset %d: %v", start, err)
		return
	}

	buffer := make([]byte, length)
	crypto_rand.Read(buffer)

	_, err = file.Write(buffer)
	if err != nil {
		LOG_ERROR("MODIFY_FILE", "Failed to write to %s: %v", path, err)
		return
	}

	file.Close()
	file = nil

	// Add 2 seconds to the modified time for the changes to be detectable in quick mode.
	modifiedTime = modifiedTime.Add(time.Second * 2)
	err = os.Chtimes(path, modifiedTime, modifiedTime)

	if err != nil {
		LOG_ERROR("MODIFY_FILE", "Failed to change the modification time of %s: %v", path, err)
		return
	}
}

func checkExistence(t *testing.T, path string, exists bool, isDir bool) {
	stat, err := os.Stat(path)
	if exists {
		if err != nil {
			t.Errorf("%s does not exist: %v", path, err)
		} else if isDir {
			if !stat.Mode().IsDir() {
				t.Errorf("%s is not a directory", path)
			}
		} else {
			if stat.Mode().IsDir() {
				t.Errorf("%s is not a file", path)
			}
		}
	} else {
		if err == nil || !os.IsNotExist(err) {
			t.Errorf("%s may exist: %v", path, err)
		}
	}
}

func truncateFile(path string) {
	file, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		LOG_ERROR("TRUNCATE_FILE", "Can't open %s for writing: %v", path, err)
		return
	}

	defer file.Close()

	oldSize, err := file.Seek(0, 2)
	if err != nil {
		LOG_ERROR("TRUNCATE_FILE", "Can't seek to the end of the file %s: %v", path, err)
		return
	}

	newSize := rand.Int63() % oldSize

	err = file.Truncate(newSize)
	if err != nil {
		LOG_ERROR("TRUNCATE_FILE", "Can't truncate the file %s to size %d: %v", path, newSize, err)
		return
	}
}

func getFileHash(path string) (hash string) {

	file, err := os.Open(path)
	if err != nil {
		LOG_ERROR("FILE_HASH", "Can't open %s for reading: %v", path, err)
		return ""
	}

	defer file.Close()

	hasher := sha256.New()
	_, err = io.Copy(hasher, file)
	if err != nil {
		LOG_ERROR("FILE_HASH", "Can't read file %s: %v", path, err)
		return ""
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func TestBackupManager(t *testing.T) {

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

	testDir := path.Join(os.TempDir(), "duplicacy_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	os.Mkdir(testDir+"/repository1", 0700)
	os.Mkdir(testDir+"/repository1/dir1", 0700)
	os.Mkdir(testDir+"/repository1/.duplicacy", 0700)
	os.Mkdir(testDir+"/repository2", 0700)
	os.Mkdir(testDir+"/repository2/.duplicacy", 0700)

	maxFileSize := 1000000
	//maxFileSize := 200000

	createRandomFile(testDir+"/repository1/file1", maxFileSize)
	createRandomFile(testDir+"/repository1/file2", maxFileSize)
	createRandomFile(testDir+"/repository1/dir1/file3", maxFileSize)

	threads := 1

	storage, err := loadStorage(testDir+"/storage", threads)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}

	delay := 0
	if _, ok := storage.(*ACDStorage); ok {
		delay = 1
	}
	if _, ok := storage.(*OneDriveStorage); ok {
		delay = 5
	}

	password := "duplicacy"

	cleanStorage(storage)

	time.Sleep(time.Duration(delay) * time.Second)
	if testFixedChunkSize {
		if !ConfigStorage(storage, 16384, 100, 64*1024, 64*1024, 64*1024, password, nil, false, "") {
			t.Errorf("Failed to initialize the storage")
		}
	} else {
		if !ConfigStorage(storage, 16384, 100, 64*1024, 256*1024, 16*1024, password, nil, false, "") {
			t.Errorf("Failed to initialize the storage")
		}
	}

	time.Sleep(time.Duration(delay) * time.Second)

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager := CreateBackupManager("host1", storage, testDir, password, "", "")
	backupManager.SetupSnapshotCache("default")

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "first", false, false, 0, false)
	time.Sleep(time.Duration(delay) * time.Second)
	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	backupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, false /*quickMode=*/, false, threads /*overwrite=*/, true,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)

	for _, f := range []string{"file1", "file2", "dir1/file3"} {
		if _, err := os.Stat(testDir + "/repository2/" + f); os.IsNotExist(err) {
			t.Errorf("File %s does not exist", f)
			continue
		}

		hash1 := getFileHash(testDir + "/repository1/" + f)
		hash2 := getFileHash(testDir + "/repository2/" + f)
		if hash1 != hash2 {
			t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
		}
	}

	modifyFile(testDir+"/repository1/file1", 0.1)
	modifyFile(testDir+"/repository1/file2", 0.2)
	modifyFile(testDir+"/repository1/dir1/file3", 0.3)

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "second", false, false, 0, false)
	time.Sleep(time.Duration(delay) * time.Second)
	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	backupManager.Restore(testDir+"/repository2", 2 /*inPlace=*/, true /*quickMode=*/, true, threads /*overwrite=*/, true,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)

	for _, f := range []string{"file1", "file2", "dir1/file3"} {
		hash1 := getFileHash(testDir + "/repository1/" + f)
		hash2 := getFileHash(testDir + "/repository2/" + f)
		if hash1 != hash2 {
			t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
		}
	}

	// Truncate file2 and add a few empty directories
	truncateFile(testDir + "/repository1/file2")
	os.Mkdir(testDir+"/repository1/dir2", 0700)
	os.Mkdir(testDir+"/repository1/dir2/dir3", 0700)
	os.Mkdir(testDir+"/repository1/dir4", 0700)
	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, false, threads, "third", false, false, 0, false)
	time.Sleep(time.Duration(delay) * time.Second)

	// Create some directories and files under repository2 that will be deleted during restore
	os.Mkdir(testDir+"/repository2/dir5", 0700)
	os.Mkdir(testDir+"/repository2/dir5/dir6", 0700)
	os.Mkdir(testDir+"/repository2/dir7", 0700)
	createRandomFile(testDir+"/repository2/file4", 100)
	createRandomFile(testDir+"/repository2/dir5/file5", 100)

	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	backupManager.Restore(testDir+"/repository2", 3 /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
		/*deleteMode=*/ true /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)

	for _, f := range []string{"file1", "file2", "dir1/file3"} {
		hash1 := getFileHash(testDir + "/repository1/" + f)
		hash2 := getFileHash(testDir + "/repository2/" + f)
		if hash1 != hash2 {
			t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
		}
	}

	// These files/dirs should not exist because deleteMode == true
	checkExistence(t, testDir+"/repository2/dir5", false, false)
	checkExistence(t, testDir+"/repository2/dir5/dir6", false, false)
	checkExistence(t, testDir+"/repository2/dir7", false, false)
	checkExistence(t, testDir+"/repository2/file4", false, false)
	checkExistence(t, testDir+"/repository2/dir5/file5", false, false)

	// These empty dirs should exist
	checkExistence(t, testDir+"/repository2/dir2", true, true)
	checkExistence(t, testDir+"/repository2/dir2/dir3", true, true)
	checkExistence(t, testDir+"/repository2/dir4", true, true)

	// Remove file2 and dir1/file3 and restore them from revision 3
	os.Remove(testDir + "/repository1/file2")
	os.Remove(testDir + "/repository1/dir1/file3")
	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager.Restore(testDir+"/repository1", 3 /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, []string{"+file2", "+dir1/file3", "-*"} /*allowFailures=*/, false)

	for _, f := range []string{"file1", "file2", "dir1/file3"} {
		hash1 := getFileHash(testDir + "/repository1/" + f)
		hash2 := getFileHash(testDir + "/repository2/" + f)
		if hash1 != hash2 {
			t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
		}
	}

	numberOfSnapshots := backupManager.SnapshotManager.ListSnapshots( /*snapshotID*/ "host1" /*revisionsToList*/, nil /*tag*/, "" /*showFiles*/, false /*showChunks*/, false)
	if numberOfSnapshots != 3 {
		t.Errorf("Expected 3 snapshots but got %d", numberOfSnapshots)
	}
	backupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1" /*revisions*/, []int{1, 2, 3} /*tag*/, "",
		/*showStatistics*/ false /*showTabular*/, false /*checkFiles*/, false /*checkChunks*/, false /*searchFossils*/, false /*resurrect*/, false, 1 /*allowFailures*/, false)
	backupManager.SnapshotManager.PruneSnapshots("host1", "host1" /*revisions*/, []int{1} /*tags*/, nil /*retentions*/, nil,
		/*exhaustive*/ false /*exclusive=*/, false /*ignoredIDs*/, nil /*dryRun*/, false /*deleteOnly*/, false /*collectOnly*/, false, 1)
	numberOfSnapshots = backupManager.SnapshotManager.ListSnapshots( /*snapshotID*/ "host1" /*revisionsToList*/, nil /*tag*/, "" /*showFiles*/, false /*showChunks*/, false)
	if numberOfSnapshots != 2 {
		t.Errorf("Expected 2 snapshots but got %d", numberOfSnapshots)
	}
	backupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1" /*revisions*/, []int{2, 3} /*tag*/, "",
		/*showStatistics*/ false /*showTabular*/, false /*checkFiles*/, false /*checkChunks*/, false /*searchFossils*/, false /*resurrect*/, false, 1 /*allowFailures*/, false)
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, false, threads, "fourth", false, false, 0, false)
	backupManager.SnapshotManager.PruneSnapshots("host1", "host1" /*revisions*/, nil /*tags*/, nil /*retentions*/, nil,
		/*exhaustive*/ false /*exclusive=*/, true /*ignoredIDs*/, nil /*dryRun*/, false /*deleteOnly*/, false /*collectOnly*/, false, 1)
	numberOfSnapshots = backupManager.SnapshotManager.ListSnapshots( /*snapshotID*/ "host1" /*revisionsToList*/, nil /*tag*/, "" /*showFiles*/, false /*showChunks*/, false)
	if numberOfSnapshots != 3 {
		t.Errorf("Expected 3 snapshots but got %d", numberOfSnapshots)
	}
	backupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1" /*revisions*/, []int{2, 3, 4} /*tag*/, "",
		/*showStatistics*/ false /*showTabular*/, false /*checkFiles*/, false /*checkChunks*/, false /*searchFossils*/, false /*resurrect*/, false, 1 /*allowFailures*/, false)

	/*buf := make([]byte, 1<<16)
	  runtime.Stack(buf, true)
	  fmt.Printf("%s", buf)*/
}

// Create file with random file with certain seed
func createRandomFileSeeded(path string, maxSize int, seed int64) {
	rand.Seed(seed)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		LOG_ERROR("RANDOM_FILE", "Can't open %s for writing: %v", path, err)
		return
	}

	defer file.Close()

	size := maxSize/2 + rand.Int()%(maxSize/2)

	buffer := make([]byte, 32*1024)
	for size > 0 {
		bytes := size
		if bytes > cap(buffer) {
			bytes = cap(buffer)
		}
		rand.Read(buffer[:bytes])
		bytes, err = file.Write(buffer[:bytes])
		if err != nil {
			LOG_ERROR("RANDOM_FILE", "Failed to write to %s: %v", path, err)
			return
		}
		size -= bytes
	}
}

func corruptFile(path string, start int, length int, seed int64) {
	rand.Seed(seed)

	file, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		LOG_ERROR("CORRUPT_FILE", "Can't open %s for writing: %v", path, err)
		return
	}

	defer func() {
		if file != nil {
			file.Close()
		}
	}()

	_, err = file.Seek(int64(start), 0)
	if err != nil {
		LOG_ERROR("CORRUPT_FILE", "Can't seek to the offset %d: %v", start, err)
		return
	}

	buffer := make([]byte, length)
	rand.Read(buffer)

	_, err = file.Write(buffer)
	if err != nil {
		LOG_ERROR("CORRUPT_FILE", "Failed to write to %s: %v", path, err)
		return
	}
}

func TestBackupManagerPersist(t *testing.T) {
	// We want deterministic output here so we can test the expected files are corrupted by missing or corrupt chunks
	// There use rand functions with fixed seed, and known keys

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

	testDir := path.Join(os.TempDir(), "duplicacy_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	os.Mkdir(testDir+"/repository1", 0700)
	os.Mkdir(testDir+"/repository1/dir1", 0700)
	os.Mkdir(testDir+"/repository1/.duplicacy", 0700)
	os.Mkdir(testDir+"/repository2", 0700)
	os.Mkdir(testDir+"/repository2/.duplicacy", 0700)
	os.Mkdir(testDir+"/repository3", 0700)
	os.Mkdir(testDir+"/repository3/.duplicacy", 0700)

	maxFileSize := 1000000
	//maxFileSize := 200000

	createRandomFileSeeded(testDir+"/repository1/file1", maxFileSize, 1)
	createRandomFileSeeded(testDir+"/repository1/file2", maxFileSize, 2)
	createRandomFileSeeded(testDir+"/repository1/dir1/file3", maxFileSize, 3)

	threads := 1

	password := "duplicacy"

	// We want deterministic output, plus ability to test encrypted storage
	// So make unencrypted storage with default keys, and encrypted as bit-identical copy of this but with password
	unencStorage, err := loadStorage(testDir+"/unenc_storage", threads)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	delay := 0
	if _, ok := unencStorage.(*ACDStorage); ok {
		delay = 1
	}
	if _, ok := unencStorage.(*OneDriveStorage); ok {
		delay = 5
	}

	time.Sleep(time.Duration(delay) * time.Second)
	cleanStorage(unencStorage)

	if !ConfigStorage(unencStorage, 16384, 100, 64*1024, 256*1024, 16*1024, "", nil, false, "") {
		t.Errorf("Failed to initialize the unencrypted storage")
	}
	time.Sleep(time.Duration(delay) * time.Second)
	unencConfig, _, err := DownloadConfig(unencStorage, "")
	if err != nil {
		t.Errorf("Failed to download storage config: %v", err)
		return
	}

	// Make encrypted storage
	storage, err := loadStorage(testDir+"/enc_storage", threads)
	if err != nil {
		t.Errorf("Failed to create encrypted storage: %v", err)
		return
	}
	time.Sleep(time.Duration(delay) * time.Second)
	cleanStorage(storage)

	if !ConfigStorage(storage, 16384, 100, 64*1024, 256*1024, 16*1024, password, unencConfig, true, "") {
		t.Errorf("Failed to initialize the encrypted storage")
	}
	time.Sleep(time.Duration(delay) * time.Second)

	// do unencrypted backup
	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	unencBackupManager := CreateBackupManager("host1", unencStorage, testDir, "", "", "")
	unencBackupManager.SetupSnapshotCache("default")

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	unencBackupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "first", false, false, 0, false)
	time.Sleep(time.Duration(delay) * time.Second)

	// do encrypted backup
	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	encBackupManager := CreateBackupManager("host1", storage, testDir, password, "", "")
	encBackupManager.SetupSnapshotCache("default")

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	encBackupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "first", false, false, 0, false)
	time.Sleep(time.Duration(delay) * time.Second)

	// check snapshots
	unencBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1" /*revisions*/, []int{1} /*tag*/, "",
		/*showStatistics*/ true /*showTabular*/, false /*checkFiles*/, true /*checkChunks*/, false,
		/*searchFossils*/ false /*resurrect*/, false, 1 /*allowFailures*/, false)

	encBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1" /*revisions*/, []int{1} /*tag*/, "",
		/*showStatistics*/ true /*showTabular*/, false /*checkFiles*/, true /*checkChunks*/, false,
		/*searchFossils*/ false /*resurrect*/, false, 1 /*allowFailures*/, false)

	// check functions
	checkAllUncorrupted := func(cmpRepository string) {
		for _, f := range []string{"file1", "file2", "dir1/file3"} {
			if _, err := os.Stat(testDir + cmpRepository + "/" + f); os.IsNotExist(err) {
				t.Errorf("File %s does not exist", f)
				continue
			}

			hash1 := getFileHash(testDir + "/repository1/" + f)
			hash2 := getFileHash(testDir + cmpRepository + "/" + f)
			if hash1 != hash2 {
				t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
			}
		}
	}
	checkMissingFile := func(cmpRepository string, expectMissing string) {
		for _, f := range []string{"file1", "file2", "dir1/file3"} {
			_, err := os.Stat(testDir + cmpRepository + "/" + f)
			if err == nil {
				if f == expectMissing {
					t.Errorf("File %s exists, expected to be missing", f)
				}
				continue
			}
			if os.IsNotExist(err) {
				if f != expectMissing {
					t.Errorf("File %s does not exist", f)
				}
				continue
			}

			hash1 := getFileHash(testDir + "/repository1/" + f)
			hash2 := getFileHash(testDir + cmpRepository + "/" + f)
			if hash1 != hash2 {
				t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
			}
		}
	}
	checkCorruptedFile := func(cmpRepository string, expectCorrupted string) {
		for _, f := range []string{"file1", "file2", "dir1/file3"} {
			if _, err := os.Stat(testDir + cmpRepository + "/" + f); os.IsNotExist(err) {
				t.Errorf("File %s does not exist", f)
				continue
			}

			hash1 := getFileHash(testDir + "/repository1/" + f)
			hash2 := getFileHash(testDir + cmpRepository + "/" + f)
			if f == expectCorrupted {
				if hash1 == hash2 {
					t.Errorf("File %s has same hashes, expected to be corrupted: %s vs %s", f, hash1, hash2)
				}

			} else {
				if hash1 != hash2 {
					t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
				}
			}
		}
	}

	// test restore all uncorrupted to repository3
	SetDuplicacyPreferencePath(testDir + "/repository3/.duplicacy")
	unencBackupManager.Restore(testDir+"/repository3", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
	checkAllUncorrupted("/repository3")

	// test for corrupt files and -persist
	// corrupt a chunk
	chunkToCorrupt1 := "/4d/538e5dfd2b08e782bfeb56d1360fb5d7eb9d8c4b2531cc2fca79efbaec910c"
	// this should affect file1
	chunkToCorrupt2 := "/2b/f953a766d0196ce026ae259e76e3c186a0e4bcd3ce10f1571d17f86f0a5497"
	// this should affect dir1/file3

	for i := 0; i < 2; i++ {
		if i == 0 {
			// test corrupt chunks
			corruptFile(testDir+"/unenc_storage"+"/chunks"+chunkToCorrupt1, 128, 128, 4)
			corruptFile(testDir+"/enc_storage"+"/chunks"+chunkToCorrupt2, 128, 128, 4)
		} else {
			// test missing chunks
			os.Remove(testDir + "/unenc_storage" + "/chunks" + chunkToCorrupt1)
			os.Remove(testDir + "/enc_storage" + "/chunks" + chunkToCorrupt2)
		}

		// check snapshots with --persist (allowFailures == true)
		// this would cause a panic and os.Exit from duplicacy_log if allowFailures == false
		unencBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1" /*revisions*/, []int{1} /*tag*/, "",
			/*showStatistics*/ true /*showTabular*/, false /*checkFiles*/, true /*checkChunks*/, false,
			/*searchFossils*/ false /*resurrect*/, false, 1 /*allowFailures*/, true)

		encBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1" /*revisions*/, []int{1} /*tag*/, "",
			/*showStatistics*/ true /*showTabular*/, false /*checkFiles*/, true /*checkChunks*/, false,
			/*searchFossils*/ false /*resurrect*/, false, 1 /*allowFailures*/, true)

		// test restore corrupted, inPlace = true, corrupted files will have hash failures
		os.RemoveAll(testDir + "/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		unencBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)

		// check restore, expect file1 to be corrupted
		checkCorruptedFile("/repository2", "file1")

		os.RemoveAll(testDir + "/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		encBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)

		// check restore, expect file3 to be corrupted
		checkCorruptedFile("/repository2", "dir1/file3")

		//SetLoggingLevel(DEBUG)
		// test restore corrupted, inPlace = false, corrupted files will be missing
		os.RemoveAll(testDir + "/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		unencBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, false /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)

		// check restore, expect file1 to be corrupted
		checkMissingFile("/repository2", "file1")

		os.RemoveAll(testDir + "/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		encBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, false /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)

		// check restore, expect file3 to be corrupted
		checkMissingFile("/repository2", "dir1/file3")

		// test restore corrupted files from different backups, inPlace = true
		// with overwrite=true, corrupted file1 from unenc will be restored correctly from enc
		// the latter will not touch the existing file3 with correct hash
		os.RemoveAll(testDir + "/repository2")
		unencBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		encBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		checkAllUncorrupted("/repository2")

		// restore to repository3, with overwrite and allowFailures (true/false), quickMode = false (use hashes)
		// should always succeed as uncorrupted files already exist with correct hash, so these will be ignored
		SetDuplicacyPreferencePath(testDir + "/repository3/.duplicacy")
		unencBackupManager.Restore(testDir+"/repository3", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
		checkAllUncorrupted("/repository3")

		unencBackupManager.Restore(testDir+"/repository3", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		checkAllUncorrupted("/repository3")
	}

}
