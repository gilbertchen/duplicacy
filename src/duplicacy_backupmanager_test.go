// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	crypto_rand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

func assertRestoreFailures(t *testing.T, failedFiles int, expectedFailedFiles int) {
	if failedFiles != expectedFailedFiles {
		t.Errorf("Failed to restore %d instead of %d file(s)", failedFiles, expectedFailedFiles)
	}
}

func TestBackupManager(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

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

	dataShards := 0
	parityShards := 0
	if *testErasureCoding {
		dataShards = 5
		parityShards = 2
	}

	if *testFixedChunkSize {
		if !ConfigStorage(storage, 16384, 100, 64*1024, 64*1024, 64*1024, password, nil, false, "", dataShards, parityShards) {
			t.Errorf("Failed to initialize the storage")
		}
	} else {
		if !ConfigStorage(storage, 16384, 100, 64*1024, 256*1024, 16*1024, password, nil, false, "", dataShards, parityShards) {
			t.Errorf("Failed to initialize the storage")
		}
	}

	time.Sleep(time.Duration(delay) * time.Second)

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager := CreateBackupManager("host1", storage, testDir, password, "", "", false)
	backupManager.SetupSnapshotCache("default")

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "first", false, false, 0, false, 1024, 1024)
	time.Sleep(time.Duration(delay) * time.Second)
	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	failedFiles := backupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, false /*quickMode=*/, false, threads /*overwrite=*/, true,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
	assertRestoreFailures(t, failedFiles, 0)

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
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "second", false, false, 0, false, 1024, 1024)
	time.Sleep(time.Duration(delay) * time.Second)
	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	failedFiles = backupManager.Restore(testDir+"/repository2", 2 /*inPlace=*/, true /*quickMode=*/, true, threads /*overwrite=*/, true,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
	assertRestoreFailures(t, failedFiles, 0)

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
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, false, threads, "third", false, false, 0, false, 1024, 1024)
	time.Sleep(time.Duration(delay) * time.Second)

	// Create some directories and files under repository2 that will be deleted during restore
	os.Mkdir(testDir+"/repository2/dir5", 0700)
	os.Mkdir(testDir+"/repository2/dir5/dir6", 0700)
	os.Mkdir(testDir+"/repository2/dir7", 0700)
	createRandomFile(testDir+"/repository2/file4", 100)
	createRandomFile(testDir+"/repository2/dir5/file5", 100)

	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	failedFiles = backupManager.Restore(testDir+"/repository2", 3 /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
		/*deleteMode=*/ true /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
	assertRestoreFailures(t, failedFiles, 0)

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
	failedFiles = backupManager.Restore(testDir+"/repository1", 3 /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, []string{"+file2", "+dir1/file3", "-*"} /*allowFailures=*/, false)
	assertRestoreFailures(t, failedFiles, 0)

	for _, f := range []string{"file1", "file2", "dir1/file3"} {
		hash1 := getFileHash(testDir + "/repository1/" + f)
		hash2 := getFileHash(testDir + "/repository2/" + f)
		if hash1 != hash2 {
			t.Errorf("File %s has different hashes: %s vs %s", f, hash1, hash2)
		}
	}

	numberOfSnapshots := backupManager.SnapshotManager.ListSnapshots( /*snapshotID*/ "host1" /*revisionsToList*/, nil /*tag*/, "" /*showFiles*/, false /*showChunks*/, false, 1)
	if numberOfSnapshots != 3 {
		t.Errorf("Expected 3 snapshots but got %d", numberOfSnapshots)
	}
			
	backupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1", /*revisions*/ []int{1, 2, 3}, /*tag*/ "", /*showStatistics*/ false,
	    /*showTabular*/ false, /*checkFiles*/ false, /*checkChunks*/ false, /*searchFossils*/ false, /*resurrect*/ false, /*rewiret*/ false, 1,  /*allowFailures*/false)
	backupManager.SnapshotManager.PruneSnapshots("host1", "host1" /*revisions*/, []int{1} /*tags*/, nil /*retentions*/, nil,
		/*exhaustive*/ false /*exclusive=*/, false /*ignoredIDs*/, nil /*dryRun*/, false /*deleteOnly*/, false /*collectOnly*/, false, 1)
	numberOfSnapshots = backupManager.SnapshotManager.ListSnapshots( /*snapshotID*/ "host1" /*revisionsToList*/, nil /*tag*/, "" /*showFiles*/, false /*showChunks*/, false, 1)
	if numberOfSnapshots != 2 {
		t.Errorf("Expected 2 snapshots but got %d", numberOfSnapshots)
	}
	backupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1", /*revisions*/ []int{2, 3}, /*tag*/ "", /*showStatistics*/ false,
	     /*showTabular*/ false, /*checkFiles*/ false, /*checkChunks*/ false, /*searchFossils*/ false, /*resurrect*/ false, /*rewiret*/ false, 1, /*allowFailures*/ false)
	backupManager.Backup(testDir+"/repository1" /*quickMode=*/, false, threads, "fourth", false, false, 0, false, 1024, 1024)
	backupManager.SnapshotManager.PruneSnapshots("host1", "host1" /*revisions*/, nil /*tags*/, nil /*retentions*/, nil,
		/*exhaustive*/ false /*exclusive=*/, true /*ignoredIDs*/, nil /*dryRun*/, false /*deleteOnly*/, false /*collectOnly*/, false, 1)
	numberOfSnapshots = backupManager.SnapshotManager.ListSnapshots( /*snapshotID*/ "host1" /*revisionsToList*/, nil /*tag*/, "" /*showFiles*/, false /*showChunks*/, false, 1)
	if numberOfSnapshots != 3 {
		t.Errorf("Expected 3 snapshots but got %d", numberOfSnapshots)
	}
	backupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1", /*revisions*/ []int{2, 3, 4}, /*tag*/ "", /*showStatistics*/ false,
	    /*showTabular*/ false, /*checkFiles*/ false, /*checkChunks*/ false, /*searchFossils*/ false, /*resurrect*/ false, /*rewiret*/ false, 1, /*allowFailures*/ false)

	/*buf := make([]byte, 1<<16)
	  runtime.Stack(buf, true)
	  fmt.Printf("%s", buf)*/
}

func corruptFile(path string, start int, length int, seed int64) {
	rng := rand.New(rand.NewSource(seed))

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
	rng.Read(buffer)

	_, err = file.Write(buffer)
	if err != nil {
		LOG_ERROR("CORRUPT_FILE", "Failed to write to %s: %v", path, err)
		return
	}
}

func TestPersistRestore(t *testing.T) {
	// We want deterministic output here so we can test the expected files are corrupted by missing or corrupt chunks
	// There use rand functions with fixed seed, and known keys

	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

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

	createRandomFileSeeded(testDir+"/repository1/file1", maxFileSize,1)
	createRandomFileSeeded(testDir+"/repository1/file2", maxFileSize,2)
	createRandomFileSeeded(testDir+"/repository1/dir1/file3", maxFileSize,3)

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

	if !ConfigStorage(unencStorage, 16384, 100, 64*1024, 256*1024, 16*1024, "", nil, false, "", 0, 0) {
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

	if !ConfigStorage(storage, 16384, 100, 64*1024, 256*1024, 16*1024, password, unencConfig, true, "", 0, 0) {
		t.Errorf("Failed to initialize the encrypted storage")
	}
	time.Sleep(time.Duration(delay) * time.Second)

	// do unencrypted backup
	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	unencBackupManager := CreateBackupManager("host1", unencStorage, testDir, "", "", "", false)
	unencBackupManager.SetupSnapshotCache("default")

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	unencBackupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "first", false, false, 0, false, 1024, 1024)
	time.Sleep(time.Duration(delay) * time.Second)


	// do encrypted backup
	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	encBackupManager := CreateBackupManager("host1", storage, testDir, password, "", "", false)
	encBackupManager.SetupSnapshotCache("default")

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	encBackupManager.Backup(testDir+"/repository1" /*quickMode=*/, true, threads, "first", false, false, 0, false, 1024, 1024)
	time.Sleep(time.Duration(delay) * time.Second)


	// check snapshots
	unencBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1", /*revisions*/ []int{1}, /*tag*/ "",
		/*showStatistics*/ true, /*showTabular*/ false, /*checkFiles*/ true, /*checkChunks*/ false,
		/*searchFossils*/ false, /*resurrect*/ false, /*rewiret*/ false, 1, /*allowFailures*/ false)

	encBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1", /*revisions*/ []int{1}, /*tag*/ "",
		/*showStatistics*/ true, /*showTabular*/ false, /*checkFiles*/ true, /*checkChunks*/ false,
		 /*searchFossils*/ false, /*resurrect*/ false, /*rewiret*/ false, 1, /*allowFailures*/ false)
		
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
				if err==nil {
					if f==expectMissing {
						t.Errorf("File %s exists, expected to be missing", f)
					}
					continue
				}
				if os.IsNotExist(err) {
					if f!=expectMissing {
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
	checkCorruptedFile  := func(cmpRepository string, expectCorrupted string) {
		for _, f := range []string{"file1", "file2", "dir1/file3"} {
			if _, err := os.Stat(testDir + cmpRepository + "/" + f); os.IsNotExist(err) {
				t.Errorf("File %s does not exist", f)
				continue
			}
	
			hash1 := getFileHash(testDir + "/repository1/" + f)
			hash2 := getFileHash(testDir + cmpRepository + "/" + f)
			if (f==expectCorrupted) {
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
	failedFiles := unencBackupManager.Restore(testDir+"/repository3", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
		 /*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
	assertRestoreFailures(t, failedFiles, 0)
	checkAllUncorrupted("/repository3")

	// test for corrupt files and -persist
	// corrupt a chunk 
	chunkToCorrupt1 := "/4d/538e5dfd2b08e782bfeb56d1360fb5d7eb9d8c4b2531cc2fca79efbaec910c"
		// this should affect file1
	chunkToCorrupt2 := "/2b/f953a766d0196ce026ae259e76e3c186a0e4bcd3ce10f1571d17f86f0a5497"
		// this should affect dir1/file3
	
	for i := 0; i < 2; i++ {
		if i==0 {
			// test corrupt chunks
			corruptFile(testDir+"/unenc_storage"+"/chunks"+chunkToCorrupt1, 128, 128, 4)
			corruptFile(testDir+"/enc_storage"+"/chunks"+chunkToCorrupt2, 128, 128, 4)
		} else {
			// test missing chunks
			os.Remove(testDir+"/unenc_storage"+"/chunks"+chunkToCorrupt1)
			os.Remove(testDir+"/enc_storage"+"/chunks"+chunkToCorrupt2)
		}

		// This is to make sure that allowFailures is set to true.  Note that this is not needed
		// in the production code because chunkOperator can be only recreated multiple time in tests.
		if unencBackupManager.SnapshotManager.chunkOperator != nil {
			unencBackupManager.SnapshotManager.chunkOperator.allowFailures = true
		}

		if encBackupManager.SnapshotManager.chunkOperator != nil {
			encBackupManager.SnapshotManager.chunkOperator.allowFailures = true
		}

		// check snapshots with --persist (allowFailures == true)
		// this would cause a panic and os.Exit from duplicacy_log if allowFailures == false
		unencBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1", /*revisions*/ []int{1}, /*tag*/ "",
			/*showStatistics*/ true, /*showTabular*/ false, /*checkFiles*/ true, /*checkChunks*/ false,
			/*searchFossils*/ false, /*resurrect*/ false, /*rewrite*/ false, 1, /*allowFailures*/ true)

		encBackupManager.SnapshotManager.CheckSnapshots( /*snapshotID*/ "host1", /*revisions*/ []int{1}, /*tag*/ "",
			/*showStatistics*/ true, /*showTabular*/ false, /*checkFiles*/ true, /*checkChunks*/ false,
			/*searchFossils*/ false, /*resurrect*/ false, /*rewrite*/ false, 1, /*allowFailures*/ true)

	
		// test restore corrupted, inPlace = true, corrupted files will have hash failures
		os.RemoveAll(testDir+"/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		failedFiles = unencBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		assertRestoreFailures(t, failedFiles, 1)

		// check restore, expect file1 to be corrupted
		checkCorruptedFile("/repository2", "file1")

		
		os.RemoveAll(testDir+"/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		failedFiles = encBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		assertRestoreFailures(t, failedFiles, 1)

		// check restore, expect file3 to be corrupted
		checkCorruptedFile("/repository2", "dir1/file3")

		//SetLoggingLevel(DEBUG)
		// test restore corrupted, inPlace = false, corrupted files will be missing
		os.RemoveAll(testDir+"/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		failedFiles = unencBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, false /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		assertRestoreFailures(t, failedFiles, 1)

		// check restore, expect file1 to be corrupted
		checkMissingFile("/repository2", "file1")

		
		os.RemoveAll(testDir+"/repository2")
		SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
		failedFiles = encBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, false /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		assertRestoreFailures(t, failedFiles, 1)

		// check restore, expect file3 to be corrupted
		checkMissingFile("/repository2", "dir1/file3")

		// test restore corrupted files from different backups, inPlace = true
		// with overwrite=true, corrupted file1 from unenc will be restored correctly from enc
		// the latter will not touch the existing file3 with correct hash
		os.RemoveAll(testDir+"/repository2")
		failedFiles = unencBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, false,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		assertRestoreFailures(t, failedFiles, 1)

		failedFiles = encBackupManager.Restore(testDir+"/repository2", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		assertRestoreFailures(t, failedFiles, 0)
		checkAllUncorrupted("/repository2")

		// restore to repository3, with overwrite and allowFailures (true/false), quickMode = false (use hashes)
		// should always succeed as uncorrupted files already exist with correct hash, so these will be ignored
		SetDuplicacyPreferencePath(testDir + "/repository3/.duplicacy")
		failedFiles = unencBackupManager.Restore(testDir+"/repository3", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
		assertRestoreFailures(t, failedFiles, 0)
		checkAllUncorrupted("/repository3")

		failedFiles = unencBackupManager.Restore(testDir+"/repository3", threads /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
			/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, true)
		assertRestoreFailures(t, failedFiles, 0)
		checkAllUncorrupted("/repository3")
	}

}

// The chunk cache holds a copy of metadata chunks that remain in the storage, so writing it does not need to flush
// to disk: a torn entry fails the id check on the next read and is re-fetched from the storage.  The snapshot files,
// which are read back without verification, must keep fsyncing.  What must not change is that the cache is still
// written and still read back, since it is what stops a metadata chunk being fetched again.
func TestSnapshotCacheSkipsSync(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_cache_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	threads := 1
	repository := path.Join(testDir, "repository")
	os.MkdirAll(path.Join(repository, ".duplicacy"), 0700)
	createRandomFile(path.Join(repository, "file1"), 300000)

	storageDir := path.Join(testDir, "storage")
	innerStorage, err := loadStorage(storageDir, threads)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	if !ConfigStorage(innerStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to configure the storage")
		return
	}

	// A storage that needs a cache is the one whose cache is written and read back; a plain local storage never
	// caches, so the cache would stay empty and the test would prove nothing.
	fileStorage, ok := innerStorage.(*FileStorage)
	if !ok {
		t.Skipf("This test requires a file storage, got %T", innerStorage)
		return
	}
	fileStorage.isCacheNeeded = true

	SetDuplicacyPreferencePath(path.Join(repository, ".duplicacy"))
	manager := CreateBackupManager("host1", innerStorage, repository, "", "", "", false)
	if !manager.SetupSnapshotCache("default") {
		t.Errorf("Failed to set up the snapshot cache")
		return
	}

	cache := manager.snapshotCache

	if !manager.Backup(repository, true, threads, "first", false, false, 0, false, 1024, 1024) {
		t.Errorf("Failed to back the repository up")
		return
	}

	// Writing the cache must still leave the cached files in place and readable.
	cachedSnapshots, _ := manager.SnapshotManager.ListAllFiles(cache, "snapshots/")
	if len(cachedSnapshots) == 0 {
		t.Errorf("The snapshot file was not written to the cache")
		return
	}

	chunk := CreateChunk(manager.config, true)
	for _, cachedSnapshot := range cachedSnapshots {
		if strings.HasSuffix(cachedSnapshot, "/") {
			continue
		}
		chunk.Reset(false)
		if err := cache.DownloadFile(0, path.Join("snapshots", cachedSnapshot), chunk); err != nil {
			t.Errorf("Failed to read the cached snapshot file %s back: %v", cachedSnapshot, err)
			return
		}
		if _, err := CreateSnapshotFromDescription(chunk.GetBytes()); err != nil {
			t.Errorf("The cached snapshot file %s is not a valid snapshot: %v", cachedSnapshot, err)
			return
		}
	}
}

// A cache entry written without fsync may be lost or left torn by a crash.  The chunk cache tolerates this because
// the reader verifies what it finds: it re-derives the chunk id and falls back to the storage on a mismatch.  This
// test pins that behaviour down, since it is what makes skipping the fsync safe, and it also checks that the entry
// is not merely bypassed but rebuilt, so that later revisions of a snapshot still get a cache hit.
func TestCorruptCachedChunkIsRefetched(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "corrupt_cache_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	threads := 1
	repository := path.Join(testDir, "repository")
	os.MkdirAll(path.Join(repository, ".duplicacy"), 0700)
	createRandomFile(path.Join(repository, "file1"), 300000)

	innerStorage, err := loadStorage(path.Join(testDir, "storage"), threads)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	if !ConfigStorage(innerStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to configure the storage")
		return
	}
	fileStorage, ok := innerStorage.(*FileStorage)
	if !ok {
		t.Skipf("This test requires a file storage, got %T", innerStorage)
		return
	}
	fileStorage.isCacheNeeded = true

	SetDuplicacyPreferencePath(path.Join(repository, ".duplicacy"))
	manager := CreateBackupManager("host1", innerStorage, repository, "", "", "", false)
	if !manager.SetupSnapshotCache("default") {
		t.Errorf("Failed to set up the snapshot cache")
		return
	}

	if !manager.Backup(repository, true, threads, "first", false, false, 0, false, 1024, 1024) {
		t.Errorf("Failed to back the repository up")
		return
	}

	cache := manager.snapshotCache
	chunkOperator := CreateChunkOperator(manager.config, innerStorage, cache, false, false, threads, false)
	defer chunkOperator.Stop()

	// Upload a metadata chunk with a known hash and download it, so that the cache holds a copy of it.
	content := []byte("a metadata chunk for the cache to hold")
	chunkHash := uploadTestMetadataChunk(manager.SnapshotManager, content)
	if chunkHash == "" {
		t.Errorf("Failed to upload a metadata chunk")
		return
	}
	chunkID := manager.config.GetChunkIDFromHash(chunkHash)

	if downloaded := chunkOperator.Download(chunkHash, 0, true); downloaded == nil || downloaded.GetID() != chunkID {
		t.Errorf("Failed to download the metadata chunk %s", chunkID)
		return
	}

	cachedPath, exist, _, err := cache.FindChunk(0, chunkID, false)
	if err != nil || !exist {
		t.Errorf("The metadata chunk %s was not written to the cache (exist=%t, err=%v)", chunkID, exist, err)
		return
	}

	// Truncate the cached copy to simulate a write torn by a crash.
	if err := os.Truncate(path.Join(cache.storageDir, cachedPath), 10); err != nil {
		t.Errorf("Failed to truncate the cached chunk %s: %v", cachedPath, err)
		return
	}

	// The torn entry must be rejected and the chunk served from the storage instead; without the verification in
	// DownloadChunk this would return the truncated bytes.
	refetched := chunkOperator.Download(chunkHash, 0, true)
	if refetched == nil {
		t.Errorf("A torn cache entry for the chunk %s was not recovered from the storage", chunkID)
		return
	}
	if refetched.GetID() != chunkID {
		t.Errorf("The recovered chunk has id %s instead of %s", refetched.GetID(), chunkID)
	}
	if !bytes.Equal(refetched.GetBytes(), content) {
		t.Errorf("The recovered chunk does not hold the original content")
	}

	// The entry must also be repaired, not merely bypassed, so that the next revision still gets a cache hit rather
	// than fetching the chunk from the storage again.
	repaired := CreateChunk(manager.config, true)
	if err := cache.DownloadFile(0, cachedPath, repaired); err != nil {
		t.Errorf("Failed to read the cached chunk %s back: %v", cachedPath, err)
		return
	}
	if !bytes.Equal(repaired.GetBytes(), content) {
		t.Errorf("The cached chunk %s was not rewritten with the correct content", cachedPath)
	}

	// A crash can also leave the entry missing rather than torn, which is the state a write interrupted before the
	// rename produces; that must be recovered and re-cached just the same.
	if err := os.Remove(path.Join(cache.storageDir, cachedPath)); err != nil {
		t.Errorf("Failed to remove the cached chunk %s: %v", cachedPath, err)
		return
	}
	if recovered := chunkOperator.Download(chunkHash, 0, true); recovered == nil || !bytes.Equal(recovered.GetBytes(), content) {
		t.Errorf("The chunk %s was not recovered after its cache entry was removed", chunkID)
		return
	}
	if _, exist, _, err := cache.FindChunk(0, chunkID, false); err != nil || !exist {
		t.Errorf("The cache entry for the chunk %s was not rebuilt after it was removed (exist=%t, err=%v)",
			chunkID, exist, err)
	}
}

// The in-place restore never creates the temporary file, so the deferred cleanup that removes it is skipped when this
// call did not make one.  What must not change is that a non-in-place restore, which does create it, still removes it
// after a failure: the run is stopped by a missing chunk, and the file left under .duplicacy/temporary must be gone.
func TestRestoreCleansUpAfterAFailure(t *testing.T) {

	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "temporary_file_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	os.Mkdir(testDir+"/repository1", 0700)
	os.Mkdir(testDir+"/repository1/.duplicacy", 0700)
	os.Mkdir(testDir+"/repository2", 0700)
	os.Mkdir(testDir+"/repository2/.duplicacy", 0700)

	createRandomFileSeeded(testDir+"/repository1/file1", 500000, 1)

	threads := 1

	storage, err := loadStorage(testDir+"/storage", threads)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	cleanStorage(storage)
	// A fixed 64 KB maximum chunk size keeps the data chunks at a known size, well above the metadata chunks that
	// hold the sequences for a single file, so the largest chunk in chunks/ is the one holding the file's content.
	if !ConfigStorage(storage, 16384, 100, 64*1024, 64*1024, 64*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to initialize the storage")
		return
	}

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager := CreateBackupManager("host1", storage, testDir, "", "", "", false)
	backupManager.SetupSnapshotCache("default")
	backupManager.Backup(testDir+"/repository1", true, threads, "first", false, false, 0, false, 1024, 1024)

	// Removing the data chunk stops the restore with a per-file failure once the temporary file has been created,
	// which is the path that must still clean it up.  Only chunks/ is walked: the config and the snapshot file live
	// elsewhere in the storage, and removing one of those would fail the restore before it downloaded anything.
	dataChunk := ""
	dataChunkSize := int64(0)
	filepath.Walk(testDir+"/storage/chunks", func(p string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() && info.Size() > dataChunkSize {
			dataChunk, dataChunkSize = p, info.Size()
		}
		return nil
	})
	if dataChunk == "" {
		t.Errorf("The backup produced no chunk to remove")
		return
	}
	if err := os.Remove(dataChunk); err != nil {
		t.Errorf("Failed to remove the data chunk %s: %v", dataChunk, err)
		return
	}

	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	temporaryPath := testDir + "/repository2/.duplicacy/temporary"
	failedFiles := backupManager.Restore(testDir+"/repository2", 1 /*revision*/, false /*inPlace*/, false /*quickMode*/,
		threads, true /*overwrite*/, false /*deleteMode*/, false /*setOwner=*/, false /*showStatistics*/, nil, true)
	assertRestoreFailures(t, failedFiles, 1)

	if _, err := os.Stat(temporaryPath); !os.IsNotExist(err) {
		t.Errorf("The temporary file was left behind after a failed non-in-place restore: %v", err)
	}
}

// The in-place restore skips the final truncate for a file this call created, since the write loop has already left it
// the right length.  What must not change is the case the truncate is for: a target that was already there and is
// longer than the snapshot's copy, which only the truncate shortens.  The file is grown past the snapshot's size and
// restored in place over it, so a guard that skipped the truncate would leave the extra bytes behind.
func TestInPlaceRestoreStillTruncatesALongerTarget(t *testing.T) {

	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "inplace_truncate_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	os.Mkdir(testDir+"/repository1", 0700)
	os.Mkdir(testDir+"/repository1/.duplicacy", 0700)
	os.Mkdir(testDir+"/repository2", 0700)
	os.Mkdir(testDir+"/repository2/.duplicacy", 0700)

	threads := 1
	fileSize := 500000
	createRandomFileSeeded(testDir+"/repository1/file1", fileSize, 1)

	storage, err := loadStorage(testDir+"/storage", threads)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	cleanStorage(storage)
	if !ConfigStorage(storage, 16384, 100, 64*1024, 64*1024, 64*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to initialize the storage")
		return
	}

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager := CreateBackupManager("host1", storage, testDir, "", "", "", false)
	backupManager.SetupSnapshotCache("default")
	backupManager.Backup(testDir+"/repository1", true, threads, "first", false, false, 0, false, 1024, 1024)

	snapshotSize := fileSize / 2
	original, err := os.ReadFile(testDir + "/repository1/file1")
	if err != nil {
		t.Errorf("Failed to read the backed-up file: %v", err)
		return
	}
	// The target is longer than the copy the snapshot holds, which is the only shape that needs the truncate.  A
	// smaller target would be grown by the write loop and would leave the truncate untested either way.
	createRandomFileSeeded(testDir+"/repository2/file1", snapshotSize*3, 2)
	if info, err := os.Stat(testDir + "/repository2/file1"); err != nil || info.Size() <= int64(len(original)) {
		t.Errorf("The target was not made longer than the snapshot's copy (%v)", err)
		return
	}

	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	// The preference path is under the repository, so the forced in-place mode the CLI uses is what this passes
	// explicitly; a non-in-place restore renames a temporary file over the target and never truncates.
	failedFiles := backupManager.Restore(testDir+"/repository2", 1 /*inPlace=*/, true /*quickMode=*/, false, threads /*overwrite=*/, true,
		/*deleteMode=*/ false /*setowner=*/, false /*showStatistics=*/, false /*patterns=*/, nil /*allowFailures=*/, false)
	assertRestoreFailures(t, failedFiles, 0)

	restored, err := os.ReadFile(testDir + "/repository2/file1")
	if err != nil {
		t.Errorf("Failed to read the restored file: %v", err)
		return
	}
	if !bytes.Equal(restored, original) {
		t.Errorf("The restored file is %d bytes and does not match the %d bytes of the snapshot's copy",
			len(restored), len(original))
	}
}

// threadRecordingStorage records the thread count of the snapshot manager's chunk operator as each chunk is read
// through it, which is how TestBackupExpandsSequencesUnderThreads tells which operator the expansion ran on.  It
// assumes the manager it was given is the one issuing the reads, so it is used within a single test.
type threadRecordingStorage struct {
	*FileStorage

	manager *BackupManager

	lock    sync.Mutex
	threads map[int]bool
}

func (storage *threadRecordingStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) error {

	if strings.HasPrefix(filePath, "chunks/") {
		operator := storage.manager.SnapshotManager.chunkOperator
		if operator == nil {
			storage.lock.Lock()
			storage.threads[0] = true
			storage.lock.Unlock()
		} else {
			storage.lock.Lock()
			storage.threads[operator.threads] = true
			storage.lock.Unlock()
		}
	}

	return storage.FileStorage.DownloadFile(threadIndex, filePath, chunk)
}

func (storage *threadRecordingStorage) operatorThreadCounts() map[int]bool {
	storage.lock.Lock()
	defer storage.lock.Unlock()

	counts := make(map[int]bool)
	for threads := range storage.threads {
		counts[threads] = true
	}
	return counts
}

// A backup of an existing revision expands the previous revision's chunk and length sequences, and those go through
// the snapshot manager's own operator.  DownloadSequence creates that operator itself, but with a hard-coded single
// thread, so unless the caller has created it first the expansion runs one metadata chunk at a time however many
// -threads were given.  Backup now creates it first, the way check, prune and restore do, and stops it on the way
// out; this pins both halves.
func TestBackupExpandsSequencesUnderThreads(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "backup_threads_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	repository := path.Join(testDir, "repository")
	os.MkdirAll(path.Join(repository, ".duplicacy"), 0700)
	createRandomFileSeeded(path.Join(repository, "file1"), 300000, 5)

	innerStorage, err := loadStorage(path.Join(testDir, "storage"), 1)
	if err != nil {
		t.Errorf("Failed to create the storage: %v", err)
		return
	}
	fileStorage, ok := innerStorage.(*FileStorage)
	if !ok {
		t.Skipf("This test requires a file storage, got %T", innerStorage)
		return
	}
	if !ConfigStorage(innerStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to configure the storage")
		return
	}

	SetDuplicacyPreferencePath(path.Join(repository, ".duplicacy"))
	manager := CreateBackupManager("host1", innerStorage, repository, "", "", "", false)
	manager.SetupSnapshotCache("default")

	// The first backup has no previous revision, so there is nothing to expand and no operator should be created.
	if !manager.Backup(repository, true, 1, "first", false, false, 0, false, 1024, 1024) {
		t.Errorf("Failed to back the repository up")
		return
	}
	if manager.SnapshotManager.chunkOperator != nil {
		t.Errorf("A first backup left the snapshot manager's chunk operator behind")
	}

	// Every later backup expands the previous revision's sequences, so this is the run whose operator is observed.
	if err := os.WriteFile(path.Join(repository, "file2"), []byte("second"), 0644); err != nil {
		t.Errorf("Failed to add a file for the second backup: %v", err)
		return
	}

	recording := &threadRecordingStorage{FileStorage: fileStorage, manager: manager, threads: make(map[int]bool)}
	manager.SnapshotManager.storage = recording
	manager.storage = recording

	if !manager.Backup(repository, true, 8, "second", false, false, 0, false, 1024, 1024) {
		t.Errorf("Failed to back the repository up a second time")
		return
	}

	// The expansion must have run on an operator created with the user's thread count, not on the one-thread
	// operator DownloadSequence would have built for itself.
	if counts := recording.operatorThreadCounts(); len(counts) != 1 || !counts[8] {
		t.Errorf("Expected the sequences to be expanded on an eight-thread operator, saw %v", counts)
	}

	// And it must have been stopped on the way out, or the next command would inherit its thread count.
	if manager.SnapshotManager.chunkOperator != nil {
		t.Errorf("Backup left the snapshot manager's chunk operator running")
	}
}
