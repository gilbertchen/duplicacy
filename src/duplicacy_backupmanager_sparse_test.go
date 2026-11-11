// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

//go:build !windows

package duplicacy

import (
	"os"
	"path"
	"runtime"
	"syscall"
	"testing"
)

// A large in-place target that does not exist is created as a sparse file, which is what keeps a restore from writing
// the hole as zeroes.  The absence is what the sparse-file branch is reached by, and the caller's stat is now the only
// thing that establishes it, since the probe that used to fail with ENOENT is skipped for a known-absent file.  The
// file is over the 100 MB threshold with its data at the end, so the hole is what the block count measures: restoring
// without the sparse file leaves every 4 KB block of the hole allocated.
func TestInPlaceRestoreOfAnAbsentLargeFileStaysSparse(t *testing.T) {

	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	if runtime.GOOS == "darwin" {
		t.Skip("macOS has no sparse file support")
	}

	testDir := path.Join(os.TempDir(), "duplicacy_test", "inplace_sparse_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	os.Mkdir(testDir+"/repository1", 0700)
	os.Mkdir(testDir+"/repository1/.duplicacy", 0700)
	os.Mkdir(testDir+"/repository2", 0700)
	os.Mkdir(testDir+"/repository2/.duplicacy", 0700)

	threads := 1
	fileSize := int64(100*1024*1024) + 5*1024*1024
	dataSize := int64(1024 * 1024)

	// The data sits at the end of a hole just over 100 MB long, so the file is past the sparse-file threshold and the
	// hole is everything before the data.
	file, err := os.OpenFile(testDir+"/repository1/file1", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Errorf("Failed to create the source file: %v", err)
		return
	}
	if err := file.Truncate(fileSize); err != nil {
		t.Errorf("Failed to create the hole in the source file: %v", err)
		return
	}
	if _, err := file.WriteAt(make([]byte, dataSize), fileSize-dataSize); err != nil {
		t.Errorf("Failed to write the data at the end of the source file: %v", err)
		return
	}
	file.Close()

	storage, err := loadStorage(testDir+"/storage", threads)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	cleanStorage(storage)
	if !ConfigStorage(storage, 16384, 100, 1024*1024, 4*1024*1024, 256*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to initialize the storage")
		return
	}

	SetDuplicacyPreferencePath(testDir + "/repository1/.duplicacy")
	backupManager := CreateBackupManager("host1", storage, testDir, "", "", "", false)
	backupManager.SetupSnapshotCache("default")
	backupManager.Backup(testDir+"/repository1", false, threads, "first", false, false, 0, false, 1024, 1024)

	// repository2 is left untouched, so its target is absent and the restore takes the branch that creates the sparse
	// file.  quickMode is false because the file has to be written for there to be anything to measure.
	SetDuplicacyPreferencePath(testDir + "/repository2/.duplicacy")
	failedFiles := backupManager.Restore(testDir+"/repository2", 1 /*revision*/, true /*inPlace=*/, false, /*quickMode=*/
		threads, true /*overwrite=*/, false /*deleteMode=*/, false /*setOwner=*/, false /*showStatistics=*/, nil, false)
	assertRestoreFailures(t, failedFiles, 0)

	info, err := os.Stat(testDir + "/repository2/file1")
	if err != nil {
		t.Errorf("Failed to stat the restored file: %v", err)
		return
	}
	if info.Size() != fileSize {
		t.Errorf("The restored file is %d bytes instead of %d", info.Size(), fileSize)
		return
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skipf("This test needs the block count from syscall.Stat_t, which %T does not provide", info.Sys())
		return
	}

	// The hole must not be allocated.  A restore that wrote it allocates one 512-byte block per 4 KB of the hole,
	// which is this fixture's difference by three orders of magnitude: a few megabytes with the sparse file against
	// the full 105 MB without it.  The bound is 8 MB, clear of both.
	allocated := stat.Blocks * 512
	if allocated > 8*1024*1024 {
		t.Errorf("The restored file has %d bytes allocated, so the hole was written rather than left sparse",
			allocated)
	}
}
