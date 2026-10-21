// Copyright (c) 2026 Mark Landis <anonymouspage@limsei.com>
// This regression test is contributed to Duplicacy and its derivative works.

package duplicacy

import (
	"os"
	"path"
	"runtime/debug"
	"testing"
)

// TestRestoreReplacesStaleSymlink is a regression test for the symlink write-through (CWE-59): when a
// path is a symlink on disk but the snapshot has a regular file there, restore must remove the symlink
// rather than follow it (which would write the file's content through the symlink to its target,
// possibly outside the restore tree). Replacing the symlink requires -overwrite; without it the symlink
// is left untouched.
func TestRestoreReplacesStaleSymlink(t *testing.T) {
	setTestingT(t)
	SetLoggingLevel(INFO)

	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case Exception:
				t.Errorf("%s %s", e.LogID, e.Message)
			default:
				t.Errorf("%v", e)
			}
			debug.PrintStack()
		}
	}()

	testDir := path.Join(os.TempDir(), "duplicacy_test_symlink")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	repo := testDir + "/repository"
	os.MkdirAll(repo+"/.duplicacy", 0700)

	// Revision 1: a stable regular file plus a symlink.
	createRandomFile(repo+"/keep", 4096)
	if err := os.Symlink("dangling_target", repo+"/morph"); err != nil {
		t.Skipf("symlinks not supported on this platform: %v", err)
	}

	threads := 1
	storage, err := loadStorage(testDir+"/storage", threads)
	if err != nil {
		t.Errorf("Failed to create storage: %v", err)
		return
	}
	cleanStorage(storage)

	password := "duplicacy"
	if !ConfigStorage(storage, 16384, 100, 64*1024, 64*1024, 64*1024, password, nil, false, "", 0, 0) {
		t.Errorf("Failed to initialize the storage")
		return
	}

	SetDuplicacyPreferencePath(repo + "/.duplicacy")
	backupManager := CreateBackupManager("host1", storage, testDir, password, "", "", false)
	backupManager.SetupSnapshotCache("default")

	SetDuplicacyPreferencePath(repo + "/.duplicacy")
	backupManager.Backup(repo /*quickMode=*/, true, threads, "rev1", false, false, 0, false, 1024, 1024)

	// Revision 2: 'morph' is now a regular file at the same path.
	os.Remove(repo + "/morph")
	createRandomFile(repo+"/morph", 4096)
	SetDuplicacyPreferencePath(repo + "/.duplicacy")
	backupManager.Backup(repo /*quickMode=*/, true, threads, "rev2", false, false, 0, false, 1024, 1024)

	restore := func(target string, revision int, overwrite, allowFailures bool) int {
		os.MkdirAll(target+"/.duplicacy", 0700)
		SetDuplicacyPreferencePath(target + "/.duplicacy")
		return backupManager.Restore(target, revision /*inPlace=*/, true /*quickMode=*/, true, threads,
			overwrite /*deleteMode=*/, false /*setOwner=*/, false /*showStatistics=*/, false /*patterns=*/, nil, allowFailures)
	}
	lmode := func(p string) os.FileMode {
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatalf("Lstat %s: %v", p, err)
		}
		return fi.Mode()
	}

	// --- with -overwrite: the stale symlink is removed and replaced by the file. ---
	rOverwrite := testDir + "/restore_overwrite"
	restore(rOverwrite, 1, true, false) // lay down rev1 (symlink on disk)
	if lmode(rOverwrite+"/morph")&os.ModeSymlink == 0 {
		t.Fatalf("setup: restore_overwrite/morph should be a symlink after rev1")
	}
	restore(rOverwrite, 2, true /*overwrite*/, false)
	if !lmode(rOverwrite + "/morph").IsRegular() {
		t.Errorf("morph should be replaced by a regular file with -overwrite; got mode %v", lmode(rOverwrite+"/morph"))
	}
	if h1, h2 := getFileHash(repo+"/morph"), getFileHash(rOverwrite+"/morph"); h1 != h2 {
		t.Errorf("morph content mismatch after replacement: %s vs %s", h1, h2)
	}

	// --- without -overwrite: the symlink is left untouched (not followed). ---
	rNone := testDir + "/restore_none"
	restore(rNone, 1, true, false)
	restore(rNone, 2, false /*overwrite*/, true /*allowFailures*/)
	if lmode(rNone+"/morph")&os.ModeSymlink == 0 {
		t.Errorf("morph should remain a symlink without -overwrite; got mode %v", lmode(rNone+"/morph"))
	}
}
