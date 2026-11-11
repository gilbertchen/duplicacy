// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"os"
	"path"
	"testing"
)

// A plain local path must not keep a snapshot cache: the cache read is the same syscall as the storage read, and the
// write-back adds a temp file, an fsync and a rename on top.  Before this decision existed the flag was off for every
// local path, so this only pins the local half of it down; the remote half cannot be tested without a network mount.
func TestCreateStorageDoesNotCacheLocalPaths(t *testing.T) {

	setTestingT(t)
	defer recovering(t)

	localDir := path.Join(os.TempDir(), "duplicacy_test", "remotefs_local")
	os.RemoveAll(localDir)
	os.MkdirAll(localDir, 0700)
	defer os.RemoveAll(localDir)

	if storage := CreateStorage(Preference{Name: "local", StorageURL: localDir}, false, 1); storage == nil {
		t.Errorf("Failed to create a file storage at %s", localDir)
	} else if storage.IsCacheNeeded() {
		t.Errorf("A path on the local filesystem must not ask for a snapshot cache")
	}

	// flat:// is the same local path with nesting turned off, and the cache decision does not depend on that.
	if storage := CreateStorage(Preference{Name: "flat", StorageURL: "flat://" + localDir}, false, 1); storage == nil {
		t.Errorf("Failed to create a file storage at %s", "flat://"+localDir)
	} else if storage.IsCacheNeeded() {
		t.Errorf("A flat:// storage must not ask for a snapshot cache")
	}

	// The decision is also exposed directly, so that the two halves can be exercised without a mount.
	if needsSnapshotCache(localDir) {
		t.Errorf("A local storage must not be asked to keep a snapshot cache")
	}
}
