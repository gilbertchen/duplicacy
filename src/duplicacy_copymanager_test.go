// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"crypto/rsa"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"
)

// TestIsBitIdenticalWith checks the predicate that decides whether a chunk may be copied without being re-encoded.  A
// pair of storages is bit-identical only when they agree on the chunk hash, the chunk id, the chunk encryption key,
// the compression and the erasure coding, and neither of them uses RSA encryption.
func TestIsBitIdenticalWith(t *testing.T) {

	setTestingT(t)

	base := CreateConfigFromParameters(DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, true, nil, false)

	bitCopy := CreateConfigFromParameters(DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, true, base, true)
	if !base.IsBitIdenticalWith(bitCopy) || !bitCopy.IsBitIdenticalWith(base) {
		t.Errorf("A bit-identical copy should be reported as bit-identical")
	}

	notBitCopy := CreateConfigFromParameters(DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, true, base, false)
	if base.IsBitIdenticalWith(notBitCopy) {
		t.Errorf("A copy that only shares the chunk seed and hash key should not be reported as bit-identical")
	}

	// The compression level changes the stored bytes; 'add -copy -bit-identical' carries it over, so the predicate
	// has to reject a pair that differs even when everything else matches.
	otherCompression := *bitCopy
	otherCompression.CompressionLevel = ZSTD_COMPRESSION_LEVEL_DEFAULT
	if base.IsBitIdenticalWith(&otherCompression) {
		t.Errorf("Storages with different compression levels should not be reported as bit-identical")
	}

	otherChunkKey := *bitCopy
	otherChunkKey.ChunkKey = []byte("a different chunk key for the test")
	if base.IsBitIdenticalWith(&otherChunkKey) {
		t.Errorf("Storages with different chunk keys should not be reported as bit-identical")
	}

	otherIDKey := *bitCopy
	otherIDKey.IDKey = []byte("a different id key for the test")
	if base.IsBitIdenticalWith(&otherIDKey) {
		t.Errorf("Storages with different id keys should not be reported as bit-identical")
	}

	erasureCoding := *bitCopy
	erasureCoding.DataShards = 5
	erasureCoding.ParityShards = 2
	if base.IsBitIdenticalWith(&erasureCoding) {
		t.Errorf("Storages with different erasure-coding settings should not be reported as bit-identical")
	}

	rsaConfig := *bitCopy
	rsaConfig.rsaPublicKey = &rsa.PublicKey{}
	if base.IsBitIdenticalWith(&rsaConfig) {
		t.Errorf("An RSA-encrypted storage should not be reported as bit-identical")
	}

	unencrypted := CreateConfigFromParameters(DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, false, nil, false)
	if !unencrypted.IsBitIdenticalWith(unencrypted) {
		t.Errorf("An unencrypted storage should be bit-identical to itself")
	}
}

// TestIsCompatibleWith checks the predicate that decides whether a copy may run at all.  It is the weaker of the two:
// a copy re-encrypts the chunks, so a destination with different keys is compatible as long as the chunks fall on the
// same boundaries and are stored under the same hash.
func TestIsCompatibleWith(t *testing.T) {

	setTestingT(t)

	base := CreateConfigFromParameters(DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, true, nil, false)
	notBitCopy := CreateConfigFromParameters(DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, true, base, false)

	if !base.IsCompatibleWith(notBitCopy) || !notBitCopy.IsCompatibleWith(base) {
		t.Errorf("A copy with different encryption keys should still be compatible")
	}

	differentHashKey := *notBitCopy
	differentHashKey.HashKey = []byte("a different hash key for the test")
	if base.IsCompatibleWith(&differentHashKey) {
		t.Errorf("Storages with different hash keys should not be reported as compatible")
	}

	differentSizes := *notBitCopy
	differentSizes.AverageChunkSize = 32 * 1024
	if base.IsCompatibleWith(&differentSizes) {
		t.Errorf("Storages with different chunk sizes should not be reported as compatible")
	}

	differentSeed := *notBitCopy
	differentSeed.ChunkSeed = []byte("a different chunk seed for the test")
	if base.IsCompatibleWith(&differentSeed) {
		t.Errorf("Storages with different chunk seeds should not be reported as compatible")
	}
}

// copyCase carries the storages of one copy scenario.
type copyCase struct {
	name        string
	encrypted   bool
	seed        int64
	bitCopy     bool
	rawExpected bool // whether the two storages should store a chunk identically
}

// TestChunkPath checks that ChunkPath derives the path FindChunk reports for a missing chunk, and rejects an
// inconsistent nesting setup.
func TestChunkPath(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_chunk_path_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	chunkID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	cases := []struct {
		name       string
		readLevels []int
		writeLevel int
		expected   string
	}{
		// The pre-2.0.10 layout of a local storage, where new chunks go to level 2 but older ones may sit at level 3.
		{name: "two levels", readLevels: []int{2, 3}, writeLevel: 2, expected: "chunks/01/23/456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		// The layout of a storage with the fixed nesting.
		{name: "one level", readLevels: []int{1}, writeLevel: 1, expected: "chunks/01/23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		// A flat storage that keeps all chunks in one directory.
		{name: "no level", readLevels: []int{0}, writeLevel: 0, expected: "chunks/" + chunkID},
	}

	for _, c := range cases {
		storage, err := CreateFileStorage(path.Join(testDir, c.name), false, 1)
		if err != nil {
			t.Errorf("Failed to create the storage of %s: %v", c.name, err)
			continue
		}
		storage.SetDefaultNestingLevels(c.readLevels, c.writeLevel)

		chunkPath, err := storage.ChunkPath(chunkID)
		if err != nil {
			t.Errorf("ChunkPath failed for %s: %v", c.name, err)
			continue
		}
		if chunkPath != c.expected {
			t.Errorf("ChunkPath returned %s instead of %s for %s", chunkPath, c.expected, c.name)
		}

		// The uploader relies on this path when it is told not to look the chunk up.
		foundPath, exist, _, err := storage.FindChunk(0, chunkID, false)
		if err != nil {
			t.Errorf("FindChunk failed for %s: %v", c.name, err)
			continue
		}
		if exist || foundPath != chunkPath {
			t.Errorf("FindChunk returned %s (exist %v) instead of %s for %s", foundPath, exist, chunkPath, c.name)
		}
	}

	// A write level outside the read levels is a broken setup.
	invalidStorage, err := CreateFileStorage(path.Join(testDir, "invalid"), false, 1)
	if err != nil {
		t.Errorf("Failed to create the invalid storage: %v", err)
		return
	}
	invalidStorage.SetDefaultNestingLevels([]int{1}, 2)
	if _, err := invalidStorage.ChunkPath(chunkID); err == nil {
		t.Errorf("ChunkPath should have rejected a write level that isn't in the read levels")
	}
}

// TestCopyChunkProbe covers the two ways a copy learns which chunks the destination already holds: listing it, in
// which case the uploader is not asked to look anything up, or looking each chunk up, which must happen before the
// download so that a chunk already there is never fetched.  Both must leave the second copy of an unchanged
// repository moving nothing.
func TestCopyChunkProbe(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_copy_probe_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	threads := 1
	seed := int64(17)

	sourceDir := path.Join(testDir, "source_storage")
	innerSource, err := loadStorage(sourceDir, threads)
	if err != nil {
		t.Errorf("Failed to create the source storage: %v", err)
		return
	}
	sourceStorage := &instrumentedStorage{FileStorage: innerSource.(*FileStorage)}
	if !ConfigStorage(sourceStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to configure the source storage")
		return
	}

	repository := path.Join(testDir, "repository")
	os.MkdirAll(path.Join(repository, ".duplicacy"), 0700)
	createRandomFileSeeded(path.Join(repository, "file1"), 300000, seed)
	createRandomFileSeeded(path.Join(repository, "file2"), 300000, seed+1)

	SetDuplicacyPreferencePath(path.Join(repository, ".duplicacy"))
	sourceManager := CreateBackupManager("host1", sourceStorage, repository, "", "", "", false)
	sourceManager.SetupSnapshotCache("source")
	if !sourceManager.Backup(repository, true, threads, "first", false, false, 0, false, 1024, 1024) {
		t.Errorf("Failed to back the repository up")
		return
	}

	sourceTree := readChunkTree(t, sourceDir)
	if len(sourceTree) == 0 {
		t.Errorf("Expected the source storage to hold chunks")
		return
	}

	// anyProbes marks an expectation that depends on how the chunk hashes fall into the destination's directories,
	// which the test cannot control: either the destination was listed, or each chunk was looked up once.
	const anyProbes = int64(-1)

	for _, c := range []struct {
		name               string
		isFastListing      bool
		numberOfDirs       int
		expectFirstProbes  int64
		expectSecondProbes int64
	}{
		// A fast-listing storage is listed no matter how many chunk directories it holds, on both copies.
		{name: "listed", isFastListing: true, numberOfDirs: len(sourceTree) + 10,
			expectFirstProbes: 0, expectSecondProbes: 0},
		// A slow-listing storage is listed while its directories are fewer than the chunks to copy, since one
		// listing then costs less than looking every chunk up.  This is the case that must not regress.
		{name: "listed_slow", isFastListing: false, numberOfDirs: 0,
			expectFirstProbes: 0, expectSecondProbes: anyProbes},
		// Once the directories outnumber the chunks to copy, each chunk is looked up instead, before it is
		// downloaded.
		{name: "probed", isFastListing: false, numberOfDirs: len(sourceTree) + 10,
			expectFirstProbes: int64(len(sourceTree)), expectSecondProbes: int64(len(sourceTree))},
	} {
		c := c

		destinationDir := path.Join(testDir, c.name+"_storage")
		innerDestination, err := CreateFileStorage(destinationDir, false, threads)
		if err != nil {
			t.Errorf("Failed to create the %s destination storage: %v", c.name, err)
			continue
		}
		// The count decides whether a slow-listing storage is listed or probed; the names are not chunk ids.
		createChunkDirectories(t, destinationDir, c.numberOfDirs)

		destinationStorage := &instrumentedStorage{FileStorage: innerDestination, isFastListing: c.isFastListing}
		if !ConfigStorage(destinationStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024,
			"", sourceManager.config, false, "", 0, 0) {
			t.Errorf("Failed to configure the %s destination storage", c.name)
			continue
		}

		destinationRepository := path.Join(testDir, c.name+"_repository")
		os.MkdirAll(path.Join(destinationRepository, ".duplicacy"), 0700)
		SetDuplicacyPreferencePath(path.Join(destinationRepository, ".duplicacy"))
		destinationManager := CreateBackupManager("host1", destinationStorage, destinationRepository, "", "", "", false)
		destinationManager.SetupSnapshotCache(c.name)

		// The first copy moves every chunk the snapshot references.
		destinationStorage.resetCounters()
		sourceStorage.resetCounters()

		if !sourceManager.CopySnapshots(destinationManager, "", nil, 1, 1) {
			t.Errorf("Failed to copy the snapshot of %s", c.name)
			continue
		}

		if probes := atomic.LoadInt64(&destinationStorage.findChunkCalls); probes != c.expectFirstProbes {
			t.Errorf("The %s copy made %d per-chunk destination lookups instead of %d",
				c.name, probes, c.expectFirstProbes)
		}
		if uploads := atomic.LoadInt64(&destinationStorage.uploadedChunks); uploads != int64(len(sourceTree)) {
			t.Errorf("The %s copy uploaded %d chunks instead of %d", c.name, uploads, len(sourceTree))
		}
		firstCopyDownloads := atomic.LoadInt64(&sourceStorage.chunkDownloads)
		if firstCopyDownloads < int64(len(sourceTree)) {
			t.Errorf("The %s copy downloaded %d chunks, fewer than the %d it had to move",
				c.name, firstCopyDownloads, len(sourceTree))
		}

		destinationTree := readChunkTree(t, destinationDir)
		if len(destinationTree) != len(sourceTree) {
			t.Errorf("The %s destination holds %d chunks instead of %d", c.name, len(destinationTree), len(sourceTree))
		}
		for chunkPath, content := range sourceTree {
			if destinationContent, found := destinationTree[chunkPath]; !found || !bytes.Equal(content, destinationContent) {
				t.Errorf("The chunk %s of %s was not copied as it is", chunkPath, c.name)
				break
			}
		}

		// The second backup adds a revision referencing the same chunks, so the next copy must move nothing and must
		// not download the chunks to find that out.
		if !sourceManager.Backup(repository, true, threads, "second", false, false, 0, false, 1024, 1024) {
			t.Errorf("Failed to back the repository of %s up again", c.name)
			continue
		}

		destinationStorage.resetCounters()
		sourceStorage.resetCounters()

		if !sourceManager.CopySnapshots(destinationManager, "", nil, 1, 1) {
			t.Errorf("Failed to copy the second snapshot of %s", c.name)
			continue
		}

		if probes := atomic.LoadInt64(&destinationStorage.findChunkCalls); c.expectSecondProbes == anyProbes {
			// Nothing else is a valid outcome.
			if probes != 0 && probes != int64(len(sourceTree)) {
				t.Errorf("The second %s copy made %d per-chunk destination lookups, which is neither a listing nor one lookup per chunk",
					c.name, probes)
			}
		} else if probes != c.expectSecondProbes {
			t.Errorf("The second %s copy made %d per-chunk destination lookups instead of %d",
				c.name, probes, c.expectSecondProbes)
		}
		if uploads := atomic.LoadInt64(&destinationStorage.uploadedChunks); uploads != 0 {
			t.Errorf("The second %s copy uploaded %d chunks although the destination already held them all",
				c.name, uploads)
		}

		// Only metadata is read from the source this time.  The first copy read the metadata too, on top of moving
		// every chunk, so this count has to be the smaller.
		if downloads := atomic.LoadInt64(&sourceStorage.chunkDownloads); downloads >= firstCopyDownloads {
			t.Errorf("The second %s copy downloaded %d chunks, as many as the first one's %d, although it had to move none",
				c.name, downloads, firstCopyDownloads)
		}

		secondDestinationTree := readChunkTree(t, destinationDir)
		if len(secondDestinationTree) != len(destinationTree) {
			t.Errorf("The second %s copy changed the destination from %d to %d chunks", c.name,
				len(destinationTree), len(secondDestinationTree))
		}
	}
}

// TestCopyDestinationRevisionCheck covers candidate fix #4: the revisions the destination already holds are read with
// one listing of its snapshot directory, instead of one existence check per source revision.  A copy of N revisions
// must ask the destination about its snapshot files at most once, and must still skip the revisions it already has.
func TestCopyDestinationRevisionCheck(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_copy_revision_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	threads := 1
	snapshotID := "host1"

	sourceDir := path.Join(testDir, "source_storage")
	innerSource, err := loadStorage(sourceDir, threads)
	if err != nil {
		t.Errorf("Failed to create the source storage: %v", err)
		return
	}
	cleanStorage(innerSource)
	if !ConfigStorage(innerSource, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024, "", nil, false, "", 0, 0) {
		t.Errorf("Failed to configure the source storage")
		return
	}
	sourceStorage := &instrumentedStorage{FileStorage: innerSource.(*FileStorage)}

	repository := path.Join(testDir, "repository")
	os.MkdirAll(path.Join(repository, ".duplicacy"), 0700)
	createRandomFileSeeded(path.Join(repository, "file1"), 300000, 11)

	SetDuplicacyPreferencePath(path.Join(repository, ".duplicacy"))
	sourceManager := CreateBackupManager(snapshotID, sourceStorage, repository, "", "", "", false)
	sourceManager.SetupSnapshotCache("source")

	// Three revisions referring to the same chunks, so the second copy has every revision already.
	for _, tag := range []string{"first", "second", "third"} {
		if !sourceManager.Backup(repository, true, threads, tag, false, false, 0, false, 1024, 1024) {
			t.Errorf("Failed to back the repository up with tag %s", tag)
			return
		}
	}

	// createDestination creates an empty storage configured like the source and its backup manager.
	createDestination := func(name string) (*instrumentedStorage, *BackupManager) {
		destinationDir := path.Join(testDir, name+"_storage")
		innerDestination, err := CreateFileStorage(destinationDir, false, threads)
		if err != nil {
			t.Errorf("Failed to create the %s destination storage: %v", name, err)
			return nil, nil
		}
		destinationStorage := &instrumentedStorage{FileStorage: innerDestination}
		if !ConfigStorage(destinationStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024,
			"", sourceManager.config, false, "", 0, 0) {
			t.Errorf("Failed to configure the %s destination storage", name)
			return nil, nil
		}

		destinationRepository := path.Join(testDir, name+"_repository")
		os.MkdirAll(path.Join(destinationRepository, ".duplicacy"), 0700)
		SetDuplicacyPreferencePath(path.Join(destinationRepository, ".duplicacy"))
		destinationManager := CreateBackupManager(snapshotID, destinationStorage, destinationRepository, "", "", "", false)
		destinationManager.SetupSnapshotCache(name)
		return destinationStorage, destinationManager
	}

	// The destination holds nothing, so all three revisions are copied.  The destination is asked once for its
	// revisions -- a single listing -- rather than once per source revision.
	destinationStorage, destinationManager := createDestination("empty")
	if destinationStorage == nil {
		return
	}

	sourceManager.CopySnapshots(destinationManager, "", nil, 1, 1)

	if listings := atomic.LoadInt64(&destinationStorage.snapshotListings); listings != 1 {
		t.Errorf("The first copy listed the destination snapshot directory %d times instead of once", listings)
	}
	if checks := atomic.LoadInt64(&destinationStorage.snapshotInfoCalls); checks != 0 {
		t.Errorf("The first copy checked the existence of %d destination snapshot files instead of listing the directory",
			checks)
	}
	if uploads := atomic.LoadInt64(&destinationStorage.uploadedSnapshots); uploads != 3 {
		t.Errorf("The first copy uploaded %d snapshot files instead of 3", uploads)
	}
	// The three revisions share one directory, so it is created once for the id rather than once per revision.
	if dirs := atomic.LoadInt64(&destinationStorage.snapshotDirCalls); dirs != 1 {
		t.Errorf("The first copy created the destination snapshot directory %d times instead of once", dirs)
	}

	// A second copy finds every revision already there and must report that without any per-revision check.
	destinationStorage.resetCounters()

	sourceManager.CopySnapshots(destinationManager, "", nil, 1, 1)

	if listings := atomic.LoadInt64(&destinationStorage.snapshotListings); listings != 1 {
		t.Errorf("The second copy listed the destination snapshot directory %d times instead of once", listings)
	}
	if checks := atomic.LoadInt64(&destinationStorage.snapshotInfoCalls); checks != 0 {
		t.Errorf("The second copy checked the existence of %d destination snapshot files instead of listing the directory",
			checks)
	}
	if uploads := atomic.LoadInt64(&destinationStorage.uploadedSnapshots); uploads != 0 {
		t.Errorf("The second copy uploaded %d snapshot files although the destination held every revision", uploads)
	}

	// Restricting the copy to one revision must still copy exactly that revision to a destination that has none.  A
	// copy with -r names its revisions, so there is nothing to enumerate: it checks the named revision against the
	// destination instead of listing the whole snapshot directory.
	restrictedStorage, restrictedManager := createDestination("restricted")
	if restrictedStorage == nil {
		return
	}

	sourceManager.CopySnapshots(restrictedManager, snapshotID, []int{2}, 1, 1)

	if listings := atomic.LoadInt64(&restrictedStorage.snapshotListings); listings != 0 {
		t.Errorf("The restricted copy listed the destination snapshot directory %d times although it named its revisions",
			listings)
	}
	if checks := atomic.LoadInt64(&restrictedStorage.snapshotInfoCalls); checks != 1 {
		t.Errorf("The restricted copy checked the existence of %d destination snapshot files instead of the one revision it named",
			checks)
	}
	if uploads := atomic.LoadInt64(&restrictedStorage.uploadedSnapshots); uploads != 1 {
		t.Errorf("The restricted copy uploaded %d snapshot files instead of the one revision it was asked for", uploads)
	}
	if exist, _, _, err := restrictedStorage.FileStorage.GetFileInfo(0, fmt.Sprintf("snapshots/%s/2", snapshotID)); err != nil {
		t.Errorf("Failed to check for the copied revision: %v", err)
	} else if !exist {
		t.Errorf("The restricted copy did not copy revision 2")
	}
	if exist, _, _, err := restrictedStorage.FileStorage.GetFileInfo(0, fmt.Sprintf("snapshots/%s/1", snapshotID)); err != nil {
		t.Errorf("Failed to check the destination snapshot directory: %v", err)
	} else if exist {
		t.Errorf("The restricted copy copied revision 1 although only revision 2 was requested")
	}
}

// TestCopySnapshots copies a snapshot between storages that store chunks identically and between storages that don't,
// and checks that the copy produces a storage that restores to the same files.  For the bit-identical pairs it also
// checks that every destination chunk file is byte-for-byte the source chunk file, which is the point of candidate
// fix #1: the decode/encode round trip is redundant and the stored bytes can be moved as they are.
func TestCopySnapshots(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	setTestingT(t)
	SetLoggingLevel(INFO)

	defer recoveringWithStack(t)

	testDir := path.Join(os.TempDir(), "duplicacy_copy_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)
	defer os.RemoveAll(testDir)

	threads := 1
	password := "duplicacy"

	cases := []copyCase{
		// An unencrypted pair always shares its keys, so the chunks are stored identically.
		{name: "unenc", encrypted: false, seed: 1, bitCopy: false, rawExpected: true},
		// An encrypted pair created with -bit-identical also stores the chunks identically.
		{name: "encbit", encrypted: true, seed: 2, bitCopy: true, rawExpected: true},
		// An encrypted pair without -bit-identical gets fresh keys, so the destination names and ciphertext differ.
		{name: "encfresh", encrypted: true, seed: 3, bitCopy: false, rawExpected: false},
	}

	for _, current := range cases {
		c := current

		storagePassword := ""
		if c.encrypted {
			storagePassword = password
		}

		// Source storage: configure it, create a repository and back it up.
		sourceDir := path.Join(testDir, c.name+"_source_storage")
		sourceStorage, err := loadStorage(sourceDir, threads)
		if err != nil {
			t.Errorf("Failed to create the source storage of %s: %v", c.name, err)
			continue
		}
		cleanStorage(sourceStorage)
		if !ConfigStorage(sourceStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024,
			storagePassword, nil, false, "", 0, 0) {
			t.Errorf("Failed to configure the source storage of %s", c.name)
			continue
		}

		repository := path.Join(testDir, c.name+"_repository")
		os.MkdirAll(path.Join(repository, ".duplicacy"), 0700)
		createRandomFileSeeded(path.Join(repository, "file1"), 300000, c.seed)
		createRandomFileSeeded(path.Join(repository, "file2"), 300000, c.seed+1)

		SetDuplicacyPreferencePath(path.Join(repository, ".duplicacy"))
		sourceManager := CreateBackupManager("host1", sourceStorage, repository, storagePassword, "", "", false)
		sourceManager.SetupSnapshotCache(c.name)
		if !sourceManager.Backup(repository, true, threads, "first", false, false, 0, false, 1024, 1024) {
			t.Errorf("Failed to back up the repository of %s", c.name)
			continue
		}

		// Destination storage: copy the source configuration, then copy the snapshot.
		destinationDir := path.Join(testDir, c.name+"_destination_storage")
		destinationStorage, err := loadStorage(destinationDir, threads)
		if err != nil {
			t.Errorf("Failed to create the destination storage of %s: %v", c.name, err)
			continue
		}
		cleanStorage(destinationStorage)
		if !ConfigStorage(destinationStorage, 16384, DEFAULT_COMPRESSION_LEVEL, 64*1024, 256*1024, 16*1024,
			storagePassword, sourceManager.config, c.bitCopy, "", 0, 0) {
			t.Errorf("Failed to configure the destination storage of %s", c.name)
			continue
		}

		destinationRepository := path.Join(testDir, c.name+"_destination_repository")
		os.MkdirAll(path.Join(destinationRepository, ".duplicacy"), 0700)
		SetDuplicacyPreferencePath(path.Join(destinationRepository, ".duplicacy"))
		destinationManager := CreateBackupManager("host1", destinationStorage, destinationRepository,
			storagePassword, "", "", false)
		destinationManager.SetupSnapshotCache(c.name + "destination")

		if !sourceManager.CopySnapshots(destinationManager, "", nil, 1, 1) {
			t.Errorf("Failed to copy the snapshot of %s", c.name)
		}

		// The copy must produce a storage from which the original files can be restored.
		restoreDir := path.Join(testDir, c.name+"_restore")
		os.MkdirAll(path.Join(restoreDir, ".duplicacy"), 0700)
		SetDuplicacyPreferencePath(path.Join(restoreDir, ".duplicacy"))
		failedFiles := destinationManager.Restore(restoreDir, 1, true, false, threads, true, false, false, false, nil, false)
		assertRestoreFailures(t, failedFiles, 0)

		for _, file := range []string{"file1", "file2"} {
			sourceHash := getFileHash(path.Join(repository, file))
			restoredHash := getFileHash(path.Join(restoreDir, file))
			if sourceHash != restoredHash {
				t.Errorf("The file %s restored from %s has a different hash: %s vs %s", file, c.name, sourceHash, restoredHash)
			}
		}

		sourceTree := readChunkTree(t, sourceDir)
		destinationTree := readChunkTree(t, destinationDir)

		if len(sourceTree) == 0 {
			t.Errorf("Expected the source storage of %s to hold chunks", c.name)
		}
		if len(sourceTree) != len(destinationTree) {
			t.Errorf("The destination of %s holds %d chunks instead of %d", c.name, len(destinationTree), len(sourceTree))
		}

		identical := 0
		for chunkPath, content := range sourceTree {
			if destinationContent, found := destinationTree[chunkPath]; found && bytes.Equal(content, destinationContent) {
				identical++
			}
		}

		if c.rawExpected {
			if identical != len(sourceTree) {
				t.Errorf("Only %d out of %d chunks of %s were copied byte for byte", identical, len(sourceTree), c.name)
			}
		} else {
			if identical != 0 {
				t.Errorf("Expected the destination chunks of %s to be re-encoded, but %d were identical", c.name, identical)
			}
		}
	}
}
