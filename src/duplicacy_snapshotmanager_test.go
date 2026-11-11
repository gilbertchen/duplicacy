// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"
)

func createDummySnapshot(snapshotID string, revision int, endTime int64) *Snapshot {
	return &Snapshot{
		ID:       snapshotID,
		Revision: revision,
		EndTime:  endTime,
	}
}

func TestIsDeletable(t *testing.T) {

	//SetLoggingLevel(DEBUG)

	now := time.Now().Unix()
	day := int64(3600 * 24)

	allSnapshots := make(map[string][]*Snapshot)
	allSnapshots["host1"] = append([]*Snapshot{}, createDummySnapshot("host1", 1, now-2*day))
	allSnapshots["host2"] = append([]*Snapshot{}, createDummySnapshot("host2", 1, now-2*day))
	allSnapshots["host1"] = append(allSnapshots["host1"], createDummySnapshot("host1", 2, now-1*day))
	allSnapshots["host2"] = append(allSnapshots["host2"], createDummySnapshot("host2", 2, now-1*day))

	collection := &FossilCollection{
		EndTime:       now - day - 3600,
		LastRevisions: make(map[string]int),
	}

	collection.LastRevisions["host1"] = 1
	collection.LastRevisions["host2"] = 1

	isDeletable, newSnapshots := collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 2 {
		t.Errorf("Scenario 1: should be deletable, 2 new snapshots")
	}

	collection.LastRevisions["host3"] = 1
	allSnapshots["host3"] = append([]*Snapshot{}, createDummySnapshot("host3", 1, now-2*day))

	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if isDeletable {
		t.Errorf("Scenario 2: should not be deletable")
	}

	allSnapshots["host3"] = append(allSnapshots["host3"], createDummySnapshot("host3", 2, now-day))
	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 3 {
		t.Errorf("Scenario 3: should be deletable, 3 new snapshots")
	}

	collection.LastRevisions["host4"] = 1
	allSnapshots["host4"] = append([]*Snapshot{}, createDummySnapshot("host4", 1, now-8*day))

	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 3 {
		t.Errorf("Scenario 4: should be deletable, 3 new snapshots")
	}

	collection.LastRevisions["repository1@host5"] = 1
	allSnapshots["repository1@host5"] = append([]*Snapshot{}, createDummySnapshot("repository1@host5", 1, now-3*day))

	collection.LastRevisions["repository2@host5"] = 1
	allSnapshots["repository2@host5"] = append([]*Snapshot{}, createDummySnapshot("repository2@host5", 1, now-2*day))

	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if isDeletable {
		t.Errorf("Scenario 5: should not be deletable")
	}

	allSnapshots["repository1@host5"] = append(allSnapshots["repository1@host5"], createDummySnapshot("repository1@host5", 2, now-day))
	isDeletable, newSnapshots = collection.IsDeletable(true, nil, allSnapshots)
	if !isDeletable || len(newSnapshots) != 4 {
		t.Errorf("Scenario 6: should be deletable, 4 new snapshots")
	}
}

func checkTestSnapshots(manager *SnapshotManager, expectedSnapshots int, expectedFossils int) {

	manager.CreateChunkOperator(false, false, 1, false)
	defer func() {
		manager.chunkOperator.Stop()
		manager.chunkOperator = nil
	}()

	var snapshotIDs []string
	var err error

	chunks := make(map[string]bool)
	files, _ := manager.ListAllFiles(manager.storage, "chunks/")
	for _, file := range files {
		if file[len(file)-1] == '/' {
			continue
		}
		chunk := chunkIDFromListedPath(file)
		chunks[chunk] = false
	}

	snapshotIDs, err = manager.ListSnapshotIDs()
	if err != nil {
		LOG_ERROR("SNAPSHOT_LIST", "Failed to list all snapshots: %v", err)
		return
	}

	numberOfSnapshots := 0

	for _, snapshotID := range snapshotIDs {

		revisions, err := manager.ListSnapshotRevisions(snapshotID)
		if err != nil {
			LOG_ERROR("SNAPSHOT_LIST", "Failed to list all revisions for snapshot %s: %v", snapshotID, err)
			return
		}

		for _, revision := range revisions {
			snapshot := manager.DownloadSnapshot(snapshotID, revision)
			numberOfSnapshots++

			for _, chunk := range manager.GetSnapshotChunks(snapshot, false) {
				chunks[chunk] = true
			}
		}
	}

	numberOfFossils := 0
	for chunk, referenced := range chunks {
		if !referenced {
			LOG_INFO("UNREFERENCED_CHUNK", "Unreferenced chunk %s", chunk)
			numberOfFossils++
		}
	}

	if numberOfSnapshots != expectedSnapshots {
		LOG_ERROR("SNAPSHOT_COUNT", "Expecting %d snapshots, got %d instead", expectedSnapshots, numberOfSnapshots)
	}

	if numberOfFossils != expectedFossils {
		LOG_ERROR("FOSSIL_COUNT", "Expecting %d unreferenced chunks, got %d instead", expectedFossils, numberOfFossils)
	}
}

// capturedLog is a single message passed to one of the logging functions.
type capturedLog struct {
	level   int
	logID   string
	message string
}

// logCapture records the log messages produced while it is installed as LogFunction, so that a test can inspect what
// a command printed.
type logCapture struct {
	logs     []capturedLog
	logsLock sync.Mutex
}

func (capture *logCapture) log(level int, logID string, message string) {
	capture.logsLock.Lock()
	defer capture.logsLock.Unlock()
	capture.logs = append(capture.logs, capturedLog{level, logID, message})
}

// messages returns the messages that were logged under 'logID'.
func (capture *logCapture) messages(logID string) (messages []string) {
	capture.logsLock.Lock()
	defer capture.logsLock.Unlock()
	for _, log := range capture.logs {
		if log.logID == logID {
			messages = append(messages, log.message)
		}
	}
	return messages
}

// failures returns the messages that were logged as errors, since they are only recorded and never propagated while
// the capture is installed.
func (capture *logCapture) failures() (failures []string) {
	capture.logsLock.Lock()
	defer capture.logsLock.Unlock()
	for _, log := range capture.logs {
		if log.level >= ERROR {
			failures = append(failures, fmt.Sprintf("%s: %s", log.logID, log.message))
		}
	}
	return failures
}

// Both the reading and the writing side derive the path of a snapshot file from snapshotDir and snapshotPath, so the
// storage layout is defined once.  This pins that layout: the snapshot file of a revision sits directly in the
// directory named after the snapshot id, and the directory carries the trailing slash the listings expect.
func TestSnapshotPathHelpers(t *testing.T) {

	if dir := snapshotDir("vm1@host1"); dir != "snapshots/vm1@host1/" {
		t.Errorf("snapshotDir returned %q instead of %q", dir, "snapshots/vm1@host1/")
	}
	if file := snapshotPath("vm1@host1", 3); file != "snapshots/vm1@host1/3" {
		t.Errorf("snapshotPath returned %q instead of %q", file, "snapshots/vm1@host1/3")
	}
}

// A chunk listed under 'chunks/' arrives as the nested path the storage lays it out in.  chunkIDFromListedPath has to
// recover the 64-character id from it, and it has to leave the '.fsl' suffix of a fossil alone: the suffix is what
// tells a caller that the entry is a fossil rather than a chunk.
func TestChunkIDFromListedPath(t *testing.T) {

	chunkID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	// The nesting directories are the first bytes of the id itself, so removing the separators has to return the id
	// unchanged.
	nestedPath := chunkID[:2] + "/" + chunkID[2:4] + "/" + chunkID[4:]

	if id := chunkIDFromListedPath(nestedPath); id != chunkID {
		t.Errorf("chunkIDFromListedPath returned %q instead of %q", id, chunkID)
	}
	if fossil := chunkIDFromListedPath(nestedPath + ".fsl"); fossil != chunkID+".fsl" {
		t.Errorf("chunkIDFromListedPath returned %q instead of %q for a fossil", fossil, chunkID+".fsl")
	}
}

// The three metadata sequences are what 'which chunks does this snapshot reference' means, and the chunk-id walk
// depends on seeing all of them.  MetadataSequences is the one definition of that set.
func TestMetadataSequences(t *testing.T) {

	snapshot := &Snapshot{
		FileSequence:   []string{"file"},
		ChunkSequence:  []string{"chunks"},
		LengthSequence: []string{"lengths"},
	}

	sequences := snapshot.MetadataSequences()
	if len(sequences) != 3 {
		t.Errorf("MetadataSequences returned %d sequences instead of 3", len(sequences))
		return
	}
	for i, expected := range []string{"file", "chunks", "lengths"} {
		if len(sequences[i]) != 1 || sequences[i][0] != expected {
			t.Errorf("MetadataSequences returned %v at index %d instead of the %s sequence", sequences[i], i, expected)
		}
	}
}

func TestPruneSingleRepository(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "repository1", 1, now-4*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "repository1", 2, now-4*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "repository1", 3, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "repository1", 4, now-1*day-3600, now-1*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 4, 0)

	t.Logf("Removing snapshot repository1 revisions 1 and 2 with --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{1, 2}, []string{}, []string{}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot repository1 revision 3 without --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{3}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 1, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "repository1", 5, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestPruneSingleHost(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "vm2@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm2@host1", 2, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 0)

}

func TestPruneMultipleHost(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshot")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "vm2@host2", 1, now-3*day-3600, now-3*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm2@host2", 2, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash6 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now+1*day-3600, now+1*day, []string{chunkHash5, chunkHash6}, "tag")
	checkTestSnapshots(snapshotManager, 4, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 4, 0)
}

func TestPruneAndResurrect(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 1, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 4, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash1}, "tag")
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- one fossil will be resurrected")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestPruneWithInactiveHost(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshot")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	// Host2 is inactive
	createTestSnapshot(snapshotManager, "vm2@host2", 1, now-7*day-3600, now-7*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 0)
}

func TestPruneWithRetentionPolicy(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	var chunkHashes []string
	for i := 0; i < 30; i++ {
		chunkHashes = append(chunkHashes, uploadRandomChunk(snapshotManager, chunkSize))
	}

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 30 snapshots")
	for i := 0; i < 30; i++ {
		createTestSnapshot(snapshotManager, "vm1@host1", i+1, now-int64(30-i)*day-3600, now-int64(30-i)*day-60, []string{chunkHashes[i]}, "tag")
	}

	checkTestSnapshots(snapshotManager, 30, 0)

	t.Logf("Removing snapshot vm1@host1 0:20 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"0:20"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 19, 0)

	t.Logf("Removing snapshot vm1@host1 -k 0:20 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"0:20"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 19, 0)

	t.Logf("Removing snapshot vm1@host1 -k 3:14 -k 2:7 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"3:14", "2:7"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 12, 0)
}

func TestPruneWithRetentionPolicyAndTag(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	var chunkHashes []string
	for i := 0; i < 30; i++ {
		chunkHashes = append(chunkHashes, uploadRandomChunk(snapshotManager, chunkSize))
	}

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 30 snapshots")
	for i := 0; i < 30; i++ {
		tag := "auto"
		if i%3 == 0 {
			tag = "manual"
		}
		createTestSnapshot(snapshotManager, "vm1@host1", i+1, now-int64(30-i)*day-3600, now-int64(30-i)*day-60, []string{chunkHashes[i]}, tag)
	}

	checkTestSnapshots(snapshotManager, 30, 0)

	t.Logf("Removing snapshot vm1@host1 0:20 with --exclusive and --tag manual")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{"manual"}, []string{"0:7"}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 22, 0)
}

// Test that an unreferenced fossil shouldn't be removed as it may be the result of another prune job in-progress.
func TestPruneWithFossils(t *testing.T) {
	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	// Create an unreferenced fossil
	snapshotManager.storage.UploadFile(0, "chunks/113b6a2350dcfd836829c47304dd330fa6b58b93dd7ac696c6b7b913e6868662.fsl", []byte("this is a test fossil"))

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	checkTestSnapshots(snapshotManager, 2, 1)

	t.Logf("Prune without removing any snapshots but with --exhaustive")
	// The unreferenced fossil shouldn't be removed
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, true, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 1)

	t.Logf("Prune without removing any snapshots but with --exclusive")
	// Now the unreferenced fossil should be removed
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, true, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestPruneMultipleThread(t *testing.T) {

	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	numberOfChunks := 256
	numberOfThreads := 4

	chunkList1 := uploadRandomChunks(snapshotManager, chunkSize, numberOfChunks)
	chunkList2 := uploadRandomChunks(snapshotManager, chunkSize, numberOfChunks)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "repository1", 1, now-4*day-3600, now-3*day-60, chunkList1, "tag")
	createTestSnapshot(snapshotManager, "repository1", 2, now-3*day-3600, now-2*day-60, chunkList2, "tag")
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot revisions 1 with --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{1}, []string{}, []string{}, false, true, []string{}, false, false, false, numberOfThreads)
	checkTestSnapshots(snapshotManager, 1, 0)

	t.Logf("Creating 1 more snapshot")
	chunkList3 := uploadRandomChunks(snapshotManager, chunkSize, numberOfChunks)
	createTestSnapshot(snapshotManager, "repository1", 3, now-2*day-3600, now-1*day-60, chunkList3, "tag")

	t.Logf("Removing snapshot repository1 revision 2 without --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{2}, []string{}, []string{}, false, false, []string{}, false, false, false, numberOfThreads)

	t.Logf("Prune without removing any snapshots but with --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{}, []string{}, []string{}, false, true, []string{}, false, false, false, numberOfThreads)
	checkTestSnapshots(snapshotManager, 1, 0)
}

// A snapshot not seen by a fossil collection should always be consider a new snapshot in the fossil deletion step
func TestPruneNewSnapshots(t *testing.T) {
	setTestingT(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 3 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	createTestSnapshot(snapshotManager, "vm2@host1", 1, now-2*day-3600, now-2*day-60, []string{chunkHash3, chunkHash4}, "tag")
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Prune snapshot 1")
	// chunkHash1 should be marked as fossil
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	// Create another snapshot of vm1 that brings back chunkHash1
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now-0*day-3600, now-0*day-60, []string{chunkHash1, chunkHash3}, "tag")
	// Create another snapshot of vm2 so the fossil collection will be processed by next prune
	createTestSnapshot(snapshotManager, "vm2@host1", 2, now+3600, now+3600*2, []string{chunkHash4, chunkHash5}, "tag")

	// Now chunkHash1 wil be resurrected
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 4, 0)
	snapshotManager.CheckSnapshots("vm1@host1", []int{2, 3}, "", false, false, false, false, false, false, false, 1, false)
}

// A fossil collection left by an aborted prune should be ignored if any supposedly deleted snapshot exists
func TestPruneGhostSnapshots(t *testing.T) {
	setTestingT(t)

	EnableStackTrace()

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	chunkSize := 1024
	chunkHash1 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash2 := uploadRandomChunk(snapshotManager, chunkSize)
	chunkHash3 := uploadRandomChunk(snapshotManager, chunkSize)

	now := time.Now().Unix()
	day := int64(24 * 3600)
	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2}, "tag")
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3}, "tag")
	checkTestSnapshots(snapshotManager, 2, 0)

	snapshot1, err := ioutil.ReadFile(path.Join(testDir, "snapshots", "vm1@host1", "1"))
	if err != nil {
		t.Errorf("Failed to read snapshot file: %v", err)
	}

	t.Logf("Prune snapshot 1")
	// chunkHash1 should be marked as fossil
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 1, 2)

	// Recover the snapshot file for revision 1; this is to simulate a scenario where prune may encounter a network error after
	// leaving the fossil collection but before deleting any snapshots.
	err = ioutil.WriteFile(path.Join(testDir, "snapshots", "vm1@host1", "1"), snapshot1, 0644)
	if err != nil {
		t.Errorf("Failed to write snapshot file: %v", err)
	}

	// Create another snapshot of vm1 so the fossil collection becomes eligible for processing.
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now-day-3600, now-day-60, []string{chunkHash3, chunkHash4}, "tag")

	// Run the prune again but the fossil collection should be igored, since revision 1 still exists
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 2)
	snapshotManager.CheckSnapshots("vm1@host1", []int{1, 2, 3}, "", false, false, false, false, true /*searchFossils*/, false, false, 1, false)

	// Prune snapshot 1 again
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 2, 2)

	// Create another snapshot
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 4, now+3600, now+3600*2, []string{chunkHash5, chunkHash5}, "tag")
	checkTestSnapshots(snapshotManager, 3, 2)

	// Run the prune again and this time the fossil collection will be processed and the fossils removed
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false, 1)
	checkTestSnapshots(snapshotManager, 3, 0)
	snapshotManager.CheckSnapshots("vm1@host1", []int{2, 3, 4}, "", false, false, false, false, false, false, false, 1, false)
}
