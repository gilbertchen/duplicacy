// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
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

func createTestSnapshotManager(testDir string) *SnapshotManager {

	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	storage, _ := CreateFileStorage(testDir, false, 1)
	storage.CreateDirectory(0, "chunks")
	storage.CreateDirectory(0, "snapshots")
	config := CreateConfig()
	snapshotManager := CreateSnapshotManager(config, storage)

	cacheDir := path.Join(testDir, "cache")
	snapshotCache, _ := CreateFileStorage(cacheDir, false, 1)
	snapshotCache.CreateDirectory(0, "chunks")
	snapshotCache.CreateDirectory(0, "snapshots")

	snapshotManager.snapshotCache = snapshotCache
	return snapshotManager
}

func uploadTestChunk(manager *SnapshotManager, content []byte) string {

	completionFunc := func(chunk *Chunk, chunkIndex int, skipped bool, chunkSize int, uploadSize int) {
		LOG_INFO("UPLOAD_CHUNK", "Chunk %s size %d uploaded", chunk.GetID(), chunkSize)
	}

	chunkUploader := CreateChunkUploader(manager.config, manager.storage, nil, testThreads, nil)
	chunkUploader.completionFunc = completionFunc
	chunkUploader.Start()

	chunk := CreateChunk(manager.config, true)
	chunk.Reset(true)
	chunk.Write(content)
	chunkUploader.StartChunk(chunk, 0)
	chunkUploader.Stop()

	return chunk.GetHash()
}

func uploadRandomChunk(manager *SnapshotManager, chunkSize int) string {
	content := make([]byte, chunkSize)
	_, err := rand.Read(content)
	if err != nil {
		LOG_ERROR("UPLOAD_RANDOM", "Error generating random content: %v", err)
		return ""
	}

	return uploadTestChunk(manager, content)
}

func createTestSnapshot(manager *SnapshotManager, snapshotID string, revision int, startTime int64, endTime int64, chunkHashes []string) {

	snapshot := &Snapshot{
		ID:          snapshotID,
		Revision:    revision,
		StartTime:   startTime,
		EndTime:     endTime,
		ChunkHashes: chunkHashes,
	}

	var chunkHashesInHex []string
	for _, chunkHash := range chunkHashes {
		chunkHashesInHex = append(chunkHashesInHex, hex.EncodeToString([]byte(chunkHash)))
	}

	sequence, _ := json.Marshal(chunkHashesInHex)
	snapshot.ChunkSequence = []string{uploadTestChunk(manager, sequence)}

	description, _ := snapshot.MarshalJSON()
	path := fmt.Sprintf("snapshots/%s/%d", snapshotID, snapshot.Revision)
	manager.storage.CreateDirectory(0, "snapshots/"+snapshotID)
	manager.UploadFile(path, path, description)
}

func checkTestSnapshots(manager *SnapshotManager, expectedSnapshots int, expectedFossils int) {

	var snapshotIDs []string
	var err error

	chunks := make(map[string]bool)
	files, _ := manager.ListAllFiles(manager.storage, "chunks/")
	for _, file := range files {
		if file[len(file)-1] == '/' {
			continue
		}
		chunk := strings.Replace(file, "/", "", -1)
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

func TestSingleRepositoryPrune(t *testing.T) {

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
	t.Logf("Creating 1 snapshot")
	createTestSnapshot(snapshotManager, "repository1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2})
	checkTestSnapshots(snapshotManager, 1, 2)

	t.Logf("Creating 2 snapshots")
	createTestSnapshot(snapshotManager, "repository1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3})
	createTestSnapshot(snapshotManager, "repository1", 3, now-1*day-3600, now-1*day-60, []string{chunkHash3, chunkHash4})
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot repository1 revision 1 with --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{1}, []string{}, []string{}, false, true, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot repository1 revision 2 without --exclusive")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{2}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 1, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "repository1", 4, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5})
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("repository1", "repository1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestSingleHostPrune(t *testing.T) {

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
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2})
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3})
	createTestSnapshot(snapshotManager, "vm2@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash3, chunkHash4})
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm2@host1", 2, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5})
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 3, 0)

}

func TestMultipleHostPrune(t *testing.T) {

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
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2})
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3})
	createTestSnapshot(snapshotManager, "vm2@host2", 1, now-3*day-3600, now-3*day-60, []string{chunkHash3, chunkHash4})
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm2@host2", 2, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5})
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash6 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now+1*day-3600, now+1*day, []string{chunkHash5, chunkHash6})
	checkTestSnapshots(snapshotManager, 4, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
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
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2})
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3})
	checkTestSnapshots(snapshotManager, 2, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1 without --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 1, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash4 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 4, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash1})
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- one fossil will be resurrected")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 0)
}

func TestInactiveHostPrune(t *testing.T) {

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
	createTestSnapshot(snapshotManager, "vm1@host1", 1, now-3*day-3600, now-3*day-60, []string{chunkHash1, chunkHash2})
	createTestSnapshot(snapshotManager, "vm1@host1", 2, now-2*day-3600, now-2*day-60, []string{chunkHash2, chunkHash3})
	// Host2 is inactive
	createTestSnapshot(snapshotManager, "vm2@host2", 1, now-7*day-3600, now-7*day-60, []string{chunkHash3, chunkHash4})
	checkTestSnapshots(snapshotManager, 3, 0)

	t.Logf("Removing snapshot vm1@host1 revision 1")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{1}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Prune without removing any snapshots -- no fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 2, 2)

	t.Logf("Creating 1 snapshot")
	chunkHash5 := uploadRandomChunk(snapshotManager, chunkSize)
	createTestSnapshot(snapshotManager, "vm1@host1", 3, now+1*day-3600, now+1*day, []string{chunkHash4, chunkHash5})
	checkTestSnapshots(snapshotManager, 3, 2)

	t.Logf("Prune without removing any snapshots -- fossils will be deleted")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{}, false, false, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 3, 0)
}

func TestRetentionPolicy(t *testing.T) {

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
		createTestSnapshot(snapshotManager, "vm1@host1", i+1, now-int64(30-i)*day-3600, now-int64(30-i)*day-60, []string{chunkHashes[i]})
	}

	checkTestSnapshots(snapshotManager, 30, 0)

	t.Logf("Removing snapshot vm1@host1 0:20 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"0:20"}, false, true, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 19, 0)

	t.Logf("Removing snapshot vm1@host1 -k 0:20 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"0:20"}, false, true, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 19, 0)

	t.Logf("Removing snapshot vm1@host1 -k 3:14 -k 2:7 with --exclusive")
	snapshotManager.PruneSnapshots("vm1@host1", "vm1@host1", []int{}, []string{}, []string{"3:14", "2:7"}, false, true, []string{}, false, false, false)
	checkTestSnapshots(snapshotManager, 12, 0)
}
