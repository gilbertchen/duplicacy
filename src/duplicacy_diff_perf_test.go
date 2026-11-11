// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// diffLineContent builds a file body of 'lines' lines of the form '<prefix> <n>', so that a test knows exactly how many
// lines two revisions have in common.
func diffLineContent(lines int, prefix string) []byte {
	var body bytes.Buffer
	for i := 1; i <= lines; i++ {
		fmt.Fprintf(&body, "%s %d\n", prefix, i)
	}
	return body.Bytes()
}

// diffLineMatrixBytes is the size of the table the line diff allocates for two files of the given sizes, one int per
// pair of lines, which is what the guard in Diff estimates.
func diffLineMatrixBytes(lines int) int64 {
	return int64(lines) * int64(lines) * int64(unsafe.Sizeof(int(0)))
}

// The line diff of a file is quadratic, so a pair of files whose differing lines would need an unreasonable amount of
// memory is compared by hash instead, and the command says why.  A file that differs in a few lines only, however
// large, still gets the exact diff, which is the case the guard must not break.
func TestDiffComparesLargeFilesByHash(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	now := time.Now().Unix()

	// Revision 1: three files.  'many.txt' is what the limit is set to catch, 'few.txt' differs in one line out of
	// many and must stay on the line diff, and 'same.txt' is byte-identical between the two revisions.
	leftMany := diffLineContent(60, "left")
	leftFew := diffLineContent(60, "common")
	leftSame := diffLineContent(60, "same")
	createTestSnapshotWithContent(snapshotManager, "vm1@host1", 1, now-3600, now,
		[]string{"many.txt", "few.txt", "same.txt"}, [][]byte{leftMany, leftFew, leftSame}, "tag")

	rightMany := diffLineContent(60, "right")
	rightFew := append([]byte{}, diffLineContent(60, "common")[:]...)
	rightFew = bytes.Replace(rightFew, []byte("common 30\n"), []byte("common 30 changed\n"), 1)
	rightSame := diffLineContent(60, "same")
	createTestSnapshotWithContent(snapshotManager, "vm1@host1", 2, now, now+3600,
		[]string{"many.txt", "few.txt", "same.txt"}, [][]byte{rightMany, rightFew, rightSame}, "tag")

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	// A limit of 4 KB: 61 x 61 lines needs about 30 KB (4 KB on a 32-bit build), 61 x 1 lines needs a few bytes, so
	// 'many.txt' must be compared by hash and 'few.txt' must not be.
	if !snapshotManager.Diff(testDir, "vm1@host1", []int{1, 2}, "many.txt", false, "", "", false, 4096) {
		t.Errorf("Diffing two revisions of a file failed: %v", capture.failures())
		return
	}

	if lines := len(capture.messages("SNAPSHOT_DIFF")); lines == 0 {
		t.Errorf("Diffing a large file should have printed the reason it was compared by hash")
	}

	// The message names the file and the lines it would have compared; the two hashes follow.
	messages := capture.messages("SNAPSHOT_DIFF")
	if !strings.Contains(messages[0], "many.txt") || !strings.Contains(messages[0], "compared by hash") {
		t.Errorf("Expecting the reason to name many.txt and the comparison that replaced the diff, got %q",
			messages[0])
	}
	if len(messages) < 3 {
		t.Errorf("Expecting the two hashes after the reason, got %v", messages)
	}

	// Raising the limit above the table size restores the exact diff of the same file: it prints the lines that differ
	// and the hash-reason message is gone.
	capture = &logCapture{}
	LogFunction = capture.log

	if !snapshotManager.Diff(testDir, "vm1@host1", []int{1, 2}, "many.txt", false, "", "", false,
		diffLineMatrixBytes(61)*4) {
		t.Errorf("Diffing two revisions of a file failed: %v", capture.failures())
		return
	}
	for _, message := range capture.messages("SNAPSHOT_DIFF") {
		if strings.Contains(message, "compared by hash") {
			t.Errorf("A file under the limit should have been diffed line by line, got %q", message)
		}
	}

	// A large file with a one-line change stays under the limit, because only the differing lines are counted.
	capture = &logCapture{}
	LogFunction = capture.log

	if !snapshotManager.Diff(testDir, "vm1@host1", []int{1, 2}, "few.txt", false, "", "", false, 4096) {
		t.Errorf("Diffing two revisions of a file failed: %v", capture.failures())
		return
	}
	for _, message := range capture.messages("SNAPSHOT_DIFF") {
		if strings.Contains(message, "compared by hash") {
			t.Errorf("A file with a one-line change should have been diffed line by line, got %q", message)
		}
	}
}

// The line diff must still be the default when no limit is given.
func TestDiffWithoutLimitUsesLineDiff(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)

	now := time.Now().Unix()
	left := diffLineContent(50, "left")
	right := diffLineContent(50, "right")
	createTestSnapshotWithContent(snapshotManager, "vm1@host1", 1, now-3600, now, []string{"f.txt"}, [][]byte{left}, "tag")
	createTestSnapshotWithContent(snapshotManager, "vm1@host1", 2, now, now+3600, []string{"f.txt"}, [][]byte{right}, "tag")

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	if !snapshotManager.Diff(testDir, "vm1@host1", []int{1, 2}, "f.txt", false, "", "", false, 0) {
		t.Errorf("Diffing two revisions of a file failed: %v", capture.failures())
		return
	}
	for _, message := range capture.messages("SNAPSHOT_DIFF") {
		if strings.Contains(message, "compared by hash") {
			t.Errorf("A limit of zero should have disabled the guard, got %q", message)
		}
	}
}

// The two-revision comparison reads each entry and validates it against the length sequence; it never reads the chunk
// hash sequence, which only RetrieveFile indexes.  Expanding both sequences fetched one metadata chunk per side that no
// output depended on, so the whole-snapshot form now reads the length sequence alone, and the per-file form -- which
// RetrieveFile serves from ChunkHashes -- still reads the chunk sequence.
func TestDiffWholeSnapshotFetchesOnlyTheLengthSequence(t *testing.T) {

	setTestingT(t)

	defer recovering(t)

	testDir := path.Join(os.TempDir(), "duplicacy_test", "snapshot_test")

	snapshotManager := createTestSnapshotManager(testDir)
	counting := &instrumentedStorage{FileStorage: snapshotManager.storage.(*FileStorage)}
	snapshotManager.storage = counting

	now := time.Now().Unix()
	// The two revisions hold different content, so every sequence chunk is its own and none is shared between them.
	left := diffLineContent(5, "left")
	right := diffLineContent(5, "right")
	createTestSnapshotWithContent(snapshotManager, "vm1@host1", 1, now-3600, now, []string{"f.txt"}, [][]byte{left}, "tag")
	createTestSnapshotWithContent(snapshotManager, "vm1@host1", 2, now, now+3600, []string{"f.txt"}, [][]byte{right}, "tag")

	savedLogFunction := LogFunction
	capture := &logCapture{}
	LogFunction = capture.log
	defer func() {
		LogFunction = savedLogFunction
	}()

	counting.resetDownloadStats()

	if !snapshotManager.Diff(testDir, "vm1@host1", []int{1, 2}, "", false, "", "", false, 0) {
		t.Errorf("Diffing two revisions failed: %v", capture.failures())
		return
	}

	// Each side reads its file sequence (through ListRemoteFiles) and its length sequence (through the sequence call):
	// two metadata chunks per side, four in all.  The chunk sequence of each side would be a fifth and sixth.
	downloads := counting.chunkDownloadCounts()
	if len(downloads) != 4 {
		t.Errorf("Expecting the file and length sequences of the two revisions (4 metadata chunks), got %v", downloads)
	}
	for chunkPath, count := range downloads {
		if count != 1 {
			t.Errorf("The metadata chunk %s was downloaded %d times instead of once", chunkPath, count)
		}
	}

	// The per-file form does read the chunk sequence, because RetrieveFile indexes ChunkHashes; dropping it there would
	// make the file retrieval fail.  Both sequences are read on each side, so there are strictly more chunks than the
	// whole-snapshot form reads.
	counting.resetDownloadStats()

	if !snapshotManager.Diff(testDir, "vm1@host1", []int{1, 2}, "f.txt", false, "", "", false, 0) {
		t.Errorf("Diffing two revisions of a file failed: %v", capture.failures())
		return
	}

	if downloads := counting.chunkDownloadCounts(); len(downloads) <= 4 {
		t.Errorf("The per-file form should still read the chunk sequence as well, got %v", downloads)
	}
}
