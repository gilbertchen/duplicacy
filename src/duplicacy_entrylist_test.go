// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"os"
	"path"
	"time"
	"testing"
	"math/rand"
)


func generateRandomString(length int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")
    b := make([]rune, length)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

var fileSizeGenerator = rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.2, 1.0, 1024)

func generateRandomFileSize() int64 {
	return int64(fileSizeGenerator.Uint64() + 1)
}

func generateRandomChunks(totalFileSize int64) (chunks []string, lengths []int) {

	totalChunkSize := int64(0)

	for totalChunkSize < totalFileSize {
		chunks = append(chunks, generateRandomString(64))
		chunkSize := int64(1 + (rand.Int() % 64))
		if chunkSize + totalChunkSize > totalFileSize {
			chunkSize = totalFileSize - totalChunkSize
		}
		lengths = append(lengths, int(chunkSize))
		totalChunkSize += chunkSize
	}
	return chunks, lengths

}

func getPreservedChunks(entries []*Entry, chunks []string, lengths []int) (preservedChunks []string, preservedChunkLengths []int) {
	lastPreservedChunk := -1
	for i := range entries {
		if entries[i].Size < 0 {
			continue
		}
		delta := entries[i].StartChunk - len(chunks)
		if lastPreservedChunk != entries[i].StartChunk {
			lastPreservedChunk = entries[i].StartChunk
			preservedChunks = append(preservedChunks, chunks[entries[i].StartChunk])
			preservedChunkLengths = append(preservedChunkLengths, lengths[entries[i].StartChunk])
			delta++
		}
		for j := entries[i].StartChunk + 1; i <= entries[i].EndChunk; i++ {
			preservedChunks = append(preservedChunks, chunks[j])
			preservedChunkLengths = append(preservedChunkLengths, lengths[j])
			lastPreservedChunk = j
		}
	}

	return
}

func testEntryList(t *testing.T, numberOfEntries int, maximumInMemoryEntries int) {

	entries := make([]*Entry, 0, numberOfEntries)
	entrySizes := make([]int64, 0)

	for i := 0; i < numberOfEntries; i++ {
		entry:= CreateEntry(generateRandomString(16), -1, 0, 0700)
		entries = append(entries, entry)
		entrySizes = append(entrySizes, generateRandomFileSize())
	}

	totalFileSize := int64(0)
	for _, size := range entrySizes {
		totalFileSize += size
	}

	testDir := path.Join(os.TempDir(), "duplicacy_test")
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	os.MkdirAll(testDir + "/list1", 0700)
	os.MkdirAll(testDir + "/list2", 0700)
	os.MkdirAll(testDir + "/list3", 0700)
	os.MkdirAll(testDir + "/list1", 0700)

	// For the first entry list, all entries are new
	entryList, _ := CreateEntryList("test", testDir + "/list1", maximumInMemoryEntries)
	for _, entry := range entries {
		entryList.AddEntry(entry)
	}
	uploadedChunks, uploadedChunksLengths := generateRandomChunks(totalFileSize)
	for i, chunk := range uploadedChunks {
		entryList.AddUploadedChunk(i, chunk, uploadedChunksLengths[i])
	}

	for i := range entryList.ModifiedEntries {
		entryList.ModifiedEntries[i].Size = entrySizes[i]
	}

	totalEntries := 0
	err := entryList.ReadEntries(func(entry *Entry) error {
		totalEntries++
		return nil
	})

	if err != nil {
		t.Errorf("ReadEntries returned an error: %s", err)
		return
	}

	if totalEntries != numberOfEntries {
		t.Errorf("EntryList contains %d entries instead of %d", totalEntries, numberOfEntries)
		return
	}

	// For the second entry list, half of the entries are new
	for i := range entries {
		if rand.Int() % 1 == 0 {
			entries[i].Size = -1
		} else {
			entries[i].Size = entrySizes[i]
		}
	}

	preservedChunks, preservedChunkLengths := getPreservedChunks(entries, uploadedChunks, uploadedChunksLengths)
	entryList, _ = CreateEntryList("test", testDir + "/list2", maximumInMemoryEntries)
	for _, entry := range entries {
		entryList.AddEntry(entry)
	}
	for i, chunk := range preservedChunks {
		entryList.AddPreservedChunk(chunk, preservedChunkLengths[i])
	}

	totalFileSize = 0
	for i := range entryList.ModifiedEntries {
		fileSize := generateRandomFileSize()		
		entryList.ModifiedEntries[i].Size = fileSize
		totalFileSize += fileSize
	}

	uploadedChunks, uploadedChunksLengths = generateRandomChunks(totalFileSize)
	for i, chunk := range uploadedChunks {
		entryList.AddUploadedChunk(i, chunk, uploadedChunksLengths[i])
	}

	totalEntries = 0
	err = entryList.ReadEntries(func(entry *Entry) error {
		totalEntries++
		return nil
	})

	if err != nil {
		t.Errorf("ReadEntries returned an error: %s", err)
		return
	}

	if totalEntries != numberOfEntries {
		t.Errorf("EntryList contains %d entries instead of %d", totalEntries, numberOfEntries)
		return
	}

}


func TestEntryList(t *testing.T) {
	testEntryList(t, 1024, 1024)
	testEntryList(t, 1024, 512)
	testEntryList(t, 1024, 0)
}
