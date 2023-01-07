// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
	"sort"

    "github.com/vmihailenco/msgpack"

)

// Snapshot represents a backup of the repository.
type Snapshot struct {
	Version       int
	ID            string // the snapshot id; must be different for different repositories
	Revision      int    // the revision number
	Options       string // options used to create this snapshot (some not included)
	Tag           string // user-assigned tag
	StartTime     int64  // at what time the snapshot was created
	EndTime       int64  // at what time the snapshot was done
	FileSize      int64  // total file size
	NumberOfFiles int64  // number of files

	// A sequence of chunks whose aggregated content is the json representation of 'Files'.
	FileSequence []string

	// A sequence of chunks whose aggregated content is the json representation of 'ChunkHashes'.
	ChunkSequence []string

	// A sequence of chunks whose aggregated content is the json representation of 'ChunkLengths'.
	LengthSequence []string

	ChunkHashes  []string // a sequence of chunks representing the file content
	ChunkLengths []int    // the length of each chunk

	Flag bool // used to mark certain snapshots for deletion or copy

}

// CreateEmptySnapshot creates an empty snapshot.
func CreateEmptySnapshot(id string) (snapshto *Snapshot) {
	return &Snapshot{
		Version:   1,
		ID:        id,
		Revision:  0,
		StartTime: time.Now().Unix(),
	}
}

type DirectoryListing struct {
	directory string
	files *[]Entry
}

func (snapshot *Snapshot) ListLocalFiles(top string, nobackupFile string,
								         filtersFile string, excludeByAttribute bool, listingChannel chan *Entry,
								         skippedDirectories *[]string, skippedFiles *[]string) {

	var patterns []string

	if filtersFile == "" {
		filtersFile = joinPath(GetDuplicacyPreferencePath(), "filters")
	}
	patterns = ProcessFilters(filtersFile)

	directories := make([]*Entry, 0, 256)
	directories = append(directories, CreateEntry("", 0, 0, 0))

	for len(directories) > 0 {

		directory := directories[len(directories)-1]
		directories = directories[:len(directories)-1]
		subdirectories, skipped, err := ListEntries(top, directory.Path, patterns, nobackupFile, excludeByAttribute, listingChannel)
		if err != nil {
			if directory.Path == "" {
				LOG_ERROR("LIST_FAILURE", "Failed to list the repository root: %v", err)
				return
			}
			LOG_WARN("LIST_FAILURE", "Failed to list subdirectory %s: %v", directory.Path, err)
			if skippedDirectories != nil {
				*skippedDirectories = append(*skippedDirectories, directory.Path)
			}
			continue
		}

		directories = append(directories, subdirectories...)

		if skippedFiles != nil {
			*skippedFiles = append(*skippedFiles, skipped...)
		}

	}
	close(listingChannel)
}

func (snapshot *Snapshot)ListRemoteFiles(config *Config, chunkOperator *ChunkOperator, entryOut func(*Entry) bool) {

	var chunks []string
	for _, chunkHash := range snapshot.FileSequence {
		chunks = append(chunks, chunkOperator.config.GetChunkIDFromHash(chunkHash))
	}

	var chunk *Chunk
	reader := NewSequenceReader(snapshot.FileSequence, func(chunkHash string) []byte {
		if chunk != nil {
			config.PutChunk(chunk)
		}
		chunk = chunkOperator.Download(chunkHash, 0, true)
		return chunk.GetBytes()
	})

	defer func() {
		if chunk != nil {
			config.PutChunk(chunk)
		}
	} ()

	// Normally if Version is 0 then the snapshot is created by CLI v2 but unfortunately CLI 3.0.1 does not set the
	// version bit correctly when copying old backups.  So we need to check the first byte -- if it is '[' then it is
	// the old format.  The new format starts with a string encoded in msgpack and the first byte can't be '['.
	if snapshot.Version == 0 || reader.GetFirstByte() == '['{
		LOG_INFO("SNAPSHOT_VERSION", "snapshot %s at revision %d is encoded in an old version format", snapshot.ID, snapshot.Revision)
		files := make([]*Entry, 0)
		decoder := json.NewDecoder(reader)

		// read open bracket
		_, err := decoder.Token()
		if err != nil {
			LOG_ERROR("SNAPSHOT_PARSE", "Failed to open the snapshot %s at revision %d: not a list of entries",
				snapshot.ID, snapshot.Revision)
			return
		}

		for decoder.More() {
			var entry Entry
			err = decoder.Decode(&entry)
			if err != nil {
				LOG_ERROR("SNAPSHOT_PARSE", "Failed to load files specified in the snapshot %s at revision %d: %v",
					snapshot.ID, snapshot.Revision, err)
				return
			}
			files = append(files, &entry)
		}

		sort.Sort(ByName(files))

		for _, file := range files {
			if !entryOut(file) {
				return
			}
		}
	} else if snapshot.Version == 1 {
		decoder := msgpack.NewDecoder(reader)

		lastEndChunk := 0

		// while the array contains values
		for _, err := decoder.PeekCode(); err != io.EOF; _, err = decoder.PeekCode() {
			if err != nil {
				LOG_ERROR("SNAPSHOT_PARSE", "Failed to parse the snapshot %s at revision %d: %v",
					snapshot.ID, snapshot.Revision, err)
				return
			}
			var entry Entry
			err = decoder.Decode(&entry)
			if err != nil {
				LOG_ERROR("SNAPSHOT_PARSE", "Failed to load the snapshot %s at revision %d: %v",
					snapshot.ID, snapshot.Revision, err)
				return
			}

			if entry.IsFile() {
				entry.StartChunk += lastEndChunk
				entry.EndChunk += entry.StartChunk
				lastEndChunk = entry.EndChunk
			}

			err = entry.check(snapshot.ChunkLengths)
			if err != nil {
				LOG_ERROR("SNAPSHOT_ENTRY", "Failed to load the snapshot %s at revision %d: %v",
					snapshot.ID, snapshot.Revision, err)
				return
			}

			if !entryOut(&entry) {
				return
			}
		}

	} else {
		LOG_ERROR("SNAPSHOT_VERSION", "snapshot %s at revision %d is encoded in unsupported version %d format",
				  snapshot.ID, snapshot.Revision, snapshot.Version)
		return
	}

}

func AppendPattern(patterns []string, new_pattern string) (new_patterns []string) {
	for _, pattern := range patterns {
		if pattern == new_pattern {
			LOG_INFO("SNAPSHOT_FILTER", "Ignoring duplicate pattern: %s ...", new_pattern)
			return patterns
		}
	}
	new_patterns = append(patterns, new_pattern)
	return new_patterns
}
func ProcessFilters(filtersFile string) (patterns []string) {
	patterns = ProcessFilterFile(filtersFile, make([]string, 0))

	LOG_DEBUG("REGEX_DEBUG", "There are %d compiled regular expressions stored", len(RegexMap))

	LOG_INFO("SNAPSHOT_FILTER", "Loaded %d include/exclude pattern(s)", len(patterns))

	if IsTracing() {
		for _, pattern := range patterns {
			LOG_TRACE("SNAPSHOT_PATTERN", "Pattern: %s", pattern)
		}

	}

	return patterns
}

func ProcessFilterFile(patternFile string, includedFiles []string) (patterns []string) {
	for _, file := range includedFiles {
		if file == patternFile {
			// cycle in include mechanism discovered.
			LOG_ERROR("SNAPSHOT_FILTER", "The filter file %s has already been included", patternFile)
			return patterns
		}
	}
	includedFiles = append(includedFiles, patternFile)
	LOG_INFO("SNAPSHOT_FILTER", "Parsing filter file %s", patternFile)
	patternFileContent, err := ioutil.ReadFile(patternFile)
	if err == nil {
		patternFileLines := strings.Split(string(patternFileContent), "\n")
		patterns = ProcessFilterLines(patternFileLines, includedFiles)
	}
	return patterns
}

func ProcessFilterLines(patternFileLines []string, includedFiles []string) (patterns []string) {
	for _, pattern := range patternFileLines {
		pattern = strings.TrimSpace(pattern)
		if len(pattern) == 0 {
			continue
		}

		if strings.HasPrefix(pattern, "@") {
			patternIncludeFile := strings.TrimSpace(pattern[1:])
			if patternIncludeFile == "" {
				continue
			}
			if ! filepath.IsAbs(patternIncludeFile) {
				basePath := ""
				if len(includedFiles) == 0 {
					basePath, _ = os.Getwd()
				} else {
					basePath = filepath.Dir(includedFiles[len(includedFiles)-1])
				}
				patternIncludeFile = joinPath(basePath, patternIncludeFile)
			}
			for _, pattern := range ProcessFilterFile(patternIncludeFile, includedFiles) {
				patterns = AppendPattern(patterns, pattern)
			}
			continue
		}

		if pattern[0] == '#' {
			continue
		}

		if IsUnspecifiedFilter(pattern) {
			pattern = "+" + pattern
		}

		if IsEmptyFilter(pattern) {
			continue
		}

		if strings.HasPrefix(pattern, "i:") || strings.HasPrefix(pattern, "e:") {
			valid, err := IsValidRegex(pattern[2:])
			if !valid || err != nil {
				LOG_ERROR("SNAPSHOT_FILTER", "Invalid regular expression encountered for filter: \"%s\", error: %v", pattern, err)
			}
		}

		patterns = AppendPattern(patterns, pattern)
	}

	return patterns
}

// CreateSnapshotFromDescription creates a snapshot from json decription.
func CreateSnapshotFromDescription(description []byte) (snapshot *Snapshot, err error) {

	var root map[string]interface{}

	err = json.Unmarshal(description, &root)
	if err != nil {
		return nil, err
	}

	snapshot = &Snapshot{}

	if value, ok := root["version"]; !ok {
		snapshot.Version = 0
	} else if version, ok := value.(float64); !ok {
		return nil, fmt.Errorf("Invalid version is specified in the snapshot")
	} else {
		snapshot.Version = int(version)
	}

	if value, ok := root["id"]; !ok {
		return nil, fmt.Errorf("No id is specified in the snapshot")
	} else if snapshot.ID, ok = value.(string); !ok {
		return nil, fmt.Errorf("Invalid id is specified in the snapshot")
	}

	if value, ok := root["revision"]; !ok {
		return nil, fmt.Errorf("No revision is specified in the snapshot")
	} else if _, ok = value.(float64); !ok {
		return nil, fmt.Errorf("Invalid revision is specified in the snapshot")
	} else {
		snapshot.Revision = int(value.(float64))
	}

	if value, ok := root["tag"]; !ok {
	} else if snapshot.Tag, ok = value.(string); !ok {
		return nil, fmt.Errorf("Invalid tag is specified in the snapshot")
	}

	if value, ok := root["options"]; !ok {
	} else if snapshot.Options, ok = value.(string); !ok {
		return nil, fmt.Errorf("Invalid options is specified in the snapshot")
	}

	if value, ok := root["start_time"]; !ok {
		return nil, fmt.Errorf("No creation time is specified in the snapshot")
	} else if _, ok = value.(float64); !ok {
		return nil, fmt.Errorf("Invalid creation time is specified in the snapshot")
	} else {
		snapshot.StartTime = int64(value.(float64))
	}

	if value, ok := root["end_time"]; !ok {
		return nil, fmt.Errorf("No creation time is specified in the snapshot")
	} else if _, ok = value.(float64); !ok {
		return nil, fmt.Errorf("Invalid creation time is specified in the snapshot")
	} else {
		snapshot.EndTime = int64(value.(float64))
	}

	if value, ok := root["file_size"]; ok {
		if _, ok = value.(float64); ok {
			snapshot.FileSize = int64(value.(float64))
		}
	}

	if value, ok := root["number_of_files"]; ok {
		if _, ok = value.(float64); ok {
			snapshot.NumberOfFiles = int64(value.(float64))
		}
	}

	for _, sequenceType := range []string{"files", "chunks", "lengths"} {
		if value, ok := root[sequenceType]; !ok {
			return nil, fmt.Errorf("No %s are specified in the snapshot", sequenceType)
		} else if _, ok = value.([]interface{}); !ok {
			return nil, fmt.Errorf("Invalid %s are specified in the snapshot", sequenceType)
		} else {
			array := value.([]interface{})
			sequence := make([]string, len(array))
			for i := 0; i < len(array); i++ {
				if hashInHex, ok := array[i].(string); !ok {
					return nil, fmt.Errorf("Invalid file sequence is specified in the snapshot")
				} else if hash, err := hex.DecodeString(hashInHex); err != nil {
					return nil, fmt.Errorf("Hash %s is not a valid hex string in the snapshot", hashInHex)
				} else {
					sequence[i] = string(hash)
				}
			}

			snapshot.SetSequence(sequenceType, sequence)
		}
	}

	return snapshot, nil
}

// LoadChunks construct 'ChunkHashes' from the json description.
func (snapshot *Snapshot) LoadChunks(description []byte) (err error) {

	var root []interface{}
	err = json.Unmarshal(description, &root)
	if err != nil {
		return err
	}

	snapshot.ChunkHashes = make([]string, len(root))

	for i, object := range root {
		if hashInHex, ok := object.(string); !ok {
			return fmt.Errorf("Invalid chunk hash is specified in the snapshot")
		} else if hash, err := hex.DecodeString(hashInHex); err != nil {
			return fmt.Errorf("The chunk hash %s is not a valid hex string", hashInHex)
		} else {
			snapshot.ChunkHashes[i] = string(hash)
		}
	}

	return err
}

// ClearChunks removes loaded chunks from memory
func (snapshot *Snapshot) ClearChunks() {
	snapshot.ChunkHashes = nil
}

// LoadLengths construct 'ChunkLengths' from the json description.
func (snapshot *Snapshot) LoadLengths(description []byte) (err error) {
	return json.Unmarshal(description, &snapshot.ChunkLengths)
}

// MarshalJSON creates a json representation of the snapshot.
func (snapshot *Snapshot) MarshalJSON() ([]byte, error) {

	object := make(map[string]interface{})

	object["version"] = snapshot.Version
	object["id"] = snapshot.ID
	object["revision"] = snapshot.Revision
	object["options"] = snapshot.Options
	object["tag"] = snapshot.Tag
	object["start_time"] = snapshot.StartTime
	object["end_time"] = snapshot.EndTime

	if snapshot.FileSize != 0 && snapshot.NumberOfFiles != 0 {
		object["file_size"] = snapshot.FileSize
		object["number_of_files"] = snapshot.NumberOfFiles
	}
	object["files"] = encodeSequence(snapshot.FileSequence)
	object["chunks"] = encodeSequence(snapshot.ChunkSequence)
	object["lengths"] = encodeSequence(snapshot.LengthSequence)

	return json.Marshal(object)
}

// MarshalSequence creates a json represetion for the specified chunk sequence.
func (snapshot *Snapshot) MarshalSequence(sequenceType string) ([]byte, error) {

	if sequenceType == "chunks" {
		return json.Marshal(encodeSequence(snapshot.ChunkHashes))
	} else {
		return json.Marshal(snapshot.ChunkLengths)
	}
}

// SetSequence assign a chunk sequence to the specified field.
func (snapshot *Snapshot) SetSequence(sequenceType string, sequence []string) {
	if sequenceType == "files" {
		snapshot.FileSequence = sequence
	} else if sequenceType == "chunks" {
		snapshot.ChunkSequence = sequence
	} else {
		snapshot.LengthSequence = sequence
	}
}

// encodeSequence turns a sequence of binary hashes into a sequence of hex hashes.
func encodeSequence(sequence []string) []string {

	sequenceInHex := make([]string, len(sequence))

	for i, hash := range sequence {
		sequenceInHex[i] = hex.EncodeToString([]byte(hash))
	}

	return sequenceInHex
}

