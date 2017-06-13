// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "os"
    "fmt"
    "time"
    "path"
    "strings"
    "strconv"
    "io/ioutil"
    "encoding/json"
    "encoding/hex"
)

// Snapshot represents a backup of the repository.
type Snapshot struct {
    ID             string      // the snapshot id; must be different for different repositories
    Revision       int         // the revision number
    Options        string      // options used to create this snapshot (some not included)
    Tag            string      // user-assigned tag
    StartTime      int64       // at what time the snapshot was created
    EndTime        int64       // at what time the snapshot was done
    FileSize       int64       // total file size
    NumberOfFiles  int64       // number of files

    // A sequence of chunks whose aggregated content is the json representation of 'Files'.
    FileSequence   []string

    // A sequence of chunks whose aggregated content is the json representation of 'ChunkHashes'.
    ChunkSequence  []string

    // A sequence of chunks whose aggregated content is the json representation of 'ChunkLengths'.
    LengthSequence []string

    Files          []*Entry    // list of files and subdirectories

    ChunkHashes    []string    // a sequence of chunks representing the file content
    ChunkLengths   []int       // the length of each chunk

    Flag           bool        // used to mark certain snapshots for deletion or copy

    discardAttributes bool
}

// CreateEmptySnapshot creates an empty snapshot.
func CreateEmptySnapshot (id string) (snapshto *Snapshot) {
    return &Snapshot{
        ID : id,
        Revision : 0,
        StartTime: time.Now().Unix(),
    }
}

// CreateSnapshotFromDirectory creates a snapshot from the local directory 'top'.  Only 'Files'
// will be constructed, while 'ChunkHashes' and 'ChunkLengths' can only be populated after uploading.
func CreateSnapshotFromDirectory(id string, top string) (snapshot *Snapshot, skippedDirectories []string,
                                                         skippedFiles []string, err error) {

    snapshot = &Snapshot {
        ID : id,
        Revision: 0,
        StartTime: time.Now().Unix(),
    }

    var patterns []string
    
    preferencePath := GetDuplicacyPreferencePath(top)
    patternFile, err := ioutil.ReadFile(path.Join(preferencePath, "filters"))
    if err == nil {
        for _, pattern := range strings.Split(string(patternFile), "\n") {
            pattern = strings.TrimSpace(pattern)
            if len(pattern) == 0 {
                continue
            }

            if pattern[0] != '+' && pattern[0] != '-' {
                pattern = "+" + pattern
            }

            if pattern == "+" || pattern == "-" {
                continue
            }

            patterns = append(patterns, pattern)
        }

        LOG_INFO("SNAPSHOT_FILTER", "Loaded %d include/exclude pattern(s)", len(patterns))

        if IsTracing() {
            for _, pattern := range patterns {
                LOG_TRACE("SNAPSHOT_PATTERN", "Pattern: %s", pattern)
            }
        }

    }

    directories := make([]*Entry, 0, 256)
    directories = append(directories, CreateEntry("", 0, 0, 0))

    snapshot.Files = make([]*Entry, 0, 256)

    attributeThreshold := 1024 * 1024
    if attributeThresholdValue, found := os.LookupEnv("DUPLICACY_ATTRIBUTE_THRESHOLD"); found && attributeThresholdValue != "" {
        attributeThreshold, _ = strconv.Atoi(attributeThresholdValue)
    }

    for len(directories) > 0 {

        directory := directories[len(directories) - 1]
        directories = directories[:len(directories) - 1]
        snapshot.Files = append(snapshot.Files, directory)
        subdirectories, skipped, err := ListEntries(top, directory.Path, &snapshot.Files, patterns, snapshot.discardAttributes)
        if err != nil {
            LOG_WARN("LIST_FAILURE", "Failed to list subdirectory: %v", err)
            skippedDirectories = append(skippedDirectories, directory.Path)
            continue
        }

        directories = append(directories, subdirectories...)
        skippedFiles = append(skippedFiles, skipped...)

        if !snapshot.discardAttributes && len(snapshot.Files) > attributeThreshold {
            LOG_INFO("LIST_ATTRIBUTES", "Discarding file attributes")
            snapshot.discardAttributes = true
            for _, file := range snapshot.Files {
                file.Attributes = nil
            }
        }
    }

    // Remove the root entry
    snapshot.Files = snapshot.Files[1:]

    return snapshot, skippedDirectories, skippedFiles, nil
}

// CreateSnapshotFromDescription creates a snapshot from json decription.
func CreateSnapshotFromDescription(description []byte) (snapshot *Snapshot, err error) {

    var root map[string] interface{}

    err = json.Unmarshal(description, &root)
    if err != nil {
        return nil, err
    }

    snapshot = &Snapshot {}

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

    for _, sequenceType := range []string { "files", "chunks", "lengths" } {
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

    var root [] interface {}
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

// LoadLengths construct 'ChunkLengths' from the json description.
func (snapshot *Snapshot) LoadLengths(description []byte) (err error) {
    return json.Unmarshal(description, &snapshot.ChunkLengths)
}

// MarshalJSON creates a json representation of the snapshot.
func (snapshot *Snapshot) MarshalJSON() ([] byte, error) {

    object := make(map[string]interface{})

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
func (snapshot *Snapshot) MarshalSequence(sequenceType string) ([] byte, error) {

    if sequenceType == "files" {
        return json.Marshal(snapshot.Files)
    } else if sequenceType == "chunks" {
        return json.Marshal(encodeSequence(snapshot.ChunkHashes))
    } else {
        return json.Marshal(snapshot.ChunkLengths)
    }
}

// SetSequence assign a chunk sequence to the specified field.
func (snapshot *Snapshot) SetSequence(sequenceType string, sequence [] string) {
    if sequenceType == "files" {
        snapshot.FileSequence = sequence
    } else if sequenceType == "chunks" {
        snapshot.ChunkSequence = sequence
    } else {
        snapshot.LengthSequence = sequence
    }
}

// encodeSequence turns a sequence of binary hashes into a sequence of hex hashes.
func encodeSequence(sequence[] string) ([] string) {

    sequenceInHex := make([]string, len(sequence))

    for i, hash := range sequence {
        sequenceInHex[i] = hex.EncodeToString([]byte(hash))
    }

    return sequenceInHex
}


