// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com
package duplicacy

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This is the hidden directory in the repository for storing various files.
var DUPLICACY_DIRECTORY = ".duplicacy"
var DUPLICACY_FILE = ".duplicacy"

// Regex for matching 'StartChunk:StartOffset:EndChunk:EndOffset'
var contentRegex = regexp.MustCompile(`^([0-9]+):([0-9]+):([0-9]+):([0-9]+)`)

// Entry encapsulates information about a file or directory.
type Entry struct {
	Path string
	Size int64
	Time int64
	Mode uint32
	Link string
	Hash string

	UID int
	GID int

	StartChunk  int
	StartOffset int
	EndChunk    int
	EndOffset   int

	Attributes map[string][]byte
}

// CreateEntry creates an entry from file properties.
func CreateEntry(path string, size int64, time int64, mode uint32) *Entry {

	if len(path) > 0 && path[len(path)-1] != '/' && (mode&uint32(os.ModeDir)) != 0 {
		path += "/"
	}

	return &Entry{
		Path: path,
		Size: size,
		Time: time,
		Mode: mode,

		UID: -1,
		GID: -1,
	}

}

// CreateEntryFromFileInfo creates an entry from a 'FileInfo' object.
func CreateEntryFromFileInfo(fileInfo os.FileInfo, directory string) *Entry {
	path := directory + fileInfo.Name()

	mode := fileInfo.Mode()

	if mode&os.ModeDir != 0 && mode&os.ModeSymlink != 0 {
		mode ^= os.ModeDir
	}

	if path[len(path)-1] != '/' && mode&os.ModeDir != 0 {
		path += "/"
	}

	entry := &Entry{
		Path: path,
		Size: fileInfo.Size(),
		Time: fileInfo.ModTime().Unix(),
		Mode: uint32(mode),
	}

	GetOwner(entry, &fileInfo)

	return entry
}

// CreateEntryFromJSON creates an entry from a json description.
func (entry *Entry) UnmarshalJSON(description []byte) (err error) {

	var object map[string]interface{}

	err = json.Unmarshal(description, &object)
	if err != nil {
		return err
	}

	var value interface{}
	var ok bool

	if value, ok = object["name"]; ok {
		pathInBase64, ok := value.(string)
		if !ok {
			return fmt.Errorf("Name is not a string for a file in the snapshot")
		}
		path, err := base64.StdEncoding.DecodeString(pathInBase64)
		if err != nil {
			return fmt.Errorf("Invalid name '%s' in the snapshot", pathInBase64)
		}
		entry.Path = string(path)
	} else if value, ok = object["path"]; !ok {
		return fmt.Errorf("Path is not specified for a file in the snapshot")
	} else if entry.Path, ok = value.(string); !ok {
		return fmt.Errorf("Path is not a string for a file in the snapshot")
	}

	if value, ok = object["size"]; !ok {
		return fmt.Errorf("Size is not specified for file '%s' in the snapshot", entry.Path)
	} else if _, ok = value.(float64); !ok {
		return fmt.Errorf("Size is not a valid integer for file '%s' in the snapshot", entry.Path)
	}
	entry.Size = int64(value.(float64))

	if value, ok = object["time"]; !ok {
		return fmt.Errorf("Time is not specified for file '%s' in the snapshot", entry.Path)
	} else if _, ok = value.(float64); !ok {
		return fmt.Errorf("Time is not a valid integer for file '%s' in the snapshot", entry.Path)
	}
	entry.Time = int64(value.(float64))

	if value, ok = object["mode"]; !ok {
		return fmt.Errorf("float64 is not specified for file '%s' in the snapshot", entry.Path)
	} else if _, ok = value.(float64); !ok {
		return fmt.Errorf("Mode is not a valid integer for file '%s' in the snapshot", entry.Path)
	}
	entry.Mode = uint32(value.(float64))

	if value, ok = object["hash"]; !ok {
		return fmt.Errorf("Hash is not specified for file '%s' in the snapshot", entry.Path)
	} else if entry.Hash, ok = value.(string); !ok {
		return fmt.Errorf("Hash is not a string for file '%s' in the snapshot", entry.Path)
	}

	if value, ok = object["link"]; ok {
		var link string
		if link, ok = value.(string); !ok {
			return fmt.Errorf("Symlink is not a valid string for file '%s' in the snapshot", entry.Path)
		}
		entry.Link = link
	}

	entry.UID = -1
	if value, ok = object["uid"]; ok {
		if _, ok = value.(float64); ok {
			entry.UID = int(value.(float64))
		}
	}

	entry.GID = -1
	if value, ok = object["gid"]; ok {
		if _, ok = value.(float64); ok {
			entry.GID = int(value.(float64))
		}
	}

	if value, ok = object["attributes"]; ok {
		if attributes, ok := value.(map[string]interface{}); !ok {
			return fmt.Errorf("Attributes are invalid for file '%s' in the snapshot", entry.Path)
		} else {
			entry.Attributes = make(map[string][]byte)
			for name, object := range attributes {
				if object == nil {
					entry.Attributes[name] = []byte("")
				} else if attributeInBase64, ok := object.(string); !ok {
					return fmt.Errorf("Attribute '%s' is invalid for file '%s' in the snapshot", name, entry.Path)
				} else if attribute, err := base64.StdEncoding.DecodeString(attributeInBase64); err != nil {
					return fmt.Errorf("Failed to decode attribute '%s' for file '%s' in the snapshot: %v",
						name, entry.Path, err)
				} else {
					entry.Attributes[name] = attribute
				}
			}
		}
	}

	if entry.IsFile() && entry.Size > 0 {
		if value, ok = object["content"]; !ok {
			return fmt.Errorf("Content is not specified for file '%s' in the snapshot", entry.Path)
		}

		if content, ok := value.(string); !ok {
			return fmt.Errorf("Content is invalid for file '%s' in the snapshot", entry.Path)
		} else {

			matched := contentRegex.FindStringSubmatch(content)
			if matched == nil {
				return fmt.Errorf("Content is specified in a wrong format for file '%s' in the snapshot", entry.Path)
			}

			entry.StartChunk, _ = strconv.Atoi(matched[1])
			entry.StartOffset, _ = strconv.Atoi(matched[2])
			entry.EndChunk, _ = strconv.Atoi(matched[3])
			entry.EndOffset, _ = strconv.Atoi(matched[4])
		}
	}

	return nil

}

func (entry *Entry) convertToObject(encodeName bool) map[string]interface{} {

	object := make(map[string]interface{})

	if encodeName {
		object["name"] = base64.StdEncoding.EncodeToString([]byte(entry.Path))
	} else {
		object["path"] = entry.Path
	}
	object["size"] = entry.Size
	object["time"] = entry.Time
	object["mode"] = entry.Mode
	object["hash"] = entry.Hash

	if entry.IsLink() {
		object["link"] = entry.Link
	}

	if entry.IsFile() && entry.Size > 0 {
		object["content"] = fmt.Sprintf("%d:%d:%d:%d",
			entry.StartChunk, entry.StartOffset, entry.EndChunk, entry.EndOffset)
	}

	if entry.UID != -1 && entry.GID != -1 {
		object["uid"] = entry.UID
		object["gid"] = entry.GID
	}

	if len(entry.Attributes) > 0 {
		object["attributes"] = entry.Attributes
	}

	return object
}

// MarshalJSON returns the json description of an entry.
func (entry *Entry) MarshalJSON() ([]byte, error) {

	object := entry.convertToObject(true)
	description, err := json.Marshal(object)
	return description, err
}

func (entry *Entry) IsFile() bool {
	return entry.Mode&uint32(os.ModeType) == 0
}

func (entry *Entry) IsDir() bool {
	return entry.Mode&uint32(os.ModeDir) != 0
}

func (entry *Entry) IsLink() bool {
	return entry.Mode&uint32(os.ModeSymlink) != 0
}

func (entry *Entry) GetPermissions() os.FileMode {
	return os.FileMode(entry.Mode) & os.ModePerm
}

func (entry *Entry) IsSameAs(other *Entry) bool {
	return entry.Size == other.Size && entry.Time <= other.Time+1 && entry.Time >= other.Time-1
}

func (entry *Entry) IsSameAsFileInfo(other os.FileInfo) bool {
	time := other.ModTime().Unix()
	return entry.Size == other.Size() && entry.Time <= time+1 && entry.Time >= time-1
}

func (entry *Entry) String(maxSizeDigits int) string {
	modifiedTime := time.Unix(entry.Time, 0).Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%*d %s %64s %s", maxSizeDigits, entry.Size, modifiedTime, entry.Hash, entry.Path)
}

func (entry *Entry) RestoreMetadata(fullPath string, fileInfo *os.FileInfo, setOwner bool) bool {

	if fileInfo == nil {
		stat, err := os.Stat(fullPath)
		fileInfo = &stat
		if err != nil {
			LOG_ERROR("RESTORE_STAT", "Failed to retrieve the file info: %v", err)
			return false
		}
	}

	if (*fileInfo).Mode()&os.ModePerm != entry.GetPermissions() {
		err := os.Chmod(fullPath, entry.GetPermissions())
		if err != nil {
			LOG_ERROR("RESTORE_CHMOD", "Failed to set the file permissions: %v", err)
			return false
		}
	}

	if (*fileInfo).ModTime().Unix() != entry.Time {
		modifiedTime := time.Unix(entry.Time, 0)
		err := os.Chtimes(fullPath, modifiedTime, modifiedTime)
		if err != nil {
			LOG_ERROR("RESTORE_CHTIME", "Failed to set the modification time: %v", err)
			return false
		}
	}

	if len(entry.Attributes) > 0 {
		entry.SetAttributesToFile(fullPath)
	}

	if setOwner {
		return SetOwner(fullPath, entry, fileInfo)
	} else {
		return true
	}
}

// Return -1 if 'left' should appear before 'right', 1 if opposite, and 0 if they are the same.
// Files are always arranged before subdirectories under the same parent directory.
func (left *Entry) Compare(right *Entry) int {

	path1 := left.Path
	path2 := right.Path

	p := 0
	for ; p < len(path1) && p < len(path2); p++ {
		if path1[p] != path2[p] {
			break
		}
	}

	// c1, c2 is the first byte that differs
	var c1, c2 byte
	if p < len(path1) {
		c1 = path1[p]
	}
	if p < len(path2) {
		c2 = path2[p]
	}

	// c3, c4 indicates how the current component ends
	// c3 == '/':  the current component is a directory
	// c3 != '/':  the current component is the last one
	c3 := c1
	for i := p; c3 != '/' && i < len(path1); i++ {
		c3 = path1[i]
	}

	c4 := c2
	for i := p; c4 != '/' && i < len(path2); i++ {
		c4 = path2[i]
	}

	if c3 == '/' {
		if c4 == '/' {
			// We are comparing two directory components
			if c1 == '/' {
				// left is shorter
				// Note that c2 maybe smaller than c1 but c1 is '/' which is counted
				// as 0
				return -1
			} else if c2 == '/' {
				// right is shorter
				return 1
			} else {
				return int(c1) - int(c2)
			}
		} else {
			return 1
		}
	} else {
		// We're at the last component of left and left is a file
		if c4 == '/' {
			// the current component of right is a directory
			return -1
		} else {
			return int(c1) - int(c2)
		}
	}
}

// This is used to sort entries by their names.
type ByName []*Entry

func (entries ByName) Len() int      { return len(entries) }
func (entries ByName) Swap(i, j int) { entries[i], entries[j] = entries[j], entries[i] }
func (entries ByName) Less(i, j int) bool {
	return entries[i].Compare(entries[j]) < 0
}

// This is used to sort entries by their starting chunks (and starting offsets if the starting chunks are the same).
type ByChunk []*Entry

func (entries ByChunk) Len() int      { return len(entries) }
func (entries ByChunk) Swap(i, j int) { entries[i], entries[j] = entries[j], entries[i] }
func (entries ByChunk) Less(i, j int) bool {
	return entries[i].StartChunk < entries[j].StartChunk ||
		(entries[i].StartChunk == entries[j].StartChunk && entries[i].StartOffset < entries[j].StartOffset)
}

// This is used to sort FileInfo objects.
type FileInfoCompare []os.FileInfo

func (files FileInfoCompare) Len() int      { return len(files) }
func (files FileInfoCompare) Swap(i, j int) { files[i], files[j] = files[j], files[i] }
func (files FileInfoCompare) Less(i, j int) bool {

	left := files[i]
	right := files[j]

	if left.IsDir() && left.Mode()&os.ModeSymlink == 0 {
		if right.IsDir() && right.Mode()&os.ModeSymlink == 0 {
			return left.Name() < right.Name()
		} else {
			return false
		}
	} else {
		if right.IsDir() && right.Mode()&os.ModeSymlink == 0 {
			return true
		} else {
			return left.Name() < right.Name()
		}
	}
}

// ListEntries returns a list of entries representing file and subdirectories under the directory 'path'.  Entry paths
// are normalized as relative to 'top'.  'patterns' are used to exclude or include certain files.
func ListEntries(top string, path string, fileList *[]*Entry, patterns []string, discardAttributes bool) (directoryList []*Entry,
	skippedFiles []string, err error) {

	LOG_DEBUG("LIST_ENTRIES", "Listing %s", path)

	fullPath := joinPath(top, path)

	files := make([]os.FileInfo, 0, 1024)

	files, err = ioutil.ReadDir(fullPath)
	if err != nil {
		return directoryList, nil, err
	}

	normalizedPath := path
	if len(normalizedPath) > 0 && normalizedPath[len(normalizedPath)-1] != '/' {
		normalizedPath += "/"
	}

	normalizedTop := top
	if normalizedTop != "" && normalizedTop[len(normalizedTop)-1] != '/' {
		normalizedTop += "/"
	}

	sort.Sort(FileInfoCompare(files))

	entries := make([]*Entry, 0, 4)

	for _, f := range files {
		if f.Name() == DUPLICACY_DIRECTORY {
			continue
		}
		entry := CreateEntryFromFileInfo(f, normalizedPath)
		if len(patterns) > 0 && !MatchPath(entry.Path, patterns) {
			LOG_DEBUG("LIST_EXCLUDE", "%s is excluded", entry.Path)
			continue
		}
		if entry.IsLink() {
			isRegular := false
			isRegular, entry.Link, err = Readlink(filepath.Join(top, entry.Path))
			if err != nil {
				LOG_WARN("LIST_LINK", "Failed to read the symlink %s: %v", entry.Path, err)
				skippedFiles = append(skippedFiles, entry.Path)
				continue
			}

			if isRegular {
				entry.Mode ^= uint32(os.ModeSymlink)
			} else if path == "" && (filepath.IsAbs(entry.Link) || filepath.HasPrefix(entry.Link, `\\`)) && !strings.HasPrefix(entry.Link, normalizedTop) {
				stat, err := os.Stat(filepath.Join(top, entry.Path))
				if err != nil {
					LOG_WARN("LIST_LINK", "Failed to read the symlink: %v", err)
					skippedFiles = append(skippedFiles, entry.Path)
					continue
				}

				newEntry := CreateEntryFromFileInfo(stat, "")
				if runtime.GOOS == "windows" {
					// On Windows, stat.Name() is the last component of the target, so we need to construct the correct
					// path from f.Name(); note that a "/" is append assuming a symbolic link is always a directory
					newEntry.Path = filepath.Join(normalizedPath, f.Name()) + "/"
				}
				entry = newEntry
			}
		}

		if !discardAttributes {
			entry.ReadAttributes(top)
		}

		if f.Mode()&(os.ModeNamedPipe|os.ModeSocket|os.ModeDevice) != 0 {
			LOG_WARN("LIST_SKIP", "Skipped non-regular file %s", entry.Path)
			skippedFiles = append(skippedFiles, entry.Path)
			continue
		}

		entries = append(entries, entry)
	}

	// For top level directory we need to sort again because symlinks may have been changed
	if path == "" {
		sort.Sort(ByName(entries))
	}

	for _, entry := range entries {
		if entry.IsDir() {
			directoryList = append(directoryList, entry)
		} else {
			*fileList = append(*fileList, entry)
		}
	}

	for i, j := 0, len(directoryList)-1; i < j; i, j = i+1, j-1 {
		directoryList[i], directoryList[j] = directoryList[j], directoryList[i]
	}

	return directoryList, skippedFiles, nil
}

// Diff returns how many bytes remain unmodifiled between two files.
func (entry *Entry) Diff(chunkHashes []string, chunkLengths []int,
	otherHashes []string, otherLengths []int) (modifiedLength int64) {

	var offset1, offset2 int64
	i1 := entry.StartChunk
	i2 := 0
	for i1 <= entry.EndChunk && i2 < len(otherHashes) {

		start := 0
		if i1 == entry.StartChunk {
			start = entry.StartOffset
		}
		end := chunkLengths[i1]
		if i1 == entry.EndChunk {
			end = entry.EndOffset
		}

		if offset1 < offset2 {
			modifiedLength += int64(end - start)
			offset1 += int64(end - start)
			i1++
		} else if offset1 > offset2 {
			offset2 += int64(otherLengths[i2])
			i2++
		} else {
			if chunkHashes[i1] == otherHashes[i2] && end-start == otherLengths[i2] {
			} else {
				modifiedLength += int64(chunkLengths[i1])
			}
			offset1 += int64(end - start)
			offset2 += int64(otherLengths[i2])
			i1++
			i2++
		}
	}

	return modifiedLength
}
