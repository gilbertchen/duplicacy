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
	"bytes"
	"crypto/sha256"

    "github.com/vmihailenco/msgpack"

)

// This is the hidden directory in the repository for storing various files.
var DUPLICACY_DIRECTORY = ".duplicacy"
var DUPLICACY_FILE = ".duplicacy"

// Mask for file permission bits
var fileModeMask = os.ModePerm | os.ModeSetuid | os.ModeSetgid | os.ModeSticky

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

	Attributes *map[string][]byte
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

func (entry *Entry) Copy() *Entry {
	return &Entry{
		Path: entry.Path,
		Size: entry.Size,
		Time: entry.Time,
		Mode: entry.Mode,
		Link: entry.Link,
		Hash: entry.Hash,

		UID: entry.UID,
		GID: entry.GID,

		StartChunk: entry.StartChunk,
		StartOffset: entry.StartOffset,
		EndChunk: entry.EndChunk,
		EndOffset: entry.EndOffset,

		Attributes: entry.Attributes,
	}
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
			entry.Attributes = &map[string][]byte{}
			for name, object := range attributes {
				if object == nil {
					(*entry.Attributes)[name] = []byte("")
				} else if attributeInBase64, ok := object.(string); !ok {
					return fmt.Errorf("Attribute '%s' is invalid for file '%s' in the snapshot", name, entry.Path)
				} else if attribute, err := base64.StdEncoding.DecodeString(attributeInBase64); err != nil {
					return fmt.Errorf("Failed to decode attribute '%s' for file '%s' in the snapshot: %v",
						name, entry.Path, err)
				} else {
					(*entry.Attributes)[name] = attribute
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

	if entry.Attributes != nil && len(*entry.Attributes) > 0 {
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

var _ msgpack.CustomEncoder = (*Entry)(nil)
var _ msgpack.CustomDecoder = (*Entry)(nil)

func (entry *Entry) EncodeMsgpack(encoder *msgpack.Encoder) error {

	err := encoder.EncodeString(entry.Path)
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(entry.Size)
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(entry.Time)
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(int64(entry.Mode))
	if err != nil {
		return err
	}

	err = encoder.EncodeString(entry.Link)
	if err != nil {
		return err
	}

	err = encoder.EncodeString(entry.Hash)
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(int64(entry.StartChunk))
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(int64(entry.StartOffset))
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(int64(entry.EndChunk))
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(int64(entry.EndOffset))
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(int64(entry.UID))
	if err != nil {
		return err
	}

	err = encoder.EncodeInt(int64(entry.GID))
	if err != nil {
		return err
	}

	var numberOfAttributes int64
	if entry.Attributes != nil {
		numberOfAttributes = int64(len(*entry.Attributes))
	}

	err = encoder.EncodeInt(numberOfAttributes)
	if err != nil {
		return err
	}

	if entry.Attributes != nil {
		attributes := make([]string, numberOfAttributes)
        i := 0
        for attribute := range *entry.Attributes {
            attributes[i] = attribute
            i++
        }
        sort.Strings(attributes)
		for _, attribute := range attributes {
			err = encoder.EncodeString(attribute)
			if err != nil {
				return err
			}
			err = encoder.EncodeString(string((*entry.Attributes)[attribute]))
			if err != nil {
				return err
			}
		}
	}

    return nil
}

func (entry *Entry) DecodeMsgpack(decoder *msgpack.Decoder) error {

	var err error

	entry.Path, err = decoder.DecodeString()
	if err != nil {
		return err
	}

	entry.Size, err = decoder.DecodeInt64()
	if err != nil {
		return err
	}

	entry.Time, err = decoder.DecodeInt64()
	if err != nil {
		return err
	}

	mode, err := decoder.DecodeInt64()
	if err != nil {
		return err
	}
	entry.Mode = uint32(mode)

	entry.Link, err = decoder.DecodeString()
	if err != nil {
		return err
	}

	entry.Hash, err = decoder.DecodeString()
	if err != nil {
		return err
	}

	startChunk, err := decoder.DecodeInt()
	if err != nil {
		return err
	}
	entry.StartChunk = int(startChunk)

	startOffset, err := decoder.DecodeInt()
	if err != nil {
		return err
	}
	entry.StartOffset = int(startOffset)

	endChunk, err := decoder.DecodeInt()
	if err != nil {
		return err
	}
	entry.EndChunk = int(endChunk)

	endOffset, err := decoder.DecodeInt()
	if err != nil {
		return err
	}
	entry.EndOffset = int(endOffset)

	uid, err := decoder.DecodeInt()
	if err != nil {
		return err
	}
	entry.UID = int(uid)

	gid, err := decoder.DecodeInt()
	if err != nil {
		return err
	}
	entry.GID = int(gid)

	numberOfAttributes, err := decoder.DecodeInt()
	if err != nil {
		return err
	}

	if numberOfAttributes > 0 {
		entry.Attributes = &map[string][]byte{}
		for i := 0; i < numberOfAttributes; i++ {
			attribute, err := decoder.DecodeString()
			if err != nil {
				return err
			}
			value, err := decoder.DecodeString()
			if err != nil {
				return err
			}
			(*entry.Attributes)[attribute] = []byte(value)
		}
	}
	return nil
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

func (entry *Entry) IsComplete() bool {
	return entry.Size >= 0
}

func (entry *Entry) GetPermissions() os.FileMode {
	return os.FileMode(entry.Mode) & fileModeMask
}

func (entry *Entry) GetParent() string {
	path := entry.Path
	if path != "" && path[len(path) - 1] == '/' {
		path = path[:len(path) - 1]
	}
	i := strings.LastIndex(path, "/")
	if i == -1 {
		return ""
	} else {
		return path[:i]
	}
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
		stat, err := os.Lstat(fullPath)
		fileInfo = &stat
		if err != nil {
			LOG_ERROR("RESTORE_STAT", "Failed to retrieve the file info: %v", err)
			return false
		}
	}

	// Note that chown can remove setuid/setgid bits so should be called before chmod
	if setOwner {
		if !SetOwner(fullPath, entry, fileInfo) {
			return false
		}
	}

	// Only set the permission if the file is not a symlink
	if !entry.IsLink() && (*fileInfo).Mode()&fileModeMask != entry.GetPermissions() {
		err := os.Chmod(fullPath, entry.GetPermissions())
		if err != nil {
			LOG_ERROR("RESTORE_CHMOD", "Failed to set the file permissions: %v", err)
			return false
		}
	}

	// Only set the time if the file is not a symlink
	if !entry.IsLink() && (*fileInfo).ModTime().Unix() != entry.Time {
		modifiedTime := time.Unix(entry.Time, 0)
		err := os.Chtimes(fullPath, modifiedTime, modifiedTime)
		if err != nil {
			LOG_ERROR("RESTORE_CHTIME", "Failed to set the modification time: %v", err)
			return false
		}
	}

	if entry.Attributes != nil && len(*entry.Attributes) > 0 {
		entry.SetAttributesToFile(fullPath)
	}

	return true
}

// Return -1 if 'left' should appear before 'right', 1 if opposite, and 0 if they are the same.
// Files are always arranged before subdirectories under the same parent directory.

func ComparePaths(left string, right string) int {
	p := 0
	for ; p < len(left) && p < len(right); p++ {
		if left[p] != right[p] {
			break
		}
	}

	// c1, c2 are the first bytes that differ
	var c1, c2 byte
	if p < len(left) {
		c1 = left[p]
	}
	if p < len(right) {
		c2 = right[p]
	}

	// c3, c4 indicate how the current component ends
	// c3 == '/':  the current component is a directory; c3 != '/':  the current component is the last one
	c3 := c1

	// last1, last2 means if the current compoent is the last component
	last1 := true
	for i := p; i < len(left); i++ {
		c3 = left[i]
		if c3 == '/' {
			last1 = i == len(left) - 1
			break
		}
	}

	c4 := c2
	last2 := true
	for i := p; i < len(right); i++ {
		c4 = right[i]
		if c4 == '/' {
			last2 = i == len(right) - 1
			break
		}
	}

	if last1 != last2 {
		if last1 {
			return -1
		} else {
			return 1
		}
	}

	if c3 == '/' {
		if c4 == '/' {
			// We are comparing two directory components
			if c1 == '/' {
				// left is shorter; note that c2 maybe smaller than c1 but c1 should be treated as 0 therefore
				// this is a special case that must be handled separately
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

func (left *Entry) Compare(right *Entry) int {
	return ComparePaths(left.Path, right.Path)
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
func ListEntries(top string, path string, patterns []string, nobackupFile string, excludeByAttribute bool, listingChannel chan *Entry) (directoryList []*Entry,
	skippedFiles []string, err error) {

	LOG_DEBUG("LIST_ENTRIES", "Listing %s", path)

	fullPath := joinPath(top, path)

	files := make([]os.FileInfo, 0, 1024)

	files, err = ioutil.ReadDir(fullPath)
	if err != nil {
		return directoryList, nil, err
	}

	// This binary search works because ioutil.ReadDir returns files sorted by Name() by default
	if nobackupFile != "" {
		ii := sort.Search(len(files), func(ii int) bool { return strings.Compare(files[ii].Name(), nobackupFile) >= 0 })
		if ii < len(files) && files[ii].Name() == nobackupFile {
			LOG_DEBUG("LIST_NOBACKUP", "%s is excluded due to nobackup file", path)
			return directoryList, skippedFiles, nil
		}
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

	for _, f := range files {
		if f.Name() == DUPLICACY_DIRECTORY {
			continue
		}
		entry := CreateEntryFromFileInfo(f, normalizedPath)
		if len(patterns) > 0 && !MatchPath(entry.Path, patterns) {
			continue
		}
		if entry.IsLink() {
			isRegular := false
			isRegular, entry.Link, err = Readlink(joinPath(top, entry.Path))
			if err != nil {
				LOG_WARN("LIST_LINK", "Failed to read the symlink %s: %v", entry.Path, err)
				skippedFiles = append(skippedFiles, entry.Path)
				continue
			}

			if isRegular {
				entry.Mode ^= uint32(os.ModeSymlink)
			} else if path == "" && (filepath.IsAbs(entry.Link) || filepath.HasPrefix(entry.Link, `\\`)) && !strings.HasPrefix(entry.Link, normalizedTop) {
				stat, err := os.Stat(joinPath(top, entry.Path))
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
				if len(patterns) > 0 && !MatchPath(newEntry.Path, patterns) {
					continue
				}
				entry = newEntry
			}
		}

		entry.ReadAttributes(top)

		if excludeByAttribute && entry.Attributes != nil && excludedByAttribute(*entry.Attributes) {
			LOG_DEBUG("LIST_EXCLUDE", "%s is excluded by attribute", entry.Path)
			continue
		}

		if f.Mode()&(os.ModeNamedPipe|os.ModeSocket|os.ModeDevice) != 0 {
			LOG_WARN("LIST_SKIP", "Skipped non-regular file %s", entry.Path)
			skippedFiles = append(skippedFiles, entry.Path)
			continue
		}

		if entry.IsDir() {
			directoryList = append(directoryList, entry)
		} else {
			listingChannel <- entry
		}
	}

	// For top level directory we need to sort again because symlinks may have been changed
	if path == "" {
		sort.Sort(ByName(directoryList))
	}

	for _, entry := range directoryList {
		listingChannel <- entry
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

func (entry *Entry) EncodeWithHash(encoder *msgpack.Encoder) error {
	entryBytes, err := msgpack.Marshal(entry)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(entryBytes)
	err = encoder.EncodeBytes(entryBytes)
	if err != nil {
		return err
	}
	err = encoder.EncodeBytes(hash[:])
	if err != nil {
		return err
	}
	return nil
}

func DecodeEntryWithHash(decoder *msgpack.Decoder) (*Entry, error) {
	entryBytes, err := decoder.DecodeBytes()
	if err != nil {
		return nil, err
	}
	hashBytes, err := decoder.DecodeBytes()
	if err != nil {
		return nil, err
	}
	expectedHash := sha256.Sum256(entryBytes)
	if bytes.Compare(expectedHash[:], hashBytes) != 0 {
		return nil, fmt.Errorf("corrupted file metadata")
	}

	var entry Entry
	err = msgpack.Unmarshal(entryBytes, &entry)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (entry *Entry) check(chunkLengths []int) error {

	if entry.Size < 0 {
		return fmt.Errorf("The file %s hash an invalid size (%d)", entry.Path, entry.Size)
	}

	if !entry.IsFile() || entry.Size == 0 {
		return nil
	}

	if entry.StartChunk < 0 {
		return fmt.Errorf("The file %s starts at chunk %d", entry.Path, entry.StartChunk)
	}

	if entry.EndChunk >= len(chunkLengths) {
		return fmt.Errorf("The file %s ends at chunk %d while the number of chunks is %d",
			entry.Path, entry.EndChunk, len(chunkLengths))
	}

	if entry.EndChunk < entry.StartChunk {
		return fmt.Errorf("The file %s starts at chunk %d and ends at chunk %d",
			entry.Path, entry.StartChunk, entry.EndChunk)
	}

	if entry.StartOffset >= chunkLengths[entry.StartChunk] {
		return fmt.Errorf("The file %s starts at offset %d of chunk %d of length %d",
			entry.Path, entry.StartOffset, entry.StartChunk, chunkLengths[entry.StartChunk])
	}

	if entry.EndOffset > chunkLengths[entry.EndChunk] {
		return fmt.Errorf("The file %s ends at offset %d of chunk %d of length %d",
			entry.Path, entry.EndOffset, entry.EndChunk, chunkLengths[entry.EndChunk])
	}

	fileSize := int64(0)

	for i := entry.StartChunk; i <= entry.EndChunk; i++ {

		start := 0
		if i == entry.StartChunk {
			start = entry.StartOffset
		}
		end := chunkLengths[i]
		if i == entry.EndChunk {
			end = entry.EndOffset
		}

		fileSize += int64(end - start)
	}

	if entry.Size != fileSize {
		return fmt.Errorf("The file %s has a size of %d but the total size of chunks is %d",
			entry.Path, entry.Size, fileSize)
	}

	return nil
}
