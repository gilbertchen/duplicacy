// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"os"
)

// FileReader wraps a number of files and turns them into a series of readers.
type FileReader struct {
	top   string
	files []*Entry

	CurrentFile  *os.File
	CurrentIndex int
	CurrentEntry *Entry

	pass	      int

	SkippedFiles []string
}

// CreateFileReader creates a file reader.
func CreateFileReader(top string, files []*Entry) *FileReader {

	reader := &FileReader{
		top:          top,
		files:        files,
		CurrentIndex: -1,
		pass:	      1,
	}

	reader.NextFile()

	return reader
}

// NextFile switches to the next file in the file reader.
func (reader *FileReader) NextFile() bool {

	if reader.CurrentFile != nil {
		reader.CurrentFile.Close()
	}

	reader.CurrentIndex++
	for reader.pass <= 2 {
		// perform two passes of the files
		if reader.CurrentIndex >= len(reader.files) {
			reader.pass++
			reader.CurrentIndex = 0
			continue
		}
		reader.CurrentEntry = reader.files[reader.CurrentIndex]
		if !reader.CurrentEntry.IsFile() || reader.CurrentEntry.Size == 0 ||
		   reader.CurrentEntry.Pass != reader.pass {
			reader.CurrentIndex++
			continue
		}

		var err error

		fullPath := joinPath(reader.top, reader.CurrentEntry.Path)
		reader.CurrentFile, err = os.OpenFile(fullPath, os.O_RDONLY, 0)
		if err != nil {
			LOG_WARN("OPEN_FAILURE", "Failed to open file for reading: %v", err)
			reader.CurrentEntry.Size = 0
			reader.SkippedFiles = append(reader.SkippedFiles, reader.CurrentEntry.Path)
			reader.CurrentIndex++
			continue
		}

		return true
	}

	reader.CurrentFile = nil
	return false
}
