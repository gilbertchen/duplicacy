// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com
// +build !windows

package duplicacy

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/gilbertchen/xattr"
)

// TestEntryExcludeByAttribute tests the excludeByAttribute parameter to the ListEntries function
func TestEntryExcludeByAttribute(t *testing.T) {

	if !(runtime.GOOS == "darwin" || runtime.GOOS == "linux") {
		t.Skip("skipping test not darwin or linux")
	}

	testDir := filepath.Join(os.TempDir(), "duplicacy_test")

	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0700)

	// Files or folders named with "exclude" below will have the exclusion attribute set on them
	// When ListEntries is called with excludeByAttribute true, they should be excluded.
	DATA := [...]string{
		"excludefile",
		"includefile",
		"excludedir/",
		"excludedir/file",
		"includedir/",
		"includedir/includefile",
		"includedir/excludefile",
	}

	for _, file := range DATA {
		fullPath := filepath.Join(testDir, file)
		if file[len(file)-1] == '/' {
			err := os.Mkdir(fullPath, 0700)
			if err != nil {
				t.Errorf("Mkdir(%s) returned an error: %s", fullPath, err)
			}
			continue
		}

		err := ioutil.WriteFile(fullPath, []byte(file), 0700)
		if err != nil {
			t.Errorf("WriteFile(%s) returned an error: %s", fullPath, err)
		}
	}

	for _, file := range DATA {
		fullPath := filepath.Join(testDir, file)
		if strings.Contains(file, "exclude") {
			xattr.Setxattr(fullPath, "com.apple.metadata:com_apple_backup_excludeItem", []byte("com.apple.backupd"))
		}
	}

	for _, excludeByAttribute := range [2]bool{true, false} {
		t.Logf("testing excludeByAttribute: %t", excludeByAttribute)
		directories := make([]*Entry, 0, 4)
		directories = append(directories, CreateEntry("", 0, 0, 0))

		entries := make([]*Entry, 0, 4)
		entryChannel := make(chan *Entry, 1024)
		entries = append(entries, CreateEntry("", 0, 0, 0))

		for len(directories) > 0 {
			directory := directories[len(directories)-1]
			directories = directories[:len(directories)-1]
			subdirectories, _, err := ListEntries(testDir, directory.Path, nil, "", excludeByAttribute, entryChannel)
			if err != nil {
				t.Errorf("ListEntries(%s, %s) returned an error: %s", testDir, directory.Path, err)
			}
			directories = append(directories, subdirectories...)
		}

		close(entryChannel)

		for entry := range entryChannel {
			entries = append(entries, entry)
		}

		entries = entries[1:]

		for _, entry := range entries {
			t.Logf("entry: %s", entry.Path)
		}

		i := 0
		for _, file := range DATA {
			entryFound := false
			var entry *Entry
			for _, entry = range entries {
				if entry.Path == file {
					entryFound = true
					break
				}
			}

			if excludeByAttribute && strings.Contains(file, "exclude") {
				if entryFound {
					t.Errorf("file: %s, expected to be excluded but wasn't. attributes: %v", file, entry.Attributes)
					i++
				} else {
					t.Logf("file: %s, excluded", file)
				}
			} else {
				if entryFound {
					t.Logf("file: %s, included. attributes: %v", file, entry.Attributes)
					i++
				} else {
					t.Errorf("file: %s, expected to be included but wasn't", file)
				}
			}
		}

	}

	if !t.Failed() {
		os.RemoveAll(testDir)
	}

}
