// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "testing"
    "io/ioutil"
    "os"
    "path/filepath"
    "math/rand"
    "sort"
)

func TestEntrySort(t *testing.T) {

    DATA := [...]string {
        "ab",
        "ab-",
        "ab0",
        "ab1",
        "\xBB\xDDfile",
        "\xFF\xDDfile",
        "ab/",
        "ab/c",
        "ab+/c-",
        "ab+/c0",
        "ab+/c/",
        "ab+/c/d",
        "ab+/c+/",
        "ab+/c+/d",
        "ab+/c0/",
        "ab+/c0/d",
        "ab-/",
        "ab-/c",
        "ab0/",
        "ab1/",
        "ab1/c",
        "ab1/\xBB\xDDfile",
        "ab1/\xFF\xDDfile",
    }

    var entry1, entry2 *Entry

    for i, p1 := range DATA {
        if p1[len(p1) - 1] == '/' {
            entry1 = CreateEntry(p1, 0, 0, 0700 | uint32(os.ModeDir))
        } else {
            entry1 = CreateEntry(p1, 0, 0, 0700)
        }
        for j, p2 := range DATA {

            if p2[len(p2) - 1] == '/' {
                entry2 = CreateEntry(p2, 0, 0, 0700 | uint32(os.ModeDir))
            } else {
                entry2 = CreateEntry(p2, 0, 0, 0700)
            }

            compared := entry1.Compare(entry2)

            if compared < 0 {
                compared = -1
            } else if compared > 0 {
                compared = 1
            }

            var expected int
            if i < j {
                expected = -1
            } else if i > j {
                expected = 1
            } else {
                expected = 0
            }

            if compared != expected {
                t.Errorf("%s vs %s: %d, expected: %d", p1, p2, compared, expected)
            }

        }
    }
}

func TestEntryList(t *testing.T) {

    testDir := filepath.Join(os.TempDir(), "duplicacy_test")
    os.RemoveAll(testDir)
    os.MkdirAll(testDir, 0700)

    DATA := [...]string {
        "ab",
        "ab-",
        "ab0",
        "ab1",
        "ab+/",
        "ab+/c",
        "ab+/c+",
        "ab+/c1",
        "ab+/c-/",
        "ab+/c-/d",
        "ab+/c0/",
        "ab+/c0/d",
        "ab2/",
        "ab2/c",
        "ab3/",
        "ab3/c",
    }


    var entry1, entry2 *Entry

    for i, p1 := range DATA {
        if p1[len(p1) - 1] == '/' {
            entry1 = CreateEntry(p1, 0, 0, 0700 | uint32(os.ModeDir))
        } else {
            entry1 = CreateEntry(p1, 0, 0, 0700)
        }
        for j, p2 := range DATA {

            if p2[len(p2) - 1] == '/' {
                entry2 = CreateEntry(p2, 0, 0, 0700 | uint32(os.ModeDir))
            } else {
                entry2 = CreateEntry(p2, 0, 0, 0700)
            }

            compared := entry1.Compare(entry2)

            if compared < 0 {
                compared = -1
            } else if compared > 0 {
                compared = 1
            }

            var expected int
            if i < j {
                expected = -1
            } else if i > j {
                expected = 1
            } else {
                expected = 0
            }

            if compared != expected {
                t.Errorf("%s vs %s: %d, expected: %d", p1, p2, compared, expected)
            }

        }
    }

    for _, file := range DATA {

        fullPath := filepath.Join(testDir, file)
        if file[len(file) - 1] == '/' {
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

    directories := make([]*Entry, 0, 4)
    directories = append(directories, CreateEntry("", 0, 0, 0))

    entries := make([]*Entry, 0, 4)

    for len(directories) > 0 {
        directory := directories[len(directories) - 1]
        directories = directories[:len(directories) - 1]
        entries = append(entries, directory)
        subdirectories, _, err := ListEntries(testDir, directory.Path, &entries, nil, false)
        if err != nil {
            t.Errorf("ListEntries(%s, %s) returned an error: %s", testDir, directory.Path, err)
        }
        directories = append(directories, subdirectories...)
    }

    entries = entries[1:]

    for _, entry := range entries {
        t.Logf("entry: %s", entry.Path)
    }

    if len(entries) != len(DATA) {
        t.Errorf("Got %d entries instead of %d", len(entries), len(DATA))
        return
    }

    for i := 0; i < len(entries); i++ {
        if entries[i].Path != DATA[i] {
            t.Errorf("entry: %s, expected: %s", entries[i].Path, DATA[i])
        }
    }

    t.Logf("shuffling %d entries", len(entries))
    for i := range entries {
        j := rand.Intn(i + 1)
        entries[i], entries[j] = entries[j], entries[i]
    }

    sort.Sort(ByName(entries))

    for i := 0; i < len(entries); i++ {
        if entries[i].Path != DATA[i] {
            t.Errorf("entry: %s, expected: %s", entries[i].Path, DATA[i])
        }
    }

    if !t.Failed() {
        os.RemoveAll(testDir)
    }

}

