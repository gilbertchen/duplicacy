// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

// +build !windows

package duplicacy

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"

	"github.com/gilbertchen/xattr"
)

func Readlink(path string) (isRegular bool, s string, err error) {
	s, err = os.Readlink(path)
	return false, s, err
}

func GetOwner(entry *Entry, fileInfo *os.FileInfo) {
	stat, ok := (*fileInfo).Sys().(*syscall.Stat_t)
	if ok && stat != nil {
		entry.UID = int(stat.Uid)
		entry.GID = int(stat.Gid)
	} else {
		entry.UID = -1
		entry.GID = -1
	}
}

func SetOwner(fullPath string, entry *Entry, fileInfo *os.FileInfo) bool {
	stat, ok := (*fileInfo).Sys().(*syscall.Stat_t)
	if ok && stat != nil && (int(stat.Uid) != entry.UID || int(stat.Gid) != entry.GID) {
		if entry.UID != -1 && entry.GID != -1 {
			err := os.Chown(fullPath, entry.UID, entry.GID)
			if err != nil {
				LOG_ERROR("RESTORE_CHOWN", "Failed to change uid or gid: %v", err)
				return false
			}
		}
	}

	return true
}

func (entry *Entry) ReadAttributes(top string) {

	fullPath := filepath.Join(top, entry.Path)
	attributes, _ := xattr.Listxattr(fullPath)
	if len(attributes) > 0 {
		entry.Attributes = make(map[string][]byte)
		for _, name := range attributes {
			attribute, err := xattr.Getxattr(fullPath, name)
			if err == nil {
				entry.Attributes[name] = attribute
			}
		}
	}
}

func (entry *Entry) SetAttributesToFile(fullPath string) {
	names, _ := xattr.Listxattr(fullPath)

	for _, name := range names {

		newAttribute, found := entry.Attributes[name]
		if found {
			oldAttribute, _ := xattr.Getxattr(fullPath, name)
			if !bytes.Equal(oldAttribute, newAttribute) {
				xattr.Setxattr(fullPath, name, newAttribute)
			}
			delete(entry.Attributes, name)
		} else {
			xattr.Removexattr(fullPath, name)
		}
	}

	for name, attribute := range entry.Attributes {
		xattr.Setxattr(fullPath, name, attribute)
	}

}
