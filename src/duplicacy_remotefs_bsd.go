// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

//go:build darwin || freebsd || dragonfly
// +build darwin freebsd dragonfly

package duplicacy

import (
	"strings"
	"syscall"
)

// isRemoteFilesystem reports whether 'path' lives on a filesystem reached over the network, where a read is a round
// trip rather than a syscall.
func isRemoteFilesystem(path string) bool {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false
	}

	return isRemoteFilesystemType(filesystemTypeName(stat.Fstypename[:]))
}

// isRemoteFilesystemType makes the decision from the filesystem name these systems report.  Only the network names
// are listed, and everything else, including a name that is not recognized, is treated as local: that keeps the
// cache off, which is the one answer that cannot be wrong for an ordinary disk, where the write-back buys nothing.
func isRemoteFilesystemType(filesystemName string) bool {
	switch strings.ToLower(filesystemName) {
	case "nfs", "nfs4", "smbfs", "cifs", "autofs", "afpfs", "ftp", "webdav", "fuse", "fusefs", "sshfs", "9p":
		return true
	}

	return false
}

// filesystemTypeName reads the NUL-terminated name a statfs result holds as a byte slice.
func filesystemTypeName(name []int8) string {
	for i, c := range name {
		if c == 0 {
			return string(byteSlice(name[:i]))
		}
	}
	return string(byteSlice(name))
}

// byteSlice reinterprets the C char array a statfs result holds as bytes, so the name can be compared.
func byteSlice(characters []int8) []byte {
	name := make([]byte, len(characters))
	for i, c := range characters {
		name[i] = byte(c)
	}
	return name
}
