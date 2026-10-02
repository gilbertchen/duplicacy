// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"syscall"
	"unsafe"
)

var procGetDriveType = syscall.NewLazyDLL("kernel32.dll").NewProc("GetDriveTypeW")

// DRIVE_REMOTE, the value GetDriveTypeW returns for a network share, whether it is mapped to a drive letter or
// reached by UNC path.
const driveRemote = 4

// isRemoteFilesystem reports whether 'path' lives on a filesystem reached over the network, where a read is a round
// trip rather than a syscall.  Windows is asked about the volume the path is on, so the answer does not depend on the
// path being spelled as a UNC path; a path with no answer, and one that is not on a remote volume, are both local.
func isRemoteFilesystem(path string) bool {
	// GetDriveTypeW derives the volume root itself, so a path below the root does not have to be shortened first.
	pathPointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return false
	}

	driveType, _, _ := procGetDriveType.Call(uintptr(unsafe.Pointer(pathPointer)))
	return driveType == driveRemote
}
