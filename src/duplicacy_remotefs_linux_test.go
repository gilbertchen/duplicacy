// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import "testing"

// The decision is made from the filesystem magic, so the magic-to-answer mapping is worth pinning down directly: a
// local filesystem wrongly called remote brings back the cache write and its fsync for nothing, and the numbers are
// the only place the two are told apart.
func TestRemoteFilesystemType(t *testing.T) {

	local := []uint64{
		0xEF53,     // ext2/ext3/ext4
		0x9123683E, // btrfs
		0x58465342, // xfs
		0x2FC12FC1, // zfs
		0x01021994, // tmpfs
		0x4D44,     // msdos/fat
		0x2011BAB0, // exfat
		0x0,        // an empty answer, as in the drvfs case where Type is not filled in
	}
	for _, filesystemType := range local {
		if isRemoteFilesystemType(filesystemType) {
			t.Errorf("The filesystem type %#x must be treated as local", filesystemType)
		}
	}

	remote := []uint64{
		0x6969,     // NFS
		0xFF534D42, // CIFS
		0xFE534D42, // SMB2
		0x65735546, // FUSE
		0x958458F6, // virtiofs
		0x01021997, // 9p
		0x5346414F, // AFS
		0x00C36400, // Ceph
	}
	for _, filesystemType := range remote {
		if !isRemoteFilesystemType(filesystemType) {
			t.Errorf("The filesystem type %#x must be treated as remote", filesystemType)
		}
	}
}
