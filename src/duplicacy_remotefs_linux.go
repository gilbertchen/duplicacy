// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import "syscall"

// isRemoteFilesystem reports whether 'path' lives on a filesystem reached over the network, where a read is a round
// trip rather than a syscall.
func isRemoteFilesystem(path string) bool {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return false
	}

	return isRemoteFilesystemType(uint64(stat.Type))
}

// isRemoteFilesystemType makes the decision from the filesystem magic of statfs.  Only the network types are listed:
// everything else, including an unknown type and the local filesystems (ext2/ext3/ext4, btrfs, xfs, zfs, tmpfs, ...),
// is treated as local.  That is the direction that keeps the cache off, which is the one that cannot be wrong for a
// local filesystem -- or the write and its fsync come back for nothing.
func isRemoteFilesystemType(filesystemType uint64) bool {
	switch filesystemType {
	case 0x6969: // NFS
		return true
	case 0xFF534D42, 0x517B, 0xFE534D42: // CIFS, SMB, SMB2
		return true
	case 0x65735546: // FUSE: sshfs, rclone, davfs, gvfs, and drvfs under WSL
		return true
	case 0x958458F6: // virtiofs, which shares a host directory with a VM
		return true
	case 0x01021997: // 9p, the older way to do the same
		return true
	case 0x5346414F: // AFS
		return true
	case 0x00C36400: // Ceph
		return true
	case 0x73757245: // Coda
		return true
	case 0x564c: // NCP (NetWare)
		return true
	case 0x7461636F: // OCFS2, which is cluster storage rather than a local disk
		return true
	}

	return false
}
