// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

//go:build !linux && !windows && !darwin && !freebsd && !dragonfly && !openbsd
// +build !linux,!windows,!darwin,!freebsd,!dragonfly,!openbsd

package duplicacy

// isRemoteFilesystem reports whether 'path' lives on a filesystem reached over the network, where a read is a round
// trip rather than a syscall.  The platforms that can tell have their own file; this is the fallback for the rest,
// which assumes local.  That keeps the cache off, which is the one answer that cannot be wrong for an ordinary disk,
// where the cache only replaces a syscall with the same syscall.
func isRemoteFilesystem(path string) bool {
	return false
}
