// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

//go:build freebsd || netbsd
// +build freebsd netbsd

package duplicacy

func excludedByAttribute(attributes map[string][]byte) bool {
	_, ok := attributes["duplicacy_exclude"]
	return ok
}
