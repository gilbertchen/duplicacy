// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

// +build freebsd netbsd linux solaris

package duplicacy

import (
)

func excludedByAttribute(attributes map[string][]byte) bool {
	_, ok := attributes["user.duplicacy_exclude"]
	return ok
}
