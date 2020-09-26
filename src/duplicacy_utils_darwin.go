// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"strings"
)

func excludedByAttribute(attirbutes map[string][]byte) bool {
	value, ok := attirbutes["com.apple.metadata:com_apple_backup_excludeItem"]
	return ok && strings.Contains(string(value), "com.apple.backupd")
}
