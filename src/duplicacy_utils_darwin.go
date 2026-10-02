// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"strings"
)

// attributeExcludeName is the extended attribute that Time Machine and other macOS backup tools use to mark a file as
// excluded from backups.
const attributeExcludeName = "com.apple.metadata:com_apple_backup_excludeItem"

// attributeExcludeValue is the value the attribute must contain for the file to be considered excluded.
const attributeExcludeValue = "com.apple.backupd"

func excludedByAttribute(attirbutes map[string][]byte) bool {
	value, ok := attirbutes[attributeExcludeName]
	return ok && strings.Contains(string(value), attributeExcludeValue)
}
