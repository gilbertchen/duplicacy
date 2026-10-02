// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

// attributeExcludeName is the extended attribute that marks a file as excluded.  On Linux only the user namespace is
// available, and github.com/pkg/xattr reports the names it lists with the "user." prefix included, so the prefix is
// part of the name here.
const attributeExcludeName = "user.duplicacy_exclude"

// attributeExcludeValue is not checked on Linux, where the mere presence of the attribute excludes the file.
const attributeExcludeValue = "1"

func excludedByAttribute(attirbutes map[string][]byte) bool {
	_, ok := attirbutes[attributeExcludeName]
	return ok
}
