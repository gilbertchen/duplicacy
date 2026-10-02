// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

// attributeExcludeName is the extended attribute that marks a file as excluded.  FreeBSD stores extended attributes
// in the user namespace without a prefix, so the name is used as is.
const attributeExcludeName = "duplicacy_exclude"

// attributeExcludeValue is not checked on FreeBSD, where the mere presence of the attribute excludes the file.
const attributeExcludeValue = "1"

func excludedByAttribute(attirbutes map[string][]byte) bool {
	_, ok := attirbutes[attributeExcludeName]
	return ok
}
