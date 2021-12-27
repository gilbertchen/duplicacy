// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

func excludedByAttribute(attirbutes map[string][]byte) bool {
	_, ok := attirbutes["duplicacy_exclude"]
	return ok
}
