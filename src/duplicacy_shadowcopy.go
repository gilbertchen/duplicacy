// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

// +build !windows

package duplicacy

func CreateShadowCopy(top string, shadowCopy bool) (shadowTop string) {
    return top
}

func DeleteShadowCopy() {}
