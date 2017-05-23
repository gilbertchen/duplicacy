// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

// +build !windows

package duplicacy

func CreateShadowCopy(top string, shadowCopy bool) (shadowTop string) {
    return top
}

func DeleteShadowCopy() {}
