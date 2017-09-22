// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

// +build !windows

package duplicacy

import (
	"github.com/gilbertchen/keyring"
)

func SetKeyringFile(path string) {
	// We only use keyring file on Windows
}

func keyringGet(key string) (value string) {
	value, err := keyring.Get("duplicacy", key)
	if err != nil {
		LOG_DEBUG("KEYRING_GET", "Failed to get the value from the keyring: %v", err)
	}
	return value
}

func keyringSet(key string, value string) {
	err := keyring.Set("duplicacy", key, value)
	if err != nil {
		LOG_DEBUG("KEYRING_GET", "Failed to store the value to the keyring: %v", err)
	}
}
