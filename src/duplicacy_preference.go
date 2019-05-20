// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
)

// Preference stores options for each storage.
type Preference struct {
	Name              string            `json:"name"`
	SnapshotID        string            `json:"id"`
	RepositoryPath    string            `json:"repository"`
	StorageURL        string            `json:"storage"`
	Encrypted         bool              `json:"encrypted"`
	BackupProhibited  bool              `json:"no_backup"`
	RestoreProhibited bool              `json:"no_restore"`
	DoNotSavePassword bool              `json:"no_save_password"`
	DoNotFollowLinks  bool              `json:"no_follow_links"`
	NobackupFile      string            `json:"nobackup_file"`
	Keys              map[string]string `json:"keys"`
}

var preferencePath string
var Preferences []Preference

func LoadPreferences(repository string) bool {

	preferencePath = path.Join(repository, DUPLICACY_DIRECTORY)

	stat, err := os.Stat(preferencePath)
	if err != nil {
		LOG_ERROR("PREFERENCE_PATH", "Failed to retrieve the information about the directory %s: %v", repository, err)
		return false
	}

	if !stat.IsDir() {
		content, err := ioutil.ReadFile(preferencePath)
		if err != nil {
			LOG_ERROR("DOT_DUPLICACY_PATH", "Failed to locate the preference path: %v", err)
			return false
		}
		realPreferencePath := strings.TrimSpace(string(content))
		stat, err := os.Stat(realPreferencePath)
		if err != nil {
			LOG_ERROR("PREFERENCE_PATH", "Failed to retrieve the information about the directory %s: %v", content, err)
			return false
		}
		if !stat.IsDir() {
			LOG_ERROR("PREFERENCE_PATH", "The preference path %s is not a directory", realPreferencePath)
		}

		preferencePath = realPreferencePath
	}

	description, err := ioutil.ReadFile(path.Join(preferencePath, "preferences"))
	if err != nil {
		LOG_ERROR("PREFERENCE_OPEN", "Failed to read the preference file from repository %s: %v", repository, err)
		return false
	}

	err = json.Unmarshal(description, &Preferences)
	if err != nil {
		LOG_ERROR("PREFERENCE_PARSE", "Failed to parse the preference file for repository %s: %v", repository, err)
		return false
	}

	if len(Preferences) == 0 {
		LOG_ERROR("PREFERENCE_NONE", "No preference found in the preference file")
		return false
	}

	for _, preference := range Preferences {
		if strings.ToLower(preference.Name) == "ssh" {
			LOG_ERROR("PREFERENCE_INVALID", "'%s' is an invalid storage name", preference.Name)
			return false
		}
	}

	return true
}

func GetDuplicacyPreferencePath() string {
	if preferencePath == "" {
		LOG_ERROR("PREFERENCE_PATH", "The preference path has not been set")
		return ""
	}
	return preferencePath
}

// Normally 'preferencePath' is set in LoadPreferences; however, if LoadPreferences is not called, this function
// provide another change to set 'preferencePath'
func SetDuplicacyPreferencePath(p string) {
	preferencePath = p
}

func SavePreferences() bool {
	description, err := json.MarshalIndent(Preferences, "", "    ")
	if err != nil {
		LOG_ERROR("PREFERENCE_MARSHAL", "Failed to marshal the repository preferences: %v", err)
		return false
	}
	preferenceFile := path.Join(GetDuplicacyPreferencePath(), "preferences")

	err = ioutil.WriteFile(preferenceFile, description, 0600)
	if err != nil {
		LOG_ERROR("PREFERENCE_WRITE", "Failed to save the preference file %s: %v", preferenceFile, err)
		return false
	}

	return true
}

func FindPreference(name string) *Preference {
	for i, preference := range Preferences {
		if preference.Name == name || preference.StorageURL == name {
			return &Preferences[i]
		}
	}

	return nil
}

func (preference *Preference) Equal(other *Preference) bool {
	return reflect.DeepEqual(preference, other)
}
