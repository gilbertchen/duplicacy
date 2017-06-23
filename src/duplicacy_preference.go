// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "encoding/json"
    "path"
    "io/ioutil"
    "reflect"
    "os"
)

// Preference stores options for each storage.
type Preference struct {
    Name string               `json:"name"`
    SnapshotID string         `json:"id"`
    StorageURL string         `json:"storage"`
    Encrypted bool            `json:"encrypted"`
    BackupProhibited bool     `json:"no_backup"`
    RestoreProhibited bool    `json:"no_restore"`
    DoNotSavePassword bool    `json:"no_save_password"`
    Keys map[string]string    `json:"keys"`
}

var preferencePath string
var Preferences [] Preference

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
        realPreferencePath := string(content)
        stat, err := os.Stat(realPreferencePath)
        if err != nil {
            LOG_ERROR("PREFERENCE_PATH", "Failed to retrieve the information about the directory %s: %v", content, err)
            return false
        }
        if !stat.IsDir() {
            LOG_ERROR("PREFERENCE_PATH", "The preference path %s is not a directory",  realPreferencePath)
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

func SavePreferences() (bool) {
    description, err := json.MarshalIndent(Preferences, "", "    ")
    if err != nil {
        LOG_ERROR("PREFERENCE_MARSHAL", "Failed to marshal the repository preferences: %v", err)
        return false
    }
    preferenceFile := path.Join(GetDuplicacyPreferencePath(), "preferences")
    
    err = ioutil.WriteFile(preferenceFile, description, 0644)
    if err != nil {
        LOG_ERROR("PREFERENCE_WRITE", "Failed to save the preference file %s: %v", preferenceFile, err)
        return false
    }

    return true
}

func FindPreference(name string) (*Preference) {
    for _, preference := range Preferences {
        if preference.Name == name || preference.StorageURL == name {
            return &preference
        }
    }

    return nil
}

func (preference *Preference) Equal(other *Preference) bool {
    return reflect.DeepEqual(preference, other)
}
