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

var Preferences [] Preference

// Compute .duplicacy directory path name:
//  - if .duplicacy is a directory -> compute absolute path name and return it
//  - if .duplicacy is a file -> assumed this file contains the real path name of .duplicacy
//  - if pointed directory does not exits... return error
func GetDotDuplicacyPathName( repository string) (duplicacyDirectory string){
    
    dotDuplicacy := path.Join(repository, DUPLICACY_DIRECTORY) //TOKEEP
    
    stat, err := os.Stat(dotDuplicacy)
    if err != nil && !os.IsNotExist(err) {
        LOG_ERROR("DOT_DUPLICACY_PATH", "Failed to retrieve the information about the directory %s: %v",
            repository, err)
        return ""
    }
    
    if stat != nil && stat.IsDir() {
        // $repository/.duplicacy exists and is a directory --> we found the .duplicacy directory
        return path.Clean(dotDuplicacy)
    }
    
    if stat != nil && stat.Mode().IsRegular() {
        b, err := ioutil.ReadFile(dotDuplicacy) // just pass the file name
        if err != nil {
            LOG_ERROR("DOT_DUPLICACY_PATH", "Failed to read file %s: %v",
                dotDuplicacy, err)
            return ""
        }
        dot_duplicacy := string(b) // convert content to a 'string'
        stat, err := os.Stat(dot_duplicacy)
        if err != nil && !os.IsNotExist(err) {
            LOG_ERROR("DOT_DUPLICACY_PATH", "Failed to retrieve the information about the directory %s: %v",
                repository, err)
            return ""
        }
        if stat != nil && stat.IsDir() {
            // If expression read from .duplicacy file is a directory --> we found the .duplicacy directory
            return path.Clean( dot_duplicacy)
        }
    }
    return ""
}

func LoadPreferences(repository string) (bool) {
    
    duplicacyDirectory := GetDotDuplicacyPathName(repository)
    description, err := ioutil.ReadFile(path.Join(duplicacyDirectory, "preferences"))
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

func SavePreferences(repository string) (bool) {
    description, err := json.MarshalIndent(Preferences, "", "    ")
    if err != nil {
        LOG_ERROR("PREFERENCE_MARSHAL", "Failed to marshal the repository preferences: %v", err)
        return false
    }
    duplicacyDirectory := GetDotDuplicacyPathName(repository)
    preferenceFile := path.Join(duplicacyDirectory, "/preferences")
    
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
