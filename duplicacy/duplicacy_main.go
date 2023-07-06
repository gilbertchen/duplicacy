// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	_ "net/http/pprof"

	"github.com/gilbertchen/cli"

	"io/ioutil"

	duplicacy "github.com/gilbertchen/duplicacy/src"
)

const (
	ArgumentExitCode = 3
)

var ScriptEnabled bool
var GitCommit = "unofficial"

func getRepositoryPreference(context *cli.Context, storageName string) (repository string,
	preference *duplicacy.Preference) {

	repository, err := os.Getwd()
	if err != nil {
		duplicacy.LOG_ERROR("REPOSITORY_PATH", "Failed to retrieve the current working directory: %v", err)
		return "", nil
	}

	for {
		stat, err := os.Stat(path.Join(repository, duplicacy.DUPLICACY_DIRECTORY)) //TOKEEP
		if err != nil && !os.IsNotExist(err) {
			duplicacy.LOG_ERROR("REPOSITORY_PATH", "Failed to retrieve the information about the directory %s: %v",
				repository, err)
			return "", nil
		}

		if stat != nil && (stat.IsDir() || stat.Mode().IsRegular()) {
			break
		}

		parent := path.Dir(repository)
		if parent == repository || parent == "" {
			duplicacy.LOG_ERROR("REPOSITORY_PATH", "Repository has not been initialized")
			return "", nil
		}
		repository = parent
	}
	duplicacy.LoadPreferences(repository)

	preferencePath := duplicacy.GetDuplicacyPreferencePath()
	duplicacy.SetKeyringFile(path.Join(preferencePath, "keyring"))

	if storageName == "" {
		storageName = context.String("storage")
	}

	if storageName == "" {
		if duplicacy.Preferences[0].RepositoryPath != "" {
			repository = duplicacy.Preferences[0].RepositoryPath
			duplicacy.LOG_INFO("REPOSITORY_SET", "Repository set to %s", repository)
		}
		return repository, &duplicacy.Preferences[0]
	}

	preference = duplicacy.FindPreference(storageName)

	if preference == nil {
		duplicacy.LOG_ERROR("STORAGE_NONE", "No storage named '%s' is found", storageName)
		return "", nil
	}

	if preference.RepositoryPath != "" {
		repository = preference.RepositoryPath
		duplicacy.LOG_INFO("REPOSITORY_SET", "Repository set to %s", repository)
	}

	return repository, preference
}

func getRevisions(context *cli.Context) (revisions []int) {

	flags := context.StringSlice("r")

	rangeRegex := regexp.MustCompile(`^([0-9]+)-([0-9]+)$`)
	numberRegex := regexp.MustCompile(`^([0-9]+)$`)

	for _, flag := range flags {
		matched := rangeRegex.FindStringSubmatch(flag)
		if matched != nil {
			start, _ := strconv.Atoi(matched[1])
			end, _ := strconv.Atoi(matched[2])
			if end > start {
				for r := start; r <= end; r++ {
					revisions = append(revisions, r)
				}
				continue
			}
		}

		matched = numberRegex.FindStringSubmatch(flag)
		if matched != nil {
			r, _ := strconv.Atoi(matched[1])
			revisions = append(revisions, r)
			continue
		}

		fmt.Fprintf(context.App.Writer, "Invalid revision: %s.\n\n", flag)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	return revisions

}

func setGlobalOptions(context *cli.Context) {
	if context.GlobalBool("log") {
		duplicacy.EnableLogHeader()
	}

	if context.GlobalBool("stack") {
		duplicacy.EnableStackTrace()
	}

	if context.GlobalBool("verbose") {
		duplicacy.SetLoggingLevel(duplicacy.TRACE)
	}

	if context.GlobalBool("debug") {
		duplicacy.SetLoggingLevel(duplicacy.DEBUG)
	}

	if context.GlobalBool("print-memory-usage") {
		go duplicacy.PrintMemoryUsage()
	}

	ScriptEnabled = true
	if context.GlobalBool("no-script") {
		ScriptEnabled = false
	}

	address := context.GlobalString("profile")
	if address != "" {
		go func() {
			http.ListenAndServe(address, nil)
		}()
	}

	for _, logID := range context.GlobalStringSlice("suppress") {
		duplicacy.SuppressLog(logID)
	}

	duplicacy.RunInBackground = context.GlobalBool("background")
}

func runScript(context *cli.Context, storageName string, phase string) bool {

	if !ScriptEnabled {
		return false
	}

	preferencePath := duplicacy.GetDuplicacyPreferencePath()
	scriptDir, _ := filepath.Abs(path.Join(preferencePath, "scripts"))
	scriptNames := []string{phase + "-" + context.Command.Name,
		storageName + "-" + phase + "-" + context.Command.Name}

	script := ""
	for _, scriptName := range scriptNames {
		script = path.Join(scriptDir, scriptName)
		if runtime.GOOS == "windows" {
			script += ".bat"
		}
		if _, err := os.Stat(script); err == nil {
			break
		} else {
			script = ""
		}
	}

	if script == "" {
		return false
	}

	duplicacy.LOG_INFO("SCRIPT_RUN", "Running script %s", script)

	output, err := exec.Command(script, os.Args...).CombinedOutput()
	for _, line := range strings.Split(string(output), "\n") {
		line := strings.TrimSpace(line)
		if line != "" {
			duplicacy.LOG_INFO("SCRIPT_OUTPUT", line)
		}
	}

	if err != nil {
		duplicacy.LOG_ERROR("SCRIPT_ERROR", "Failed to run %s script: %v", script, err)
		return false
	}

	return true
}

func loadRSAPrivateKey(keyFile string, passphrase string, preference *duplicacy.Preference, backupManager *duplicacy.BackupManager, resetPasswords bool) {
	if keyFile == "" {
		return
	}

	prompt := fmt.Sprintf("Enter the passphrase for %s:", keyFile)
	if passphrase == "" {
		passphrase = duplicacy.GetPassword(*preference, "rsa_passphrase", prompt, false, resetPasswords)
		backupManager.LoadRSAPrivateKey(keyFile, passphrase)
		duplicacy.SavePassword(*preference, "rsa_passphrase", passphrase)
	} else {
		backupManager.LoadRSAPrivateKey(keyFile, passphrase)
	}

}

func initRepository(context *cli.Context) {
	configRepository(context, true)
}

func addStorage(context *cli.Context) {
	configRepository(context, false)
}

func configRepository(context *cli.Context, init bool) {

	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	numberOfArgs := 3
	if init {
		numberOfArgs = 2
	}
	if len(context.Args()) != numberOfArgs {
		fmt.Fprintf(context.App.Writer, "The %s command requires %d arguments.\n\n",
			context.Command.Name, numberOfArgs)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	var storageName string
	var snapshotID string
	var storageURL string

	if init {
		storageName = context.String("storage-name")
		if len(storageName) == 0 {
			storageName = "default"
		}
		snapshotID = context.Args()[0]
		storageURL = context.Args()[1]
	} else {
		storageName = context.Args()[0]
		snapshotID = context.Args()[1]
		storageURL = context.Args()[2]

		if strings.ToLower(storageName) == "ssh" {
			duplicacy.LOG_ERROR("PREFERENCE_INVALID", "'%s' is an invalid storage name", storageName)
			return
		}
	}

	snapshotIDRegex := regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)
	matched := snapshotIDRegex.FindStringSubmatch(snapshotID)
	if matched == nil {
		duplicacy.LOG_ERROR("PREFERENCE_INVALID", "'%s' is an invalid snapshot id", snapshotID)
		return
	}

	var repository string
	var err error

	if init {
		repository, err = os.Getwd()
		if err != nil {
			duplicacy.LOG_ERROR("REPOSITORY_PATH", "Failed to retrieve the current working directory: %v", err)
			return
		}

		preferencePath := context.String("pref-dir")
		if preferencePath == "" {
			preferencePath = path.Join(repository, duplicacy.DUPLICACY_DIRECTORY) // TOKEEP
		}

		if stat, _ := os.Stat(path.Join(preferencePath, "preferences")); stat != nil {
			duplicacy.LOG_ERROR("REPOSITORY_INIT", "The repository %s has already been initialized", repository)
			return
		}

		err = os.Mkdir(preferencePath, 0744)
		if err != nil && !os.IsExist(err) {
			duplicacy.LOG_ERROR("REPOSITORY_INIT", "Failed to create the directory %s: %v",
				preferencePath, err)
			return
		}
		if context.String("pref-dir") != "" {
			// out of tree preference file
			// write real path into .duplicacy file inside repository
			duplicacyFileName := path.Join(repository, duplicacy.DUPLICACY_FILE)
			d1 := []byte(preferencePath)
			err = ioutil.WriteFile(duplicacyFileName, d1, 0644)
			if err != nil {
				duplicacy.LOG_ERROR("REPOSITORY_PATH", "Failed to write %s file inside repository  %v", duplicacyFileName, err)
				return
			}
		}
		duplicacy.SetDuplicacyPreferencePath(preferencePath)
		duplicacy.SetKeyringFile(path.Join(preferencePath, "keyring"))

	} else {
		repository, _ = getRepositoryPreference(context, "")
		if duplicacy.FindPreference(storageName) != nil {
			duplicacy.LOG_ERROR("STORAGE_DUPLICATE", "There is already a storage named '%s'", storageName)
			return
		}
	}

	repositoryPath := ""
	if context.String("repository") != "" {
		repositoryPath = context.String("repository")
	}
	preference := duplicacy.Preference{
		Name:           storageName,
		SnapshotID:     snapshotID,
		RepositoryPath: repositoryPath,
		StorageURL:     storageURL,
		Encrypted:      context.Bool("encrypt"),
	}

	storage := duplicacy.CreateStorage(preference, true, 1)
	storagePassword := ""
	if preference.Encrypted {
		prompt := fmt.Sprintf("Enter storage password for %s:", preference.StorageURL)
		storagePassword = duplicacy.GetPassword(preference, "password", prompt, false, true)
	} else {
		if context.String("key") != "" {
			duplicacy.LOG_ERROR("STORAGE_CONFIG", "RSA encryption can't be enabled with an unencrypted storage")
			return
		}
	}

	existingConfig, _, err := duplicacy.DownloadConfig(storage, storagePassword)
	if err != nil {
		duplicacy.LOG_ERROR("STORAGE_CONFIG", "Failed to download the configuration file from the storage: %v", err)
		return
	}

	if existingConfig != nil {
		duplicacy.LOG_INFO("STORAGE_CONFIGURED",
			"The storage '%s' has already been initialized", preference.StorageURL)
		if existingConfig.CompressionLevel >= -1 && existingConfig.CompressionLevel <= 9 {
			duplicacy.LOG_INFO("STORAGE_FORMAT", "This storage is configured to use the pre-1.2.0 format")
		} else if existingConfig.CompressionLevel != 100 {
			duplicacy.LOG_ERROR("STORAGE_COMPRESSION", "This storage is configured with an invalid compression level %d", existingConfig.CompressionLevel)
			return
		} else if existingConfig.CompressionLevel != duplicacy.DEFAULT_COMPRESSION_LEVEL {
			duplicacy.LOG_INFO("STORAGE_COMPRESSION", "Compression level: %d", existingConfig.CompressionLevel)
		}

		// Don't print config in the background mode
		if !duplicacy.RunInBackground {
			existingConfig.Print()
		}
	} else {
		averageChunkSize := duplicacy.AtoSize(context.String("chunk-size"))
		if averageChunkSize == 0 {
			fmt.Fprintf(context.App.Writer, "Invalid average chunk size: %s.\n\n", context.String("chunk-size"))
			cli.ShowCommandHelp(context, context.Command.Name)
			os.Exit(ArgumentExitCode)
		}

		size := 1
		for size*2 <= averageChunkSize {
			size *= 2
		}

		if size != averageChunkSize {
			fmt.Fprintf(context.App.Writer, "Invalid average chunk size: %d is not a power of 2.\n\n",
				averageChunkSize)
			cli.ShowCommandHelp(context, context.Command.Name)
			os.Exit(ArgumentExitCode)
		}

		maximumChunkSize := 4 * averageChunkSize
		minimumChunkSize := averageChunkSize / 4

		if context.String("max-chunk-size") != "" {
			maximumChunkSize = duplicacy.AtoSize(context.String("max-chunk-size"))
			if maximumChunkSize < averageChunkSize {
				fmt.Fprintf(context.App.Writer, "Invalid maximum chunk size: %s.\n\n",
					context.String("max-chunk-size"))
				cli.ShowCommandHelp(context, context.Command.Name)
				os.Exit(ArgumentExitCode)
			}
		}

		if context.String("min-chunk-size") != "" {
			minimumChunkSize = duplicacy.AtoSize(context.String("min-chunk-size"))
			if minimumChunkSize > averageChunkSize || minimumChunkSize == 0 {
				fmt.Fprintf(context.App.Writer, "Invalid minimum chunk size: %s.\n\n",
					context.String("min-chunk-size"))
				cli.ShowCommandHelp(context, context.Command.Name)
				os.Exit(ArgumentExitCode)
			}
		}

		if preference.Encrypted {
			repeatedPassword := duplicacy.GetPassword(preference, "password", "Re-enter storage password:",
				false, true)
			if repeatedPassword != storagePassword {
				duplicacy.LOG_ERROR("STORAGE_PASSWORD", "Storage passwords do not match")
				return
			}
		}

		var otherConfig *duplicacy.Config
		var bitCopy bool
		if context.String("copy") != "" {

			otherPreference := duplicacy.FindPreference(context.String("copy"))

			if otherPreference == nil {
				duplicacy.LOG_ERROR("STORAGE_NOTFOUND", "Storage '%s' can't be found", context.String("copy"))
				return
			}

			otherStorage := duplicacy.CreateStorage(*otherPreference, false, 1)

			otherPassword := ""
			if otherPreference.Encrypted {
				prompt := fmt.Sprintf("Enter storage password for %s:", otherPreference.StorageURL)
				otherPassword = duplicacy.GetPassword(*otherPreference, "password", prompt, false, false)
			}

			otherConfig, _, err = duplicacy.DownloadConfig(otherStorage, otherPassword)
			if err != nil {
				duplicacy.LOG_ERROR("STORAGE_COPY", "Failed to download the configuration file from the storage: %v",
					err)
				return
			}

			if otherConfig == nil {
				duplicacy.LOG_ERROR("STORAGE_NOT_CONFIGURED",
					"The storage to copy the configuration from has not been initialized")
			}

			bitCopy = context.Bool("bit-identical")
		}

		iterations := context.Int("iterations")
		if iterations == 0 {
			iterations = duplicacy.CONFIG_DEFAULT_ITERATIONS
		}

		dataShards := 0
		parityShards := 0
		shards := context.String("erasure-coding")
		if shards != "" {
			shardsRegex := regexp.MustCompile(`^([0-9]+):([0-9]+)$`)
			matched := shardsRegex.FindStringSubmatch(shards)
			if matched == nil {
				duplicacy.LOG_ERROR("STORAGE_ERASURECODE", "Invalid erasure coding parameters: %s", shards)
			} else {
				dataShards, _ = strconv.Atoi(matched[1])
				parityShards, _ = strconv.Atoi(matched[2])
				if dataShards == 0 || dataShards > 256 || parityShards == 0 || parityShards > dataShards {
					duplicacy.LOG_ERROR("STORAGE_ERASURECODE", "Invalid erasure coding parameters: %s", shards)
				}
			}
		}

		compressionLevel := 100
		zstdLevel := context.String("zstd-level")
		if zstdLevel != "" {
			if level, found := duplicacy.ZSTD_COMPRESSION_LEVELS[zstdLevel]; found {
				compressionLevel = level
			} else {
				duplicacy.LOG_ERROR("STORAGE_COMPRESSION", "Invalid zstd compression level: %s", zstdLevel)
			}
		} else if context.Bool("zstd") {
			compressionLevel = duplicacy.ZSTD_COMPRESSION_LEVEL_DEFAULT
		}

		duplicacy.ConfigStorage(storage, iterations, compressionLevel, averageChunkSize, maximumChunkSize,
			minimumChunkSize, storagePassword, otherConfig, bitCopy, context.String("key"), dataShards, parityShards)
	}

	duplicacy.Preferences = append(duplicacy.Preferences, preference)

	duplicacy.SavePreferences()

	if repositoryPath == "" {
		repositoryPath = repository
	}

	duplicacy.LOG_INFO("REPOSITORY_INIT", "%s will be backed up to %s with id %s",
		repositoryPath, preference.StorageURL, preference.SnapshotID)
}

type TriBool struct {
	Value int
}

func (triBool *TriBool) Set(value string) error {
	value = strings.ToLower(value)
	if value == "yes" || value == "true" || value == "1" {
		triBool.Value = 2
		return nil
	} else if value == "no" || value == "false" || value == "0" {
		triBool.Value = 1
		return nil
	} else if value == "" {
		// Only set to true if it hasn't been set before.  This is necessary because for 'encrypt, e' this may
		// be called twice, the second time with a value of ""
		if triBool.Value == 0 {
			triBool.Value = 2
		}
		return nil
	} else {
		return fmt.Errorf("Invalid boolean value '%s'", value)
	}
}

// IsBoolFlag implements the private interface flag.boolFlag to indicate that this is a bool flag so the argument
// is optional
func (triBool *TriBool) IsBoolFlag() bool { return true }

func (triBool *TriBool) String() string {
	return ""
}

func (triBool *TriBool) IsSet() bool {
	return triBool.Value != 0
}

func (triBool *TriBool) IsTrue() bool {
	return triBool.Value == 2
}

func setPreference(context *cli.Context) {

	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) > 0 {
		fmt.Fprintf(context.App.Writer, "The %s command takes no arguments.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	storageName := context.String("storage")

	repository, oldPreference := getRepositoryPreference(context, storageName)

	if oldPreference == nil {
		duplicacy.LOG_ERROR("STORAGE_SET", "The storage '%s' has not been added to the repository %s",
			storageName, repository)
		return
	}

	newPreference := *oldPreference

	triBool := context.Generic("e").(*TriBool)
	if triBool.IsSet() {
		newPreference.Encrypted = triBool.IsTrue()
	}

	triBool = context.Generic("no-backup").(*TriBool)
	if triBool.IsSet() {
		newPreference.BackupProhibited = triBool.IsTrue()
	}

	triBool = context.Generic("no-restore").(*TriBool)
	if triBool.IsSet() {
		newPreference.RestoreProhibited = triBool.IsTrue()
	}

	triBool = context.Generic("no-save-password").(*TriBool)
	if triBool.IsSet() {
		newPreference.DoNotSavePassword = triBool.IsTrue()
	}

	if context.String("nobackup-file") != "" {
		newPreference.NobackupFile = context.String("nobackup-file")
	}

	if context.String("filters") != "" {
		newPreference.FiltersFile = context.String("filters")
	}

	triBool = context.Generic("exclude-by-attribute").(*TriBool)
	if triBool.IsSet() {
		newPreference.ExcludeByAttribute = triBool.IsTrue()
	}

	key := context.String("key")
	value := context.String("value")

	if len(key) > 0 {

		// Make a deep copy of the keys otherwise we would be change both preferences at once.
		newKeys := make(map[string]string)
		for k, v := range newPreference.Keys {
			newKeys[k] = v
		}
		newPreference.Keys = newKeys

		if len(value) == 0 {
			delete(newPreference.Keys, key)
		} else {
			if len(newPreference.Keys) == 0 {
				newPreference.Keys = make(map[string]string)
			}
			newPreference.Keys[key] = value
		}
	}

	if duplicacy.IsTracing() {
		description, _ := json.MarshalIndent(newPreference, "", "    ")
		fmt.Printf("%s\n", description)
	}

	if newPreference.Equal(oldPreference) {
		duplicacy.LOG_INFO("STORAGE_SET", "The options for storage %s have not been modified",
			oldPreference.StorageURL)
	} else {
		*oldPreference = newPreference
		duplicacy.SavePreferences()
		duplicacy.LOG_INFO("STORAGE_SET", "New options for storage %s have been saved", oldPreference.StorageURL)
	}
}

func changePassword(context *cli.Context) {

	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 0 {
		fmt.Fprintf(context.App.Writer, "The %s command requires no arguments.\n\n",
			context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	_, preference := getRepositoryPreference(context, "")

	storage := duplicacy.CreateStorage(*preference, false, 1)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password",
			fmt.Sprintf("Enter old password for storage %s:", preference.StorageURL),
			false, true)
	}

	config, _, err := duplicacy.DownloadConfig(storage, password)
	if err != nil {
		duplicacy.LOG_ERROR("STORAGE_CONFIG", "Failed to download the configuration file from the storage: %v", err)
		return
	}

	if config == nil {
		duplicacy.LOG_ERROR("STORAGE_NOT_CONFIGURED", "The storage has not been initialized")
		return
	}

	newPassword := duplicacy.GetPassword(*preference, "password", "Enter new storage password:", false, true)
	repeatedPassword := duplicacy.GetPassword(*preference, "password", "Re-enter new storage password:", false, true)
	if repeatedPassword != newPassword {
		duplicacy.LOG_ERROR("PASSWORD_CHANGE", "The new passwords do not match")
		return
	}
	if newPassword == password {
		duplicacy.LOG_ERROR("PASSWORD_CHANGE", "The new password is the same as the old one")
		return
	}

	iterations := context.Int("iterations")
	if iterations == 0 {
		iterations = duplicacy.CONFIG_DEFAULT_ITERATIONS
	}

	description, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		duplicacy.LOG_ERROR("CONFIG_MARSHAL", "Failed to marshal the config: %v", err)
		return
	}

	configPath := path.Join(duplicacy.GetDuplicacyPreferencePath(), "config")
	err = ioutil.WriteFile(configPath, description, 0600)
	if err != nil {
		duplicacy.LOG_ERROR("CONFIG_SAVE", "Failed to save the old config to %s: %v", configPath, err)
		return
	}
	duplicacy.LOG_INFO("CONFIG_SAVE", "The old config has been temporarily saved to %s", configPath)

	removeLocalCopy := false
	defer func() {
		if removeLocalCopy {
			err = os.Remove(configPath)
			if err != nil {
				duplicacy.LOG_WARN("CONFIG_CLEAN", "Failed to delete %s: %v", configPath, err)
			} else {
				duplicacy.LOG_INFO("CONFIG_CLEAN", "The local copy of the old config has been removed")
			}
		}
	}()

	err = storage.DeleteFile(0, "config")
	if err != nil {
		duplicacy.LOG_ERROR("CONFIG_DELETE", "Failed to delete the old config from the storage: %v", err)
		return
	}

	duplicacy.UploadConfig(storage, config, newPassword, iterations)

	duplicacy.SavePassword(*preference, "password", newPassword)

	duplicacy.LOG_INFO("STORAGE_SET", "The password for storage %s has been changed", preference.StorageURL)

	removeLocalCopy = true
}

func backupRepository(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 0 {
		fmt.Fprintf(context.App.Writer, "The %s command requires no arguments.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository, preference := getRepositoryPreference(context, "")

	if preference.BackupProhibited {
		duplicacy.LOG_ERROR("BACKUP_DISABLED", "Backup from this repository to %s was disabled by the preference",
			preference.StorageURL)
		return
	}

	runScript(context, preference.Name, "pre")

	threads := context.Int("threads")
	if threads < 1 {
		threads = 1
	}

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, threads)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	quickMode := true
	if context.Bool("hash") {
		quickMode = false
	}

	showStatistics := context.Bool("stats")

	enableVSS := context.Bool("vss")
	vssTimeout := context.Int("vss-timeout")

	dryRun := context.Bool("dry-run")
	uploadRateLimit := context.Int("limit-rate")
	enumOnly := context.Bool("enum-only")
	storage.SetRateLimits(0, uploadRateLimit)
	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, preference.NobackupFile, preference.FiltersFile, preference.ExcludeByAttribute)
	duplicacy.SavePassword(*preference, "password", password)

	backupManager.SetupSnapshotCache(preference.Name)
	backupManager.SetDryRun(dryRun)

	zstdLevel := context.String("zstd-level")
	if zstdLevel != "" {
		if level, found := duplicacy.ZSTD_COMPRESSION_LEVELS[zstdLevel]; found {
			backupManager.SetCompressionLevel(level)
		} else {
			duplicacy.LOG_ERROR("STORAGE_COMPRESSION", "Invalid zstd compression level: %s", zstdLevel)
		}
	} else if context.Bool("zstd") {
		backupManager.SetCompressionLevel(duplicacy.ZSTD_COMPRESSION_LEVEL_DEFAULT)
	}

	metadataChunkSize := context.Int("metadata-chunk-size")
	maximumInMemoryEntries := context.Int("max-in-memory-entries")
	backupManager.Backup(repository, quickMode, threads, context.String("t"), showStatistics, enableVSS, vssTimeout, enumOnly, metadataChunkSize, maximumInMemoryEntries)

	runScript(context, preference.Name, "post")
}

func restoreRepository(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	revision := context.Int("r")
	if revision <= 0 {
		fmt.Fprintf(context.App.Writer, "The revision flag is not specified or invalid\n\n")
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository, preference := getRepositoryPreference(context, "")

	if preference.RestoreProhibited {
		duplicacy.LOG_ERROR("RESTORE_DISABLED", "Restore from %s to this repository was disabled by the preference",
			preference.StorageURL)
		return
	}

	runScript(context, preference.Name, "pre")

	threads := context.Int("threads")
	if threads < 1 {
		threads = 1
	}

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, threads)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	quickMode := !context.Bool("hash")
	overwrite := context.Bool("overwrite")
	deleteMode := context.Bool("delete")
	setOwner := !context.Bool("ignore-owner")

	showStatistics := context.Bool("stats")
	persist := context.Bool("persist")

	var patterns []string
	for _, pattern := range context.Args() {

		pattern = strings.TrimSpace(pattern)

		for strings.HasPrefix(pattern, "--") {
			pattern = pattern[1:]
		}

		for strings.HasPrefix(pattern, "++") {
			pattern = pattern[1:]
		}

		patterns = append(patterns, pattern)
	}

	patterns = duplicacy.ProcessFilterLines(patterns, make([]string, 0))

	duplicacy.LOG_DEBUG("REGEX_DEBUG", "There are %d compiled regular expressions stored", len(duplicacy.RegexMap))

	duplicacy.LOG_INFO("SNAPSHOT_FILTER", "Loaded %d include/exclude pattern(s)", len(patterns))

	storage.SetRateLimits(context.Int("limit-rate"), 0)
	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, preference.NobackupFile, preference.FiltersFile, preference.ExcludeByAttribute)
	duplicacy.SavePassword(*preference, "password", password)

	loadRSAPrivateKey(context.String("key"), context.String("key-passphrase"), preference, backupManager, false)

	backupManager.SetupSnapshotCache(preference.Name)
	failed := backupManager.Restore(repository, revision, true, quickMode, threads, overwrite, deleteMode, setOwner, showStatistics, patterns, persist)
	if failed > 0 {
		duplicacy.LOG_ERROR("RESTORE_FAIL", "%d file(s) were not restored correctly", failed)
		return
	}

	runScript(context, preference.Name, "post")
}

func listSnapshots(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 0 {
		fmt.Fprintf(context.App.Writer, "The %s command requires no arguments.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository, preference := getRepositoryPreference(context, "")

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)

	runScript(context, preference.Name, "pre")

	resetPassword := context.Bool("reset-passwords")
	storage := duplicacy.CreateStorage(*preference, resetPassword, 1)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:",
			false, resetPassword)
	}

	tag := context.String("t")
	revisions := getRevisions(context)

	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, "", "", preference.ExcludeByAttribute)
	duplicacy.SavePassword(*preference, "password", password)

	id := preference.SnapshotID
	if context.Bool("all") {
		id = ""
	} else if context.String("id") != "" {
		id = context.String("id")
	}

	showFiles := context.Bool("files")
	showChunks := context.Bool("chunks")

	// list doesn't need to decrypt file chunks; but we need -key here so we can reset the passphrase for the private key
	loadRSAPrivateKey(context.String("key"), "", preference, backupManager, resetPassword)

	backupManager.SetupSnapshotCache(preference.Name)
	backupManager.SnapshotManager.ListSnapshots(id, revisions, tag, showFiles, showChunks)

	runScript(context, preference.Name, "post")
}

func checkSnapshots(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 0 {
		fmt.Fprintf(context.App.Writer, "The %s command requires no arguments.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository, preference := getRepositoryPreference(context, "")

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)

	runScript(context, preference.Name, "pre")

	threads := context.Int("threads")
	if threads < 1 {
		threads = 1
	}

	storage := duplicacy.CreateStorage(*preference, false, threads)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	tag := context.String("t")
	revisions := getRevisions(context)

	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, "", "", false)
	duplicacy.SavePassword(*preference, "password", password)

	loadRSAPrivateKey(context.String("key"), context.String("key-passphrase"), preference, backupManager, false)

	id := preference.SnapshotID
	if context.Bool("all") {
		id = ""
	} else if context.String("id") != "" {
		id = context.String("id")
	}

	showStatistics := context.Bool("stats")
	showTabular := context.Bool("tabular")
	checkFiles := context.Bool("files")
	checkChunks := context.Bool("chunks")
	searchFossils := context.Bool("fossils")
	resurrect := context.Bool("resurrect")
	rewrite := context.Bool("rewrite")
	persist := context.Bool("persist")

	backupManager.SetupSnapshotCache(preference.Name)
	backupManager.SnapshotManager.CheckSnapshots(id, revisions, tag, showStatistics, showTabular, checkFiles, checkChunks, searchFossils, resurrect, rewrite, threads, persist)

	runScript(context, preference.Name, "post")
}

func printFile(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) > 1 {
		fmt.Fprintf(context.App.Writer, "The %s command requires at most 1 argument.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository, preference := getRepositoryPreference(context, "")

	runScript(context, preference.Name, "pre")

	// Do not print out storage for this command
	//duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, 1)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	revision := context.Int("r")

	snapshotID := preference.SnapshotID
	if context.String("id") != "" {
		snapshotID = context.String("id")
	}

	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, "", "", false)
	duplicacy.SavePassword(*preference, "password", password)

	loadRSAPrivateKey(context.String("key"), context.String("key-passphrase"), preference, backupManager, false)

	backupManager.SetupSnapshotCache(preference.Name)

	file := ""
	if len(context.Args()) > 0 {
		file = context.Args()[0]
	}
	backupManager.SnapshotManager.PrintFile(snapshotID, revision, file)

	runScript(context, preference.Name, "post")
}

func diff(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) > 1 {
		fmt.Fprintf(context.App.Writer, "The %s command requires 0 or 1 argument.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository, preference := getRepositoryPreference(context, "")

	runScript(context, preference.Name, "pre")

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, 1)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	revisions := context.IntSlice("r")
	if len(revisions) > 2 {
		fmt.Fprintf(context.App.Writer, "The %s command requires at most 2 revisions.\n", context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	snapshotID := preference.SnapshotID
	if context.String("id") != "" {
		snapshotID = context.String("id")
	}

	path := ""
	if len(context.Args()) > 0 {
		path = context.Args()[0]
	}

	compareByHash := context.Bool("hash")
	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, "", "", false)
	duplicacy.SavePassword(*preference, "password", password)

	loadRSAPrivateKey(context.String("key"), context.String("key-passphrase"), preference, backupManager, false)

	backupManager.SetupSnapshotCache(preference.Name)
	backupManager.SnapshotManager.Diff(repository, snapshotID, revisions, path, compareByHash, preference.NobackupFile, preference.FiltersFile, preference.ExcludeByAttribute)

	runScript(context, preference.Name, "post")
}

func showHistory(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 1 {
		fmt.Fprintf(context.App.Writer, "The %s command requires 1 argument.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository, preference := getRepositoryPreference(context, "")

	runScript(context, preference.Name, "pre")

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, 1)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	snapshotID := preference.SnapshotID
	if context.String("id") != "" {
		snapshotID = context.String("id")
	}

	path := context.Args()[0]

	revisions := getRevisions(context)
	showLocalHash := context.Bool("hash")
	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, "", "", false)
	duplicacy.SavePassword(*preference, "password", password)

	backupManager.SetupSnapshotCache(preference.Name)
	backupManager.SnapshotManager.ShowHistory(repository, snapshotID, revisions, path, showLocalHash)

	runScript(context, preference.Name, "post")
}

func pruneSnapshots(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 0 {
		fmt.Fprintf(context.App.Writer, "The %s command requires no arguments.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	threads := context.Int("threads")
	if threads < 1 {
		threads = 1
	}

	repository, preference := getRepositoryPreference(context, "")

	runScript(context, preference.Name, "pre")

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, threads)
	if storage == nil {
		return
	}

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	revisions := getRevisions(context)
	tags := context.StringSlice("t")
	retentions := context.StringSlice("keep")
	selfID := preference.SnapshotID
	snapshotID := preference.SnapshotID
	if context.Bool("all") {
		snapshotID = ""
	} else if context.String("id") != "" {
		snapshotID = context.String("id")
	}

	ignoredIDs := context.StringSlice("ignore")
	exhaustive := context.Bool("exhaustive")
	exclusive := context.Bool("exclusive")
	dryRun := context.Bool("dry-run")
	deleteOnly := context.Bool("delete-only")
	collectOnly := context.Bool("collect-only")

	if !storage.IsMoveFileImplemented() && !exclusive {
		fmt.Fprintf(context.App.Writer, "The --exclusive option must be enabled for storage %s\n",
			preference.StorageURL)
		os.Exit(ArgumentExitCode)
	}

	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, "", "", false)
	duplicacy.SavePassword(*preference, "password", password)

	backupManager.SetupSnapshotCache(preference.Name)
	backupManager.SnapshotManager.PruneSnapshots(selfID, snapshotID, revisions, tags, retentions,
		exhaustive, exclusive, ignoredIDs, dryRun, deleteOnly, collectOnly, threads)

	runScript(context, preference.Name, "post")
}

func copySnapshots(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 0 {
		fmt.Fprintf(context.App.Writer, "The %s command requires no arguments.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	uploadingThreads := context.Int("threads")
	if uploadingThreads < 1 {
		uploadingThreads = 1
	}

	downloadingThreads := context.Int("download-threads")
	if downloadingThreads < 1 {
		downloadingThreads = 1
	}

	repository, source := getRepositoryPreference(context, context.String("from"))

	runScript(context, source.Name, "pre")

	duplicacy.LOG_INFO("STORAGE_SET", "Source storage set to %s", source.StorageURL)
	sourceStorage := duplicacy.CreateStorage(*source, false, downloadingThreads)
	if sourceStorage == nil {
		return
	}

	sourcePassword := ""
	if source.Encrypted {
		sourcePassword = duplicacy.GetPassword(*source, "password", "Enter source storage password:", false, false)
	}

	sourceManager := duplicacy.CreateBackupManager(source.SnapshotID, sourceStorage, repository, sourcePassword, "", "", false)
	sourceManager.SetupSnapshotCache(source.Name)
	duplicacy.SavePassword(*source, "password", sourcePassword)

	loadRSAPrivateKey(context.String("key"), context.String("key-passphrase"), source, sourceManager, false)

	_, destination := getRepositoryPreference(context, context.String("to"))

	if destination.Name == source.Name {
		duplicacy.LOG_ERROR("COPY_IDENTICAL", "The source storage and the destination storage are the same")
		return
	}

	if destination.BackupProhibited {
		duplicacy.LOG_ERROR("COPY_DISABLED", "Copying snapshots to %s was disabled by the preference",
			destination.StorageURL)
		return
	}

	duplicacy.LOG_INFO("STORAGE_SET", "Destination storage set to %s", destination.StorageURL)
	destinationStorage := duplicacy.CreateStorage(*destination, false, uploadingThreads)
	if destinationStorage == nil {
		return
	}

	destinationPassword := ""
	if destination.Encrypted {
		destinationPassword = duplicacy.GetPassword(*destination, "password",
			"Enter destination storage password:", false, false)
	}

	sourceStorage.SetRateLimits(context.Int("download-limit-rate"), 0)
	destinationStorage.SetRateLimits(0, context.Int("upload-limit-rate"))

	destinationManager := duplicacy.CreateBackupManager(destination.SnapshotID, destinationStorage, repository,
		destinationPassword, "", "", false)
	duplicacy.SavePassword(*destination, "password", destinationPassword)
	destinationManager.SetupSnapshotCache(destination.Name)

	revisions := getRevisions(context)
	snapshotID := ""
	if context.String("id") != "" {
		snapshotID = context.String("id")
	}

	sourceManager.CopySnapshots(destinationManager, snapshotID, revisions, uploadingThreads, downloadingThreads)
	runScript(context, source.Name, "post")
}

func infoStorage(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 1 {
		fmt.Fprintf(context.App.Writer, "The %s command requires a storage URL argument.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository := context.String("repository")
	if repository != "" {
		preferencePath := path.Join(repository, duplicacy.DUPLICACY_DIRECTORY)
		duplicacy.SetDuplicacyPreferencePath(preferencePath)
		duplicacy.SetKeyringFile(path.Join(preferencePath, "keyring"))
	}

	resetPasswords := context.Bool("reset-passwords")
	isEncrypted := context.Bool("e")
	preference := duplicacy.Preference{
		Name:              "default",
		SnapshotID:        "default",
		StorageURL:        context.Args()[0],
		Encrypted:         isEncrypted,
		DoNotSavePassword: true,
	}

	storageName := context.String("storage-name")
	if storageName != "" {
		preference.Name = storageName
	}

	if resetPasswords {
		// We don't want password entered for the info command to overwrite the saved password for the default storage,
		// so we simply assign an empty name.
		preference.Name = ""
	}

	password := ""
	if isEncrypted {
		password = duplicacy.GetPassword(preference, "password", "Enter the storage password:", false, resetPasswords)
	}

	storage := duplicacy.CreateStorage(preference, resetPasswords, 1)
	config, isStorageEncrypted, err := duplicacy.DownloadConfig(storage, password)

	if isStorageEncrypted {
		duplicacy.LOG_INFO("STORAGE_ENCRYPTED", "The storage is encrypted with a password")
	} else if err != nil {
		duplicacy.LOG_ERROR("STORAGE_ERROR", "%v", err)
	} else if config == nil {
		duplicacy.LOG_INFO("STORAGE_NOT_INITIALIZED", "The storage has not been initialized")
	} else {
		config.Print()
	}

	dirs, _, err := storage.ListFiles(0, "snapshots/")
	if err != nil {
		duplicacy.LOG_WARN("STORAGE_LIST", "Failed to list repository ids: %v", err)
		return
	}

	for _, dir := range dirs {
		if len(dir) > 0 && dir[len(dir)-1] == '/' {
			duplicacy.LOG_INFO("STORAGE_SNAPSHOT", "%s", dir[0:len(dir)-1])
		}
	}

}

func benchmark(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	fileSize := context.Int("file-size")
	if fileSize == 0 {
		fileSize = 256
	}

	chunkCount := context.Int("chunk-count")
	if chunkCount == 0 {
		chunkCount = 64
	}

	chunkSize := context.Int("chunk-size")
	if chunkSize == 0 {
		chunkSize = 4
	}

	downloadThreads := context.Int("download-threads")
	if downloadThreads < 1 {
		downloadThreads = 1
	}

	uploadThreads := context.Int("upload-threads")
	if uploadThreads < 1 {
		uploadThreads = 1
	}

	threads := downloadThreads
	if threads < uploadThreads {
		threads = uploadThreads
	}

	repository, preference := getRepositoryPreference(context, context.String("storage"))

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, threads)
	if storage == nil {
		return
	}
	duplicacy.Benchmark(repository, storage, int64(fileSize)*1024*1024, chunkSize*1024*1024, chunkCount, uploadThreads, downloadThreads)
}

func mountSnapshot(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) != 1 {
		fmt.Fprintf(context.App.Writer, "The %s command requires mount directory argument.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	setGlobalOptions(context)

	repository, preference := getRepositoryPreference(context, context.String("storage"))

	runScript(context, preference.Name, "pre")

	threads := context.Int("threads")
	if threads < 1 {
		threads = 1
	}

	duplicacy.LOG_INFO("STORAGE_SET", "Storage set to %s", preference.StorageURL)
	storage := duplicacy.CreateStorage(*preference, false, threads)
	if storage == nil {
		duplicacy.LOG_ERROR("MOUNTING_FILESYSTEM", "No storage was created.")
		return
	}

	rateLimit := context.Int("limit-rate")
	storage.SetRateLimits(rateLimit, 0)

	password := ""
	if preference.Encrypted {
		password = duplicacy.GetPassword(*preference, "password", "Enter storage password:", false, false)
	}

	duplicacy.LOG_DEBUG("REGEX_DEBUG", "There are %d compiled regular expressions stored", len(duplicacy.RegexMap))

	backupManager := duplicacy.CreateBackupManager(preference.SnapshotID, storage, repository, password, preference.NobackupFile, preference.FiltersFile, preference.ExcludeByAttribute)

	loadRSAPrivateKey(context.String("key"), context.String("key-passphrase"), preference, backupManager, false)

	backupManager.SetupSnapshotCache(preference.Name)

	duplicacy.MountFileSystem(
		context.Args()[0],
		backupManager,
		&duplicacy.MountOptions{
			Flat:      context.Bool("flat"),
			Revisions: context.String("revisions"),
			DiskCache: context.Bool("disk-cache"),
			Threads:   threads,
			Snapshots: []string{preference.SnapshotID},
		})
}

func mountStorage(context *cli.Context) {
	setGlobalOptions(context)
	defer duplicacy.CatchLogException()

	if len(context.Args()) < 1 {
		fmt.Fprintf(context.App.Writer, "The %s command requires a storage URL argument.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	if len(context.Args()) < 2 {
		fmt.Fprintf(context.App.Writer, "The %s command requires mount directory argument.\n\n", context.Command.Name)
		cli.ShowCommandHelp(context, context.Command.Name)
		os.Exit(ArgumentExitCode)
	}

	repository := context.String("repository")
	if repository != "" {
		preferencePath := path.Join(repository, duplicacy.DUPLICACY_DIRECTORY)
		duplicacy.SetDuplicacyPreferencePath(preferencePath)
		duplicacy.SetKeyringFile(path.Join(preferencePath, "keyring"))
	}

	resetPasswords := context.Bool("reset-passwords")
	isEncrypted := context.Bool("e")
	preference := duplicacy.Preference{
		Name:              "_MountStorage",
		SnapshotID:        "default",
		StorageURL:        context.Args()[0],
		Encrypted:         isEncrypted,
		DoNotSavePassword: true,
	}

	if resetPasswords {
		// We don't want password entered for the info command to overwrite the saved password for the default storage,
		// so we simply assign an empty name.
		preference.Name = ""
	}

	password := ""
	if isEncrypted {
		password = duplicacy.GetPassword(preference, "password", "Enter the storage password:", false, resetPasswords)
	}

	threads := context.Int("threads")
	if threads < 1 {
		threads = 1
	}

	storage := duplicacy.CreateStorage(preference, resetPasswords, threads)
	if storage == nil {
		duplicacy.LOG_ERROR("MOUNTING_FILESYSTEM", "No storage was created.")
		return
	}

	rateLimit := context.Int("limit-rate")
	storage.SetRateLimits(rateLimit, 0)

	config, isStorageEncrypted, err := duplicacy.DownloadConfig(storage, password)

	if isStorageEncrypted {
		duplicacy.LOG_INFO("STORAGE_ENCRYPTED", "The storage is encrypted with a password")
	} else if err != nil {
		duplicacy.LOG_ERROR("STORAGE_ERROR", "%v", err)
	} else if config == nil {
		duplicacy.LOG_INFO("STORAGE_NOT_INITIALIZED", "The storage has not been initialized")
	} else {
		config.Print()
	}

	dirs, _, err := storage.ListFiles(0, "snapshots/")
	if err != nil {
		duplicacy.LOG_WARN("STORAGE_LIST", "Failed to list repository ids: %v", err)
		return
	}

	snapshotsToLoad := make(map[string]bool)
	loadSpecific := false
	for _, id := range strings.Split(context.String("snapshots"), ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}

		snapshotsToLoad[id] = true
		loadSpecific = true
	}

	snapshots := []string{}
	for _, dir := range dirs {
		if len(dir) > 0 && dir[len(dir)-1] == '/' {
			id := dir[0 : len(dir)-1]
			if loadSpecific {
				_, ok := snapshotsToLoad[id]
				if !ok {
					duplicacy.LOG_DEBUG("MOUNT_STORAGE", "Ignoring snapshot %s.", id)
					continue
				}
			}
			duplicacy.LOG_INFO("STORAGE_SNAPSHOT", "Load snapshot id: %s", id)
			snapshots = append(snapshots, id)
		}
	}

	if len(snapshots) == 0 {
		duplicacy.LOG_WARN("MOUNT_STORAGE", "No snapshots to load, quitting.")
		return
	}

	revisions := context.String("revisions")
	if len(snapshots) > 1 && revisions != "" {
		revisions = ""
		duplicacy.LOG_WARN("MOUNT_STORAGE", "Loading multiple snapshots, -revisions parameter is being ignored.")
	}

	if len(snapshots) == 1 {
		duplicacy.LOG_WARN("MOUNT_STORAGE", "A single snapshot was found, using at as root.")
	}

	backupManager := duplicacy.CreateBackupManager("", storage, "", password, preference.NobackupFile, preference.FiltersFile, preference.ExcludeByAttribute)
	loadRSAPrivateKey(context.String("key"), context.String("key-passphrase"), &preference, backupManager, false)

	if repository != "" {
		backupManager.SetupSnapshotCache(preference.Name)
	} else {
		cacheDir, err := ioutil.TempDir("", "duplicacy-mount-storagecache")
		if err != nil {
			duplicacy.LOG_ERROR("MOUNT_STORAGE", "Failed to create temp dir.")
			return
		}
		duplicacy.LOG_DEBUG("MOUNT_STORAGE", "Using '%s' as the storage cache dir", cacheDir)
		defer os.RemoveAll(cacheDir)
		backupManager.SetupSnapshotCacheDir(cacheDir)
	}

	duplicacy.MountFileSystem(
		context.Args()[1],
		backupManager,
		&duplicacy.MountOptions{
			Flat:      context.Bool("flat"),
			Revisions: revisions,
			DiskCache: context.Bool("disk-cache"),
			Threads:   threads,
			Snapshots: snapshots,
		})
}

func main() {

	duplicacy.SetLoggingLevel(duplicacy.INFO)

	app := cli.NewApp()

	app.Commands = []cli.Command{
		{
			Name: "init",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "encrypt, e",
					Usage: "encrypt the storage with a password",
				},
				cli.StringFlag{
					Name:     "chunk-size, c",
					Value:    "4M",
					Usage:    "the average size of chunks (defaults to 4M)",
					Argument: "<size>",
				},
				cli.StringFlag{
					Name:     "max-chunk-size, max",
					Usage:    "the maximum size of chunks (defaults to chunk-size*4)",
					Argument: "<size>",
				},
				cli.StringFlag{
					Name:     "min-chunk-size, min",
					Usage:    "the minimum size of chunks (defaults to chunk-size/4)",
					Argument: "<size>",
				},
				cli.StringFlag{
					Name:     "zstd-level",
					Usage:    "set zstd compression level (fast, default, better, or best)",
					Argument: "<level>",
				},
				cli.BoolFlag{
					Name:     "zstd",
					Usage:    "short for -zstd default",
				},
				cli.IntFlag{
					Name:     "iterations",
					Usage:    "the number of iterations used in storage key derivation (default is 16384)",
					Argument: "<i>",
				},
				cli.StringFlag{
					Name:     "pref-dir",
					Usage:    "alternate location for the .duplicacy directory (absolute or relative to current directory)",
					Argument: "<path>",
				},
				cli.StringFlag{
					Name:     "storage-name",
					Usage:    "assign a name to the storage",
					Argument: "<name>",
				},
				cli.StringFlag{
					Name:     "repository",
					Usage:    "initialize a new repository at the specified path rather than the current working directory",
					Argument: "<path>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA public key to encrypt file chunks",
					Argument: "<public key>",
				},
				cli.StringFlag{
					Name:     "erasure-coding",
					Usage:    "enable erasure coding to protect against storage corruption",
					Argument: "<data shards>:<parity shards>",
				},
			},
			Usage:     "Initialize the storage if necessary and the current directory as the repository",
			ArgsUsage: "<snapshot id> <storage url>",
			Action:    initRepository,
		},
		{
			Name: "backup",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "hash",
					Usage: "detect file differences by hash (rather than size and timestamp)",
				},
				cli.StringFlag{
					Name:     "t",
					Usage:    "assign a tag to the backup",
					Argument: "<tag>",
				},
				cli.BoolFlag{
					Name:  "stats",
					Usage: "show statistics during and after backup",
				},
				cli.IntFlag{
					Name:     "threads",
					Value:    1,
					Usage:    "number of uploading threads",
					Argument: "<n>",
				},
				cli.IntFlag{
					Name:     "limit-rate",
					Value:    0,
					Usage:    "the maximum upload rate (in kilobytes/sec)",
					Argument: "<kB/s>",
				},
				cli.BoolFlag{
					Name:  "dry-run",
					Usage: "dry run for testing, don't backup anything. Use with -stats and -d",
				},
				cli.StringFlag{
					Name:     "zstd-level",
					Usage:    "set zstd compression level (fast, default, better, or best)",
					Argument: "<level>",
				},
				cli.BoolFlag{
					Name:     "zstd",
					Usage:    "short for -zstd default",
				},
				cli.BoolFlag{
					Name:  "vss",
					Usage: "enable the Volume Shadow Copy service (Windows and macOS using APFS only)",
				},
				cli.IntFlag{
					Name:     "vss-timeout",
					Value:    0,
					Usage:    "the timeout in seconds to wait for the Volume Shadow Copy operation to complete",
					Argument: "<timeout>",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "backup to the specified storage instead of the default one",
					Argument: "<storage name>",
				},
				cli.BoolFlag{
					Name:  "enum-only",
					Usage: "enumerate the repository recursively and then exit",
				},
				cli.IntFlag{
					Name:     "metadata-chunk-size",
					Value:    1024 * 1024,
					Usage:    "the average size of metadata chunks (defaults to 1M)",
					Argument: "<size>",
				},
				cli.IntFlag{
					Name:     "max-in-memory-entries",
					Value:    1024 * 1024,
					Usage:    "the maximum number of entries kept in memory (defaults to 1M)",
					Argument: "<number>",
				},
			},
			Usage:     "Save a snapshot of the repository to the storage",
			ArgsUsage: " ",
			Action:    backupRepository,
		},

		{
			Name: "restore",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:     "r",
					Usage:    "the revision number of the snapshot (required)",
					Argument: "<revision>",
				},
				cli.BoolFlag{
					Name:  "hash",
					Usage: "detect file differences by hash (rather than size and timestamp)",
				},
				cli.BoolFlag{
					Name:  "overwrite",
					Usage: "overwrite existing files in the repository",
				},
				cli.BoolFlag{
					Name:  "delete",
					Usage: "delete files not in the snapshot",
				},
				cli.BoolFlag{
					Name:  "ignore-owner",
					Usage: "do not set the original uid/gid on restored files",
				},
				cli.BoolFlag{
					Name:  "stats",
					Usage: "show statistics during and after restore",
				},
				cli.IntFlag{
					Name:     "threads",
					Value:    1,
					Usage:    "number of downloading threads",
					Argument: "<n>",
				},
				cli.IntFlag{
					Name:     "limit-rate",
					Value:    0,
					Usage:    "the maximum download rate (in kilobytes/sec)",
					Argument: "<kB/s>",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "restore from the specified storage instead of the default one",
					Argument: "<storage name>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks",
					Argument: "<private key>",
				},
				cli.BoolFlag{
					Name:  "persist",
					Usage: "continue processing despite chunk errors or existing files (without -overwrite), reporting any affected files",
				},
				cli.StringFlag{
					Name:     "key-passphrase",
					Usage:    "the passphrase to decrypt the RSA private key",
					Argument: "<private key passphrase>",
				},
			},
			Usage:     "Restore the repository to a previously saved snapshot",
			ArgsUsage: "[--] [pattern] ...",
			Action:    restoreRepository,
		},

		{
			Name: "list",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "all, a",
					Usage: "list snapshots with any id",
				},
				cli.StringFlag{
					Name:     "id",
					Usage:    "list snapshots with the specified id rather than the default one",
					Argument: "<snapshot id>",
				},
				cli.StringSliceFlag{
					Name:     "r",
					Usage:    "the revision number of the snapshot",
					Argument: "<revision>",
				},
				cli.StringFlag{
					Name:     "t",
					Usage:    "list snapshots with the specified tag",
					Argument: "<tag>",
				},
				cli.BoolFlag{
					Name:  "files",
					Usage: "print the file list in each snapshot",
				},
				cli.BoolFlag{
					Name:  "chunks",
					Usage: "print chunks in each snapshot or all chunks if no snapshot specified",
				},
				cli.BoolFlag{
					Name:  "reset-passwords",
					Usage: "take passwords from input rather than keychain/keyring",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "retrieve snapshots from the specified storage",
					Argument: "<storage name>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks",
					Argument: "<private key>",
				},
			},
			Usage:     "List snapshots",
			ArgsUsage: " ",
			Action:    listSnapshots,
		},
		{
			Name: "check",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "all, a",
					Usage: "check snapshots with any id",
				},
				cli.StringFlag{
					Name:     "id",
					Usage:    "check snapshots with the specified id rather than the default one",
					Argument: "<snapshot id>",
				},
				cli.StringSliceFlag{
					Name:     "r",
					Usage:    "the revision number of the snapshot",
					Argument: "<revision>",
				},
				cli.StringFlag{
					Name:     "t",
					Usage:    "check snapshots with the specified tag",
					Argument: "<tag>",
				},
				cli.BoolFlag{
					Name:  "fossils",
					Usage: "search fossils if a chunk can't be found",
				},
				cli.BoolFlag{
					Name:  "resurrect",
					Usage: "turn referenced fossils back into chunks",
				},
				cli.BoolFlag{
					Name:  "rewrite",
					Usage: "rewrite chunks with recoverable corruption",
				},
				cli.BoolFlag{
					Name:  "files",
					Usage: "verify the integrity of every file",
				},
				cli.BoolFlag{
					Name:  "chunks",
					Usage: "verify the integrity of every chunk",
				},
				cli.BoolFlag{
					Name:  "stats",
					Usage: "show deduplication statistics (imply -all and all revisions)",
				},
				cli.BoolFlag{
					Name:  "tabular",
					Usage: "show tabular usage and deduplication statistics (imply -stats, -all, and all revisions)",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "retrieve snapshots from the specified storage",
					Argument: "<storage name>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks",
					Argument: "<private key>",
				},
				cli.StringFlag{
					Name:     "key-passphrase",
					Usage:    "the passphrase to decrypt the RSA private key",
					Argument: "<private key passphrase>",
				},
				cli.IntFlag{
					Name:     "threads",
					Value:    1,
					Usage:    "number of threads used to verify chunks",
					Argument: "<n>",
				},
				cli.BoolFlag{
					Name:  "persist",
					Usage: "continue processing despite chunk errors, reporting any affected (corrupted) files",
				},
			},
			Usage:     "Check the integrity of snapshots",
			ArgsUsage: " ",
			Action:    checkSnapshots,
		},
		{
			Name: "cat",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "id",
					Usage:    "retrieve from the snapshot with the specified id",
					Argument: "<snapshot id>",
				},
				cli.IntFlag{
					Name:     "r",
					Usage:    "the revision number of the snapshot",
					Argument: "<revision>",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "retrieve the file from the specified storage",
					Argument: "<storage name>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks",
					Argument: "<private key>",
				},
				cli.StringFlag{
					Name:     "key-passphrase",
					Usage:    "the passphrase to decrypt the RSA private key",
					Argument: "<private key passphrase>",
				},
			},
			Usage:     "Print to stdout the specified file, or the snapshot content if no file is specified",
			ArgsUsage: "[<file>]",
			Action:    printFile,
		},

		{
			Name: "diff",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "id",
					Usage:    "diff snapshots with the specified id",
					Argument: "<snapshot id>",
				},
				cli.IntSliceFlag{
					Name:     "r",
					Usage:    "the revision number of the snapshot",
					Argument: "<revision>",
				},
				cli.BoolFlag{
					Name:  "hash",
					Usage: "compute the hashes of on-disk files",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "retrieve files from the specified storage",
					Argument: "<storage name>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks",
					Argument: "<private key>",
				},
				cli.StringFlag{
					Name:     "key-passphrase",
					Usage:    "the passphrase to decrypt the RSA private key",
					Argument: "<private key passphrase>",
				},
			},
			Usage:     "Compare two snapshots or two revisions of a file",
			ArgsUsage: "[<file>]",
			Action:    diff,
		},

		{
			Name: "history",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "id",
					Usage:    "find the file in the snapshot with the specified id",
					Argument: "<snapshot id>",
				},
				cli.StringSliceFlag{
					Name:     "r",
					Usage:    "show history of the specified revisions",
					Argument: "<revision>",
				},
				cli.BoolFlag{
					Name:  "hash",
					Usage: "show the hash of the on-disk file",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "retrieve files from the specified storage",
					Argument: "<storage name>",
				},
			},
			Usage:     "Show the history of a file",
			ArgsUsage: "<file>",
			Action:    showHistory,
		},

		{
			Name: "prune",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "id",
					Usage:    "delete snapshots with the specified id instead of the default one",
					Argument: "<snapshot id>",
				},
				cli.BoolFlag{
					Name:  "all, a",
					Usage: "match against all snapshot IDs",
				},
				cli.StringSliceFlag{
					Name:     "r",
					Usage:    "delete snapshots with the specified revisions",
					Argument: "<revision>",
				},
				cli.StringSliceFlag{
					Name:     "t",
					Usage:    "delete snapshots with the specified tags",
					Argument: "<tag>",
				},
				cli.StringSliceFlag{
					Name:     "keep",
					Usage:    "keep 1 snapshot every n days for snapshots older than m days",
					Argument: "<n:m>",
				},
				cli.BoolFlag{
					Name:  "exhaustive",
					Usage: "remove all unreferenced chunks (not just those referenced by deleted snapshots)",
				},
				cli.BoolFlag{
					Name:  "exclusive",
					Usage: "assume exclusive access to the storage (disable two-step fossil collection)",
				},
				cli.BoolFlag{
					Name:  "dry-run, d",
					Usage: "show what would have been deleted",
				},
				cli.BoolFlag{
					Name:  "delete-only",
					Usage: "delete fossils previously collected (if deletable) and don't collect fossils",
				},
				cli.BoolFlag{
					Name:  "collect-only",
					Usage: "identify and collect fossils, but don't delete fossils previously collected",
				},
				cli.StringSliceFlag{
					Name:     "ignore",
					Usage:    "ignore snapshots with the specified id when deciding if fossils can be deleted",
					Argument: "<id>",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "prune snapshots from the specified storage",
					Argument: "<storage name>",
				},
				cli.IntFlag{
					Name:     "threads",
					Value:    1,
					Usage:    "number of threads used to prune unreferenced chunks",
					Argument: "<n>",
				},
			},
			Usage:     "Prune snapshots by revision, tag, or retention policy",
			ArgsUsage: " ",
			Action:    pruneSnapshots,
		},

		{
			Name: "password",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "storage",
					Usage:    "change the password used to access the specified storage",
					Argument: "<storage name>",
				},
				cli.IntFlag{
					Name:     "iterations",
					Usage:    "the number of iterations used in storage key derivation (default is 16384)",
					Argument: "<i>",
				},
			},
			Usage:     "Change the storage password",
			ArgsUsage: " ",
			Action:    changePassword,
		},

		{
			Name: "add",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "encrypt, e",
					Usage: "encrypt the storage with a password",
				},
				cli.StringFlag{
					Name:     "chunk-size, c",
					Value:    "4M",
					Usage:    "the average size of chunks (default is 4M)",
					Argument: "<size>",
				},
				cli.StringFlag{
					Name:     "max-chunk-size, max",
					Usage:    "the maximum size of chunks (default is chunk-size*4)",
					Argument: "<size>",
				},
				cli.StringFlag{
					Name:     "min-chunk-size, min",
					Usage:    "the minimum size of chunks (default is chunk-size/4)",
					Argument: "<size>",
				},
				cli.StringFlag{
					Name:     "zstd-level",
					Usage:    "set zstd compression level (fast, default, better, or best)",
					Argument: "<level>",
				},
				cli.BoolFlag{
					Name:     "zstd",
					Usage:    "short for -zstd default",
				},
				cli.IntFlag{
					Name:     "iterations",
					Usage:    "the number of iterations used in storage key derivation (default is 16384)",
					Argument: "<i>",
				},
				cli.StringFlag{
					Name:     "copy",
					Usage:    "make the new storage compatible with an existing one to allow for copy operations",
					Argument: "<storage name>",
				},
				cli.BoolFlag{
					Name:  "bit-identical",
					Usage: "(when using -copy) make the new storage bit-identical to also allow rsync etc.",
				},
				cli.StringFlag{
					Name:     "repository",
					Usage:    "specify the path of the repository (instead of the current working directory)",
					Argument: "<path>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA public key to encrypt file chunks",
					Argument: "<public key>",
				},
				cli.StringFlag{
					Name:     "erasure-coding",
					Usage:    "enable erasure coding to protect against storage corruption",
					Argument: "<data shards>:<parity shards>",
				},
			},
			Usage:     "Add an additional storage to be used for the existing repository",
			ArgsUsage: "<storage name> <snapshot id> <storage url>",
			Action:    addStorage,
		},

		{
			Name: "set",
			Flags: []cli.Flag{
				cli.GenericFlag{
					Name:  "encrypt, e",
					Usage: "encrypt the storage with a password",
					Value: &TriBool{},
					Arg:   "true",
				},
				cli.GenericFlag{
					Name:  "no-backup",
					Usage: "backup to this storage is prohibited",
					Value: &TriBool{},
					Arg:   "true",
				},
				cli.GenericFlag{
					Name:  "no-restore",
					Usage: "restore from this storage is prohibited",
					Value: &TriBool{},
					Arg:   "true",
				},
				cli.GenericFlag{
					Name:  "no-save-password",
					Usage: "don't save password or access keys to keychain/keyring",
					Value: &TriBool{},
					Arg:   "true",
				},
				cli.StringFlag{
					Name:     "nobackup-file",
					Usage:    "Directories containing a file with this name will not be backed up",
					Argument: "<file name>",
					Value:    "",
				},
				cli.GenericFlag{
					Name:  "exclude-by-attribute",
					Usage: "Exclude files based on file attributes. (macOS only, com_apple_backup_excludeItem)",
					Value: &TriBool{},
					Arg:   "true",
				},
				cli.StringFlag{
					Name:  "key",
					Usage: "add a key/password whose value is supplied by the -value option",
				},
				cli.StringFlag{
					Name:  "value",
					Usage: "the value of the key/password",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "use the specified storage instead of the default one",
					Argument: "<storage name>",
				},
				cli.StringFlag{
					Name:     "filters",
					Usage:    "specify the path of the filters file containing include/exclude patterns",
					Argument: "<file path>",
				},
			},
			Usage:     "Change the options for the default or specified storage",
			ArgsUsage: " ",
			Action:    setPreference,
		},
		{
			Name: "copy",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:     "id",
					Usage:    "copy snapshots with the specified id instead of all snapshot ids",
					Argument: "<snapshot id>",
				},
				cli.StringSliceFlag{
					Name:     "r",
					Usage:    "copy snapshots with the specified revisions",
					Argument: "<revision>",
				},
				cli.StringFlag{
					Name:     "from",
					Usage:    "copy snapshots from the specified storage",
					Argument: "<storage name>",
				},
				cli.StringFlag{
					Name:     "to",
					Usage:    "copy snapshots to the specified storage",
					Argument: "<storage name>",
				},
				cli.IntFlag{
					Name:     "download-limit-rate",
					Value:    0,
					Usage:    "the maximum download rate (in kilobytes/sec)",
					Argument: "<kB/s>",
				},
				cli.IntFlag{
					Name:     "upload-limit-rate",
					Value:    0,
					Usage:    "the maximum upload rate (in kilobytes/sec)",
					Argument: "<kB/s>",
				},
				cli.IntFlag{
					Name:     "threads",
					Value:    1,
					Usage:    "number of uploading threads",
					Argument: "<n>",
				},
				cli.IntFlag{
					Name:     "download-threads",
					Value:    1,
					Usage:    "number of downloading threads",
					Argument: "<n>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks from the source storage",
					Argument: "<private key>",
				},
				cli.StringFlag{
					Name:     "key-passphrase",
					Usage:    "the passphrase to decrypt the RSA private key",
					Argument: "<private key passphrase>",
				},
			},
			Usage:     "Copy snapshots between compatible storages",
			ArgsUsage: " ",
			Action:    copySnapshots,
		},

		{
			Name: "info",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "encrypt, e",
					Usage: "The storage is encrypted with a password",
				},
				cli.StringFlag{
					Name:     "repository",
					Usage:    "retrieve saved passwords from the specified repository",
					Argument: "<repository directory>",
				},
				cli.StringFlag{
					Name:     "storage-name",
					Usage:    "the storage name to be assigned to the storage url",
					Argument: "<name>",
				},
				cli.BoolFlag{
					Name:  "reset-passwords",
					Usage: "take passwords from input rather than keychain/keyring",
				},
			},
			Usage:     "Show the information about the specified storage",
			ArgsUsage: "<storage url>",
			Action:    infoStorage,
		},

		{
			Name: "benchmark",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:     "file-size",
					Usage:    "the size of the local file to write to and read from (in MB, default to 256)",
					Argument: "<size>",
				},
				cli.IntFlag{
					Name:     "chunk-count",
					Usage:    "the number of chunks to upload and download (default to 64)",
					Argument: "<count>",
				},
				cli.IntFlag{
					Name:     "chunk-size",
					Usage:    "the size of chunks to upload and download (in MB, default to 4)",
					Argument: "<size>",
				},
				cli.IntFlag{
					Name:     "upload-threads",
					Usage:    "the number of upload threads (default to 1)",
					Argument: "<n>",
				},
				cli.IntFlag{
					Name:     "download-threads",
					Usage:    "the number of download threads (default to 1)",
					Argument: "<n>",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "run the download/upload test agaist the specified storage",
					Argument: "<storage name>",
				},
			},
			Usage:     "Run a set of benchmarks to test download and upload speeds",
			ArgsUsage: " ",
			Action:    benchmark,
		},

		{
			Name: "mount",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "flat",
					Usage: "use a flat directory structure instead of organized by date",
				},
				cli.BoolFlag{
					Name:  "disk-cache",
					Usage: "use the disk to cache revision file list. try this if you notice excessive memory usage. need to have sqinn binary on PATH",
				},
				cli.StringFlag{
					Name:     "revisions",
					Usage:    "revisions or range of revisions to mount separated by comma. ie. 1,4-7,9",
					Argument: "<revisions>",
				},
				cli.StringFlag{
					Name:     "storage",
					Usage:    "use the specified storage instead of the default one",
					Argument: "<storage name>",
				},
				cli.IntFlag{
					Name:     "threads",
					Value:    1,
					Usage:    "number of downloading threads",
					Argument: "<n>",
				},
				cli.IntFlag{
					Name:     "limit-rate",
					Value:    0,
					Usage:    "the maximum download rate (in kilobytes/sec)",
					Argument: "<kB/s>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks",
					Argument: "<private key>",
				},
				cli.StringFlag{
					Name:     "key-passphrase",
					Usage:    "the passphrase to decrypt the RSA private key",
					Argument: "<private key passphrase>",
				},
			},
			Usage:     "Mount the backup in the filesystem",
			ArgsUsage: "<mount path>",
			Action:    mountSnapshot,
		},

		{
			Name: "mount-storage",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "flat",
					Usage: "use a flat directory structure instead of organized by date",
				},
				cli.BoolFlag{
					Name:  "disk-cache",
					Usage: "use the disk to cache file list and chunks. use this if you notice excessive memory usage. need to have sqinn binary on PATH",
				},
				cli.StringFlag{
					Name:     "snapshots",
					Usage:    "specific snapshots to mount, separated by comma",
					Argument: "<snapshots>",
				},
				cli.StringFlag{
					Name:     "revisions",
					Usage:    "revisions or range of revisions to mount separated by comma. ie. 1,4-7,9. only used if you specify a single snapshot with -snapshots.",
					Argument: "<revisions>",
				},
				cli.BoolFlag{
					Name:  "encrypt, e",
					Usage: "The storage is encrypted with a password",
				},
				cli.StringFlag{
					Name:     "repository",
					Usage:    "retrieve saved passwords from the specified repository. if you get errors about 'preference path not set', use this option",
					Argument: "<repository directory>",
				},
				cli.IntFlag{
					Name:     "threads",
					Value:    1,
					Usage:    "number of downloading threads",
					Argument: "<n>",
				},
				cli.IntFlag{
					Name:     "limit-rate",
					Value:    0,
					Usage:    "the maximum download rate (in kilobytes/sec)",
					Argument: "<kB/s>",
				},
				cli.StringFlag{
					Name:     "key",
					Usage:    "the RSA private key to decrypt file chunks",
					Argument: "<private key>",
				},
				cli.StringFlag{
					Name:     "key-passphrase",
					Usage:    "the passphrase to decrypt the RSA private key",
					Argument: "<private key passphrase>",
				},
			},
			Usage:     "Mount the storage in the filesystem",
			ArgsUsage: "<storage url> <mount path>",
			Action:    mountStorage,
		},
	}

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "verbose, v",
			Usage: "show more detailed information",
		},
		cli.BoolFlag{
			Name:  "debug, d",
			Usage: "show even more detailed information, useful for debugging",
		},
		cli.BoolFlag{
			Name:  "log",
			Usage: "enable log-style output",
		},
		cli.BoolFlag{
			Name:  "stack",
			Usage: "print the stack trace when an error occurs",
		},
		cli.BoolFlag{
			Name:  "no-script",
			Usage: "do not run script before or after command execution",
		},
		cli.BoolFlag{
			Name:  "background",
			Usage: "read passwords, tokens, or keys only from keychain/keyring or env",
		},
		cli.StringFlag{
			Name:     "profile",
			Value:    "",
			Usage:    "enable the profiling tool and listen on the specified address:port",
			Argument: "<address:port>",
		},
		cli.StringFlag{
			Name:  "comment",
			Usage: "add a comment to identify the process",
		},
		cli.StringSliceFlag{
			Name:     "suppress, s",
			Usage:    "suppress logs with the specified id",
			Argument: "<id>",
		},
		cli.BoolFlag{
			Name:  "print-memory-usage",
			Usage: "print memory usage every second",
		},
	}

	app.HideVersion = true
	app.Name = "duplicacy"
	app.HelpName = "duplicacy"
	app.Usage = "A new generation cloud backup tool based on lock-free deduplication"
	app.Version = "3.1.0" + " (" + GitCommit + ")"

	// Exit with code 2 if an invalid command is provided
	app.CommandNotFound = func(context *cli.Context, command string) {
		fmt.Fprintf(context.App.Writer, "Invalid command: %s\n", command)
		os.Exit(2)
	}

	// If the program is interrupted, call the RunAtError function.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			duplicacy.RunAtError()
			os.Exit(1)
		}
	}()

	err := app.Run(os.Args)
	if err != nil {
		os.Exit(2)
	}

}
