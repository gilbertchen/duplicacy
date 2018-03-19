// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type Storage interface {
	// ListFiles return the list of files and subdirectories under 'dir'.  A subdirectories returned must have a trailing '/', with
	// a size of 0.  If 'dir' is 'snapshots', only subdirectories will be returned.  If 'dir' is 'snapshots/repository_id', then only
	// files will be returned.  If 'dir' is 'chunks', the implementation can return the list either recusively or non-recusively.
	ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error)

	// DeleteFile deletes the file or directory at 'filePath'.
	DeleteFile(threadIndex int, filePath string) (err error)

	// MoveFile renames the file.
	MoveFile(threadIndex int, from string, to string) (err error)

	// CreateDirectory creates a new directory.
	CreateDirectory(threadIndex int, dir string) (err error)

	// GetFileInfo returns the information about the file or directory at 'filePath'.
	GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error)

	// FindChunk finds the chunk with the specified id.  If 'isFossil' is true, it will search for chunk files with
	// the suffix '.fsl'.
	FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error)

	// DownloadFile reads the file at 'filePath' into the chunk.
	DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error)

	// UploadFile writes 'content' to the file at 'filePath'.
	UploadFile(threadIndex int, filePath string, content []byte) (err error)

	// SetNestingLevels sets up the chunk nesting structure.
	SetNestingLevels(config *Config)

	// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
	// managing snapshots.
	IsCacheNeeded() bool

	// If the 'MoveFile' method is implemented.
	IsMoveFileImplemented() bool

	// If the storage can guarantee strong consistency.
	IsStrongConsistent() bool

	// If the storage supports fast listing of files names.
	IsFastListing() bool

	// Enable the test mode.
	EnableTestMode()

	// Set the maximum transfer speeds.
	SetRateLimits(downloadRateLimit int, uploadRateLimit int)
}

// StorageBase is the base struct from which all storages are derived from
type StorageBase struct {
	DownloadRateLimit int // Maximum download rate (bytes/seconds)
	UploadRateLimit   int // Maximum upload reate (bytes/seconds)

	DerivedStorage Storage // Used as the pointer to the derived storage class

	readLevels []int // At which nesting level to find the chunk with the given id
	writeLevel int   // Store the uploaded chunk to this level
}

// SetRateLimits sets the maximum download and upload rates
func (storage *StorageBase) SetRateLimits(downloadRateLimit int, uploadRateLimit int) {
	storage.DownloadRateLimit = downloadRateLimit
	storage.UploadRateLimit = uploadRateLimit
}

// SetDefaultNestingLevels sets the default read and write levels.  This is usually called by
// derived storages to set the levels with old values so that storages initialied by ealier versions
// will continue to work.
func (storage *StorageBase) SetDefaultNestingLevels(readLevels []int, writeLevel int) {
	storage.readLevels = readLevels
	storage.writeLevel = writeLevel
}

// SetNestingLevels sets the new read and write levels (normally both at 1) if the 'config' file has
// the 'fixed-nesting' key, or if a file named 'nesting' exists on the storage.
func (storage *StorageBase) SetNestingLevels(config *Config) {

	// 'FixedNesting' is true only for the 'config' file with the new format (2.0.10+)
	if config.FixedNesting {

		storage.readLevels = nil

		// Check if the 'nesting' file exist
		exist, _, _, err := storage.DerivedStorage.GetFileInfo(0, "nesting")
		if err == nil && exist {
			nestingFile := CreateChunk(CreateConfig(), true)
			if storage.DerivedStorage.DownloadFile(0, "nesting", nestingFile) == nil {
				var nesting struct {
					ReadLevels []int `json:"read-levels"`
					WriteLevel int   `json:"write-level"`
				}
				if json.Unmarshal(nestingFile.GetBytes(), &nesting) == nil {
					storage.readLevels = nesting.ReadLevels
					storage.writeLevel = nesting.WriteLevel
				}
			}
		}

		if len(storage.readLevels) == 0 {
			storage.readLevels = []int{1}
			storage.writeLevel = 1
		}
	}

	LOG_DEBUG("STORAGE_NESTING", "Chunk read levels: %v, write level: %d", storage.readLevels, storage.writeLevel)
	for _, level := range storage.readLevels {
		if storage.writeLevel == level {
			return
		}
	}
	LOG_ERROR("STORAGE_NESTING", "The write level %d isn't in the read levels %v", storage.readLevels, storage.writeLevel)
}

// FindChunk finds the chunk with the specified id at the levels one by one as specified by 'readLevels'.
func (storage *StorageBase) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {
	chunkPaths := make([]string, 0)
	for _, level := range storage.readLevels {
		chunkPath := "chunks/"
		for i := 0; i < level; i++ {
			chunkPath += chunkID[2*i:2*i+2] + "/"
		}
		chunkPath += chunkID[2*level:]
		if isFossil {
			chunkPath += ".fsl"
		}
		exist, _, size, err = storage.DerivedStorage.GetFileInfo(threadIndex, chunkPath)
		if err == nil && exist {
			return chunkPath, exist, size, err
		}
		chunkPaths = append(chunkPaths, chunkPath)
	}
	for i, level := range storage.readLevels {
		if storage.writeLevel == level {
			return chunkPaths[i], false, 0, nil
		}
	}
	return "", false, 0, fmt.Errorf("Invalid chunk nesting setup")
}

func checkHostKey(hostname string, remote net.Addr, key ssh.PublicKey) error {

	if preferencePath == "" {
		return fmt.Errorf("Can't verify SSH host since the preference path is not set")
	}
	hostFile := path.Join(preferencePath, "known_hosts")
	file, err := os.OpenFile(hostFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	lineRegex := regexp.MustCompile(`^([^\s]+)\s+(.+)`)

	keyString := string(ssh.MarshalAuthorizedKey(key))
	keyString = strings.Replace(keyString, "\n", "", -1)
	remoteAddress := remote.String()
	if strings.HasSuffix(remoteAddress, ":22") {
		remoteAddress = remoteAddress[:len(remoteAddress)-len(":22")]
	}

	for i, line := range strings.Split(string(content), "\n") {
		matched := lineRegex.FindStringSubmatch(line)
		if matched == nil {
			continue
		}

		if matched[1] == remote.String() {
			if keyString != matched[2] {
				LOG_WARN("HOSTKEY_OLD", "The existing key for '%s' is %s (file %s, line %d)",
					remote.String(), matched[2], hostFile, i)
				LOG_WARN("HOSTKEY_NEW", "The new key is '%s'", keyString)
				return fmt.Errorf("The host key for '%s' has changed", remote.String())
			} else {
				return nil
			}
		}
	}

	file.Write([]byte(remote.String() + " " + keyString + "\n"))
	return nil
}

// CreateStorage creates a storage object based on the provide storage URL.
func CreateStorage(preference Preference, resetPassword bool, threads int) (storage Storage) {

	storageURL := preference.StorageURL

	isFileStorage := false
	isCacheNeeded := false

	if strings.HasPrefix(storageURL, "/") {
		isFileStorage = true
	} else if runtime.GOOS == "windows" {
		if len(storageURL) >= 3 && storageURL[1] == ':' && (storageURL[2] == '/' || storageURL[2] == '\\') {
			volume := strings.ToLower(storageURL[:1])
			if volume[0] >= 'a' && volume[0] <= 'z' {
				isFileStorage = true
			}
		}

		if !isFileStorage && strings.HasPrefix(storageURL, `\\`) {
			isFileStorage = true
			isCacheNeeded = true
		}
	}

	if isFileStorage {
		fileStorage, err := CreateFileStorage(storageURL, isCacheNeeded, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the file storage at %s: %v", storageURL, err)
			return nil
		}
		return fileStorage
	}

	if strings.HasPrefix(storageURL, "flat://") {
		fileStorage, err := CreateFileStorage(storageURL[7:], false, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the file storage at %s: %v", storageURL, err)
			return nil
		}
		return fileStorage
	}

	if strings.HasPrefix(storageURL, "samba://") {
		fileStorage, err := CreateFileStorage(storageURL[8:], true, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the file storage at %s: %v", storageURL, err)
			return nil
		}
		return fileStorage
	}

	urlRegex := regexp.MustCompile(`^(\w+)://([\w\-]+@)?([^/]+)(/(.+))?`)

	matched := urlRegex.FindStringSubmatch(storageURL)

	if matched == nil {
		LOG_ERROR("STORAGE_CREATE", "Unrecognizable storage URL: %s", storageURL)
		return nil
	} else if matched[1] == "sftp" {
		server := matched[3]
		username := matched[2]
		storageDir := matched[5]
		port := 22

		if strings.Contains(server, ":") {
			index := strings.Index(server, ":")
			port, _ = strconv.Atoi(server[index+1:])
			server = server[:index]
		}

		if storageDir == "" {
			LOG_ERROR("STORAGE_CREATE", "The SFTP storage directory can't be empty")
			return nil
		}

		if username != "" {
			username = username[:len(username)-1]
		}

		// If ssh_key_file is set, skip password-based login
		keyFile := GetPasswordFromPreference(preference, "ssh_key_file")

		password := ""
		passwordCallback := func() (string, error) {
			LOG_DEBUG("SSH_PASSWORD", "Attempting password login")
			password = GetPassword(preference, "ssh_password", "Enter SSH password:", false, resetPassword)
			return password, nil
		}

		keyboardInteractive := func(user, instruction string, questions []string, echos []bool) (answers []string,
			err error) {
			if len(questions) == 1 {
				LOG_DEBUG("SSH_INTERACTIVE", "Attempting keyboard interactive login")
				password = GetPassword(preference, "ssh_password", "Enter SSH password:", false, resetPassword)
				answers = []string{password}
				return answers, nil
			} else {
				return nil, nil
			}
		}

		publicKeysCallback := func() ([]ssh.Signer, error) {
			LOG_DEBUG("SSH_PUBLICKEY", "Attempting public key authentication")

			signers := []ssh.Signer{}

			agentSock := os.Getenv("SSH_AUTH_SOCK")
			if agentSock != "" {
				connection, err := net.Dial("unix", agentSock)
				// TODO:  looks like we need to close the connection
				if err == nil {
					LOG_DEBUG("SSH_AGENT", "Attempting public key authentication via agent")
					sshAgent := agent.NewClient(connection)
					signers, err = sshAgent.Signers()
					if err != nil {
						LOG_DEBUG("SSH_AGENT", "Can't log in using public key authentication via agent: %v", err)
					} else if len(signers) == 0 {
						LOG_DEBUG("SSH_AGENT", "SSH agent doesn't return any signer")
					}
				}
			}

			keyFile = GetPassword(preference, "ssh_key_file", "Enter the path of the private key file:",
				true, resetPassword)

			var key ssh.Signer
			var err error

			if keyFile == "" {
				LOG_INFO("SSH_PUBLICKEY", "No private key file is provided")
			} else {
				var content []byte
				content, err = ioutil.ReadFile(keyFile)
				if err != nil {
					LOG_INFO("SSH_PUBLICKEY", "Failed to read the private key file: %v", err)
				} else {
					key, err = ssh.ParsePrivateKey(content)
					if err != nil {
						LOG_INFO("SSH_PUBLICKEY", "Failed to parse the private key file %s: %v", keyFile, err)
					}
				}
			}

			if key != nil {
				signers = append(signers, key)
			}

			if len(signers) > 0 {
				return signers, nil
			} else {
				return nil, err
			}

		}

		authMethods := []ssh.AuthMethod{}
		passwordAuthMethods := []ssh.AuthMethod{
			ssh.PasswordCallback(passwordCallback),
			ssh.KeyboardInteractive(keyboardInteractive),
		}
		keyFileAuthMethods := []ssh.AuthMethod{
			ssh.PublicKeysCallback(publicKeysCallback),
		}
		if keyFile != "" {
			authMethods = append(keyFileAuthMethods, passwordAuthMethods...)
		} else {
			authMethods = append(passwordAuthMethods, keyFileAuthMethods...)
		}

		if RunInBackground {

			passwordKey := "ssh_password"
			keyFileKey := "ssh_key_file"
			if preference.Name != "default" {
				passwordKey = preference.Name + "_" + passwordKey
				keyFileKey = preference.Name + "_" + keyFileKey
			}

			authMethods = []ssh.AuthMethod{}
			if keyringGet(passwordKey) != "" {
				authMethods = append(authMethods, ssh.PasswordCallback(passwordCallback))
				authMethods = append(authMethods, ssh.KeyboardInteractive(keyboardInteractive))
			}
			if keyringGet(keyFileKey) != "" || os.Getenv("SSH_AUTH_SOCK") != "" {
				authMethods = append(authMethods, ssh.PublicKeysCallback(publicKeysCallback))
			}
		}

		hostKeyChecker := func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return checkHostKey(hostname, remote, key)
		}

		sftpStorage, err := CreateSFTPStorage(server, port, username, storageDir, 2, authMethods, hostKeyChecker, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the SFTP storage at %s: %v", storageURL, err)
			return nil
		}

		if keyFile != "" {
			SavePassword(preference, "ssh_key_file", keyFile)
		} else if password != "" {
			SavePassword(preference, "ssh_password", password)
		}
		return sftpStorage
	} else if matched[1] == "s3" || matched[1] == "s3c" || matched[1] == "minio" || matched[1] == "minios" {

		// urlRegex := regexp.MustCompile(`^(\w+)://([\w\-]+@)?([^/]+)(/(.+))?`)

		region := matched[2]
		endpoint := matched[3]
		bucket := matched[5]

		if region != "" {
			region = region[:len(region)-1]
		}

		if strings.EqualFold(endpoint, "amazon") || strings.EqualFold(endpoint, "amazon.com") {
			endpoint = ""
		}

		storageDir := ""
		if strings.Contains(bucket, "/") {
			firstSlash := strings.Index(bucket, "/")
			storageDir = bucket[firstSlash+1:]
			bucket = bucket[:firstSlash]
		}

		accessKey := GetPassword(preference, "s3_id", "Enter S3 Access Key ID:", true, resetPassword)
		secretKey := GetPassword(preference, "s3_secret", "Enter S3 Secret Access Key:", true, resetPassword)

		var err error

		if matched[1] == "s3c" {
			storage, err = CreateS3CStorage(region, endpoint, bucket, storageDir, accessKey, secretKey, threads)
			if err != nil {
				LOG_ERROR("STORAGE_CREATE", "Failed to load the S3C storage at %s: %v", storageURL, err)
				return nil
			}
		} else {
			isMinioCompatible := (matched[1] == "minio" || matched[1] == "minios")
			isSSLSupported := (matched[1] == "s3" || matched[1] == "minios")
			storage, err = CreateS3Storage(region, endpoint, bucket, storageDir, accessKey, secretKey, threads, isSSLSupported, isMinioCompatible)
			if err != nil {
				LOG_ERROR("STORAGE_CREATE", "Failed to load the S3 storage at %s: %v", storageURL, err)
				return nil
			}
		}
		SavePassword(preference, "s3_id", accessKey)
		SavePassword(preference, "s3_secret", secretKey)

		return storage

	} else if matched[1] == "wasabi" {

		region := matched[2]
		endpoint := matched[3]
		bucket := matched[5]

		if region != "" {
			region = region[:len(region)-1]
		}

		key := GetPassword(preference, "wasabi_key",
			"Enter Wasabi key:", true, resetPassword)
		secret := GetPassword(preference, "wasabi_secret",
			"Enter Wasabi secret:", true, resetPassword)

		storageDir := ""
		if strings.Contains(bucket, "/") {
			firstSlash := strings.Index(bucket, "/")
			storageDir = bucket[firstSlash+1:]
			bucket = bucket[:firstSlash]
		}

		storage, err := CreateWasabiStorage(region, endpoint,
			bucket, storageDir, key, secret, threads)

		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the Wasabi storage at %s: %v", storageURL, err)
			return nil
		}

		SavePassword(preference, "wasabi_key", key)
		SavePassword(preference, "wasabi_secret", secret)

		return storage

	} else if matched[1] == "dropbox" {
		storageDir := matched[3] + matched[5]
		token := GetPassword(preference, "dropbox_token", "Enter Dropbox access token:", true, resetPassword)
		dropboxStorage, err := CreateDropboxStorage(token, storageDir, 1, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the dropbox storage: %v", err)
			return nil
		}
		SavePassword(preference, "dropbox_token", token)
		return dropboxStorage
	} else if matched[1] == "b2" {
		bucket := matched[3]

		accountID := GetPassword(preference, "b2_id", "Enter Backblaze Account ID:", true, resetPassword)
		applicationKey := GetPassword(preference, "b2_key", "Enter Backblaze Application Key:", true, resetPassword)

		b2Storage, err := CreateB2Storage(accountID, applicationKey, bucket, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the Backblaze B2 storage at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "b2_id", accountID)
		SavePassword(preference, "b2_key", applicationKey)
		return b2Storage
	} else if matched[1] == "azure" {
		account := matched[3]
		container := matched[5]

		if container == "" {
			LOG_ERROR("STORAGE_CREATE", "The container name for the Azure storage can't be empty")
			return nil
		}

		prompt := fmt.Sprintf("Enter the Access Key for the Azure storage account %s:", account)
		accessKey := GetPassword(preference, "azure_key", prompt, true, resetPassword)

		azureStorage, err := CreateAzureStorage(account, accessKey, container, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the Azure storage at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "azure_key", accessKey)
		return azureStorage
	} else if matched[1] == "acd" {
		storagePath := matched[3] + matched[4]
		prompt := fmt.Sprintf("Enter the path of the Amazon Cloud Drive token file (downloadable from https://duplicacy.com/acd_start):")
		tokenFile := GetPassword(preference, "acd_token", prompt, true, resetPassword)
		acdStorage, err := CreateACDStorage(tokenFile, storagePath, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the Amazon Cloud Drive storage at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "acd_token", tokenFile)
		return acdStorage
	} else if matched[1] == "gcs" {
		bucket := matched[3]
		storageDir := matched[5]
		prompt := fmt.Sprintf("Enter the path of the Google Cloud Storage token file (downloadable from https://duplicacy.com/gcs_start) or the service account credential file:")
		tokenFile := GetPassword(preference, "gcs_token", prompt, true, resetPassword)
		gcsStorage, err := CreateGCSStorage(tokenFile, bucket, storageDir, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the Google Cloud Storage backend at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "gcs_token", tokenFile)
		return gcsStorage
	} else if matched[1] == "gcd" {
		storagePath := matched[3] + matched[4]
		prompt := fmt.Sprintf("Enter the path of the Google Drive token file (downloadable from https://duplicacy.com/gcd_start):")
		tokenFile := GetPassword(preference, "gcd_token", prompt, true, resetPassword)
		gcdStorage, err := CreateGCDStorage(tokenFile, storagePath, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the Google Drive storage at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "gcd_token", tokenFile)
		return gcdStorage
	} else if matched[1] == "one" {
		storagePath := matched[3] + matched[4]
		prompt := fmt.Sprintf("Enter the path of the OneDrive token file (downloadable from https://duplicacy.com/one_start):")
		tokenFile := GetPassword(preference, "one_token", prompt, true, resetPassword)
		oneDriveStorage, err := CreateOneDriveStorage(tokenFile, storagePath, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the OneDrive storage at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "one_token", tokenFile)
		return oneDriveStorage
	} else if matched[1] == "hubic" {
		storagePath := matched[3] + matched[4]
		prompt := fmt.Sprintf("Enter the path of the Hubic token file (downloadable from https://duplicacy.com/hubic_start):")
		tokenFile := GetPassword(preference, "hubic_token", prompt, true, resetPassword)
		hubicStorage, err := CreateHubicStorage(tokenFile, storagePath, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the Hubic storage at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "hubic_token", tokenFile)
		return hubicStorage
	} else if matched[1] == "swift" {
		prompt := fmt.Sprintf("Enter the OpenStack Swift key:")
		key := GetPassword(preference, "swift_key", prompt, true, resetPassword)
		swiftStorage, err := CreateSwiftStorage(storageURL[8:], key, threads)
		if err != nil {
			LOG_ERROR("STORAGE_CREATE", "Failed to load the OpenStack Swift storage at %s: %v", storageURL, err)
			return nil
		}
		SavePassword(preference, "swift_key", key)
		return swiftStorage
	} else {
		LOG_ERROR("STORAGE_CREATE", "The storage type '%s' is not supported", matched[1])
		return nil
	}

}
