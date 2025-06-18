// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package backend

import (
	"fmt"
	"strconv"
	"sync"
)

type StorageMaker func(conf map[string]string, threads int) (storage Storage, err error)

var storages = make(map[string]StorageMaker)
var storageLock sync.Mutex

func StorageRegist(name string, maker StorageMaker) {
	storageLock.Lock()
	defer storageLock.Unlock()

	if _, ok := storages[name]; ok {
		panic(fmt.Errorf("storage name: %v, exist", name))
	}
	storages[name] = maker
}

func StorageGet(name string, conf map[string]string, threads int) (storage Storage, err error) {
	storageLock.Lock()
	defer storageLock.Unlock()

	if maker, ok := storages[name]; ok {
		return maker(conf, threads)
	}
	return nil, fmt.Errorf("Invalid storage named: %s", name)
}

func init() {
	localMaker := func(config map[string]string, threads int) (Storage, error) {
		localStoragePath := config["localStoragePath"]
		storage, err := CreateFileStorage(localStoragePath, false, threads)
		if storage != nil {
			// Use a read level of at least 2 because this will catch more errors than a read level of 1.
			storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		}
		return storage, err
	}
	StorageRegist("", localMaker)
	StorageRegist("flat", localMaker)
	StorageRegist("file", localMaker)
	StorageRegist("samba", localMaker)

	StorageRegist("sftp", func(config map[string]string, threads int) (Storage, error) {
		port, _ := strconv.Atoi(config["port"])
		storage, err := CreateSFTPStorageWithPassword(config["server"], port, config["username"], config["directory"], 2, config["password"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("s3", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, false)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("wasabi", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateWasabiStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("s3c", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateS3CStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("digitalocean", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateS3CStorage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("minio", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, false, true)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("minios", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, true)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("dropbox", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateDropboxStorage(config["token"], config["directory"], 1, threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("b2", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateB2Storage(config["account"], config["key"], "", config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("gcs-s3", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateS3Storage(config["region"], config["endpoint"], config["bucket"], config["directory"], config["access_key"], config["secret_key"], threads, true, false)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("gcs", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateGCSStorage(config["token_file"], config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("gcs-sa", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateGCSStorage(config["token_file"], config["bucket"], config["directory"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("azure", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateAzureStorage(config["account"], config["key"], config["container"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("acd", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateACDStorage(config["token_file"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("gcd", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateGCDStorage(config["token_file"], "", config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("gcd-shared", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateGCDStorage(config["token_file"], config["drive"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("gcd-impersonate", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateGCDStorage(config["token_file"], config["drive"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("one", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateOneDriveStorage(config["token_file"], false, config["storage_path"], threads, "", "", "")
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("odb", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateOneDriveStorage(config["token_file"], true, config["storage_path"], threads, "", "", "")
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("hubic", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateHubicStorage(config["token_file"], config["storage_path"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})
	StorageRegist("memset", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateSwiftStorage(config["storage_url"], config["key"], threads)
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	webdavMaker := func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateWebDAVStorage(config["host"], 0, config["username"], config["password"], config["storage_path"], false, threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	}
	StorageRegist("pcloud", webdavMaker)
	StorageRegist("box", webdavMaker)

	StorageRegist("fabric", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateFileFabricStorage(config["endpoint"], config["token"], config["storage_path"], threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("storj", func(config map[string]string, threads int) (Storage, error) {
		storage, err := CreateStorjStorage(config["satellite"], config["key"], config["passphrase"], config["bucket"], config["storage_path"], threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

	StorageRegist("smb", func(config map[string]string, threads int) (Storage, error) {
		port, _ := strconv.Atoi(config["port"])
		storage, err := CreateSambaStorage(config["server"], port, config["username"], config["password"], config["share"], config["storage_path"], threads)
		if err != nil {
			return nil, err
		}
		storage.SetDefaultNestingLevels([]int{2, 3}, 2)
		return storage, err
	})

}
