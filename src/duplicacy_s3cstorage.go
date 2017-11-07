// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"time"

	"github.com/gilbertchen/goamz/aws"
	"github.com/gilbertchen/goamz/s3"
)

// S3CStorage is a storage backend for s3 compatible storages that require V2 Signing.
type S3CStorage struct {
	StorageBase

	buckets    []*s3.Bucket
	storageDir string
}

// CreateS3CStorage creates a amazon s3 storage object.
func CreateS3CStorage(regionName string, endpoint string, bucketName string, storageDir string,
	accessKey string, secretKey string, threads int) (storage *S3CStorage, err error) {

	var region aws.Region

	if endpoint == "" {
		if regionName == "" {
			regionName = "us-east-1"
		}
		region = aws.Regions[regionName]
	} else {
		region = aws.Region{Name: regionName, S3Endpoint: "https://" + endpoint}
	}

	auth := aws.Auth{AccessKey: accessKey, SecretKey: secretKey}

	var buckets []*s3.Bucket
	for i := 0; i < threads; i++ {
		s3Client := s3.New(auth, region)
		s3Client.AttemptStrategy = aws.AttemptStrategy{
			Min:   8,
			Total: 300 * time.Second,
			Delay: 1000 * time.Millisecond,
		}

		bucket := s3Client.Bucket(bucketName)
		buckets = append(buckets, bucket)
	}

	if len(storageDir) > 0 && storageDir[len(storageDir)-1] != '/' {
		storageDir += "/"
	}

	storage = &S3CStorage{
		buckets:    buckets,
		storageDir: storageDir,
	}

	storage.DerivedStorage = storage
	storage.SetDefaultNestingLevels([]int{0}, 0)
	return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *S3CStorage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {
	if len(dir) > 0 && dir[len(dir)-1] != '/' {
		dir += "/"
	}

	dirLength := len(storage.storageDir + dir)
	if dir == "snapshots/" {
		results, err := storage.buckets[threadIndex].List(storage.storageDir+dir, "/", "", 100)
		if err != nil {
			return nil, nil, err
		}

		for _, subDir := range results.CommonPrefixes {
			files = append(files, subDir[dirLength:])
		}
		return files, nil, nil
	} else if dir == "chunks/" {
		marker := ""
		for {
			results, err := storage.buckets[threadIndex].List(storage.storageDir+dir, "", marker, 1000)
			if err != nil {
				return nil, nil, err
			}

			for _, object := range results.Contents {
				files = append(files, object.Key[dirLength:])
				sizes = append(sizes, object.Size)
			}

			if !results.IsTruncated {
				break
			}

			marker = results.Contents[len(results.Contents)-1].Key
		}
		return files, sizes, nil

	} else {

		results, err := storage.buckets[threadIndex].List(storage.storageDir+dir, "", "", 1000)
		if err != nil {
			return nil, nil, err
		}

		for _, object := range results.Contents {
			files = append(files, object.Key[dirLength:])
		}
		return files, nil, nil
	}
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *S3CStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	return storage.buckets[threadIndex].Del(storage.storageDir + filePath)
}

// MoveFile renames the file.
func (storage *S3CStorage) MoveFile(threadIndex int, from string, to string) (err error) {

	options := s3.CopyOptions{ContentType: "application/duplicacy"}
	_, err = storage.buckets[threadIndex].PutCopy(storage.storageDir+to, s3.Private, options, storage.buckets[threadIndex].Name+"/"+storage.storageDir+from)
	if err != nil {
		return nil
	}

	return storage.DeleteFile(threadIndex, from)
}

// CreateDirectory creates a new directory.
func (storage *S3CStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *S3CStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

	response, err := storage.buckets[threadIndex].Head(storage.storageDir+filePath, nil)
	if err != nil {
		if e, ok := err.(*s3.Error); ok && (e.StatusCode == 403 || e.StatusCode == 404) {
			return false, false, 0, nil
		} else {
			return false, false, 0, err
		}
	}

	if response.StatusCode == 403 || response.StatusCode == 404 {
		return false, false, 0, nil
	} else {
		return true, false, response.ContentLength, nil
	}
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *S3CStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {

	readCloser, err := storage.buckets[threadIndex].GetReader(storage.storageDir + filePath)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/len(storage.buckets))
	return err

}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *S3CStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

	options := s3.Options{}
	reader := CreateRateLimitedReader(content, storage.UploadRateLimit/len(storage.buckets))
	return storage.buckets[threadIndex].PutReader(storage.storageDir+filePath, reader, int64(len(content)), "application/duplicacy", s3.Private, options)
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *S3CStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *S3CStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *S3CStorage) IsStrongConsistent() bool { return false }

// If the storage supports fast listing of files names.
func (storage *S3CStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *S3CStorage) EnableTestMode() {}
