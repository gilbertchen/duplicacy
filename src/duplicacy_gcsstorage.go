// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/url"
	"time"

	gcs "cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type GCSStorage struct {
	RateLimitedStorage

	bucket     *gcs.BucketHandle
	storageDir string

	numberOfThreads int
	TestMode        bool
}

type GCSConfig struct {
	ClientID     string          `json:"client_id"`
	ClientSecret string          `json:"client_secret"`
	Endpoint     oauth2.Endpoint `json:"end_point"`
	Token        oauth2.Token    `json:"token"`
}

// CreateGCSStorage creates a GCD storage object.
func CreateGCSStorage(tokenFile string, bucketName string, storageDir string, threads int) (storage *GCSStorage, err error) {

	ctx := context.Background()

	description, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	var object map[string]interface{}

	err = json.Unmarshal(description, &object)
	if err != nil {
		return nil, err
	}

	isServiceAccount := false
	if value, ok := object["type"]; ok {
		if authType, ok := value.(string); ok && authType == "service_account" {
			isServiceAccount = true
		}
	}

	var tokenSource oauth2.TokenSource

	if isServiceAccount {
		config, err := google.JWTConfigFromJSON(description, gcs.ScopeReadWrite)
		if err != nil {
			return nil, err
		}
		tokenSource = config.TokenSource(ctx)
	} else {
		gcsConfig := &GCSConfig{}
		if err := json.Unmarshal(description, gcsConfig); err != nil {
			return nil, err
		}

		config := oauth2.Config{
			ClientID:     gcsConfig.ClientID,
			ClientSecret: gcsConfig.ClientSecret,
			Endpoint:     gcsConfig.Endpoint,
		}
		tokenSource = config.TokenSource(ctx, &gcsConfig.Token)
	}

	options := option.WithTokenSource(tokenSource)
	client, err := gcs.NewClient(ctx, options)

	bucket := client.Bucket(bucketName)

	if len(storageDir) > 0 && storageDir[len(storageDir)-1] != '/' {
		storageDir += "/"
	}

	storage = &GCSStorage{
		bucket:          bucket,
		storageDir:      storageDir,
		numberOfThreads: threads,
	}

	return storage, nil

}

func (storage *GCSStorage) shouldRetry(backoff *int, err error) (bool, error) {

	retry := false
	message := ""
	if err == nil {
		return false, nil
	} else if e, ok := err.(*googleapi.Error); ok {
		if 500 <= e.Code && e.Code < 600 {
			// Retry for 5xx response codes.
			message = fmt.Sprintf("HTTP status code %d", e.Code)
			retry = true
		} else if e.Code == 429 {
			// Too many requests{
			message = "HTTP status code 429"
			retry = true
		} else if e.Code == 403 {
			// User Rate Limit Exceeded
			message = "User Rate Limit Exceeded"
			retry = true
		}
	} else if e, ok := err.(*url.Error); ok {
		message = e.Error()
		retry = true
	} else if err == io.ErrUnexpectedEOF {
		// Retry on unexpected EOFs and temporary network errors.
		message = "Unexpected EOF"
		retry = true
	} else if err, ok := err.(net.Error); ok {
		message = "Temporary network error"
		retry = err.Temporary()
	}

	if !retry || *backoff >= 256 {
		return false, err
	}

	delay := float32(*backoff) * rand.Float32()
	LOG_INFO("GCS_RETRY", "%s; retrying after %.2f seconds", message, delay)
	time.Sleep(time.Duration(float32(*backoff) * float32(time.Second)))
	*backoff *= 2
	return true, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *GCSStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
	for len(dir) > 0 && dir[len(dir)-1] == '/' {
		dir = dir[:len(dir)-1]
	}

	query := gcs.Query{
		Prefix: storage.storageDir + dir + "/",
	}
	dirOnly := false
	prefixLength := len(query.Prefix)

	if dir == "snapshots" {
		query.Delimiter = "/"
		dirOnly = true
	}

	files := []string{}
	sizes := []int64{}
	iter := storage.bucket.Objects(context.Background(), &query)
	for {
		attributes, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if dirOnly {
			if len(attributes.Prefix) != 0 {
				prefix := attributes.Prefix
				files = append(files, prefix[prefixLength:])
			}
		} else {
			if len(attributes.Prefix) == 0 {
				files = append(files, attributes.Name[prefixLength:])
				sizes = append(sizes, attributes.Size)
			}
		}
	}

	return files, sizes, nil
}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *GCSStorage) DeleteFile(threadIndex int, filePath string) (err error) {
	err = storage.bucket.Object(storage.storageDir + filePath).Delete(context.Background())
	if err == gcs.ErrObjectNotExist {
		return nil
	}
	return err
}

// MoveFile renames the file.
func (storage *GCSStorage) MoveFile(threadIndex int, from string, to string) (err error) {

	source := storage.bucket.Object(storage.storageDir + from)
	destination := storage.bucket.Object(storage.storageDir + to)

	_, err = destination.CopierFrom(source).Run(context.Background())
	if err != nil {
		return err
	}

	return storage.DeleteFile(threadIndex, from)
}

// CreateDirectory creates a new directory.
func (storage *GCSStorage) CreateDirectory(threadIndex int, dir string) (err error) {
	return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *GCSStorage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {
	object := storage.bucket.Object(storage.storageDir + filePath)

	attributes, err := object.Attrs(context.Background())

	if err != nil {
		if err == gcs.ErrObjectNotExist {
			return false, false, 0, nil
		} else {
			return false, false, 0, err
		}
	}

	return true, false, attributes.Size, nil
}

// FindChunk finds the chunk with the specified id.  If 'isFossil' is true, it will search for chunk files with
// the suffix '.fsl'.
func (storage *GCSStorage) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {
	filePath = "chunks/" + chunkID
	if isFossil {
		filePath += ".fsl"
	}

	exist, _, size, err = storage.GetFileInfo(threadIndex, filePath)

	return filePath, exist, size, err
}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *GCSStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {
	readCloser, err := storage.bucket.Object(storage.storageDir + filePath).NewReader(context.Background())
	if err != nil {
		return err
	}
	defer readCloser.Close()
	_, err = RateLimitedCopy(chunk, readCloser, storage.DownloadRateLimit/storage.numberOfThreads)
	return err
}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *GCSStorage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

	backoff := 1
	for {
		writeCloser := storage.bucket.Object(storage.storageDir + filePath).NewWriter(context.Background())
		defer writeCloser.Close()
		reader := CreateRateLimitedReader(content, storage.UploadRateLimit/storage.numberOfThreads)
		_, err = io.Copy(writeCloser, reader)

		if retry, e := storage.shouldRetry(&backoff, err); e == nil && !retry {
			break
		} else if retry {
			continue
		} else {
			return err
		}
	}

	return err
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *GCSStorage) IsCacheNeeded() bool { return true }

// If the 'MoveFile' method is implemented.
func (storage *GCSStorage) IsMoveFileImplemented() bool { return true }

// If the storage can guarantee strong consistency.
func (storage *GCSStorage) IsStrongConsistent() bool { return true }

// If the storage supports fast listing of files names.
func (storage *GCSStorage) IsFastListing() bool { return true }

// Enable the test mode.
func (storage *GCSStorage) EnableTestMode() { storage.TestMode = true }
