//
// Storage module for Wasabi (https://www.wasabi.com)
//

// Wasabi is nominally compatible with AWS S3, but the copy-and-delete
// method used for renaming objects creates additional expense under
// Wasabi's billing system.  This module is a pass-through to the
// existing S3 module for everything other than that one operation.
//
// This module copyright 2017 Mark Feit (https://github.com/markfeit)
// and may be distributed under the same terms as Duplicacy.

package duplicacy

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type WasabiStorage struct {
	StorageBase

	s3         *S3Storage
	region     string
	endpoint   string
	bucket     string
	storageDir string
	key        string
	secret     string
	client     *http.Client
}

// See the Storage interface in duplicacy_storage.go for function
// descriptions.

func CreateWasabiStorage(
	regionName string, endpoint string,
	bucketName string, storageDir string,
	accessKey string, secretKey string,
	threads int,
) (storage *WasabiStorage, err error) {

	s3storage, error := CreateS3Storage(regionName, endpoint, bucketName,
		storageDir, accessKey, secretKey, threads,
		true,  // isSSLSupported
		false, // isMinioCompatible
	)

	if err != nil {
		return nil, error
	}

	wasabi := &WasabiStorage{

		// Pass-through to existing S3 module
		s3: s3storage,

		// Local copies required for renaming
		region:     regionName,
		endpoint:   endpoint,
		bucket:     bucketName,
		storageDir: storageDir,
		key:        accessKey,
		secret:     secretKey,
		client:     &http.Client{},
	}

	wasabi.DerivedStorage = wasabi
	wasabi.SetDefaultNestingLevels([]int{0}, 0)

	return wasabi, nil
}

func (storage *WasabiStorage) ListFiles(
	threadIndex int, dir string,
) (files []string, sizes []int64, err error) {
	return storage.s3.ListFiles(threadIndex, dir)
}

func (storage *WasabiStorage) DeleteFile(
	threadIndex int, filePath string,
) (err error) {
	return storage.s3.DeleteFile(threadIndex, filePath)

}

// This is a lightweight implementation of a call to Wasabi for a
// rename.  It's designed to get the job done with as few dependencies
// on other packages as possible rather than being somethng
// general-purpose and reusable.
func (storage *WasabiStorage) MoveFile(
	threadIndex int, from string, to string,
) (err error) {

	// The from path includes the bucket
	from_path := fmt.Sprintf("/%s/%s/%s", storage.bucket, storage.storageDir, from)

	object := fmt.Sprintf("https://%s@%s%s",
		storage.region, storage.endpoint, from_path)

	// The object's new name is relative to the top of the bucket.
	new_name := fmt.Sprintf("%s/%s", storage.storageDir, to)

	timestamp := time.Now().Format(time.RFC1123Z)

	signing_string := fmt.Sprintf("MOVE\n\n\n%s\n%s", timestamp, from_path)

	signer := hmac.New(sha1.New, []byte(storage.secret))
	signer.Write([]byte(signing_string))

	signature := base64.StdEncoding.EncodeToString(signer.Sum(nil))

	authorization := fmt.Sprintf("AWS %s:%s", storage.key, signature)

	request, error := http.NewRequest("MOVE", object, nil)
	if error != nil {
		return error
	}
	request.Header.Add("Authorization", authorization)
	request.Header.Add("Date", timestamp)
	request.Header.Add("Destination", new_name)
	request.Header.Add("Host", storage.endpoint)
	request.Header.Add("Overwrite", "true")

	response, error := storage.client.Do(request)
	if error != nil {
		return error
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return errors.New(response.Status)
	}

	return nil
}

func (storage *WasabiStorage) CreateDirectory(
	threadIndex int, dir string,
) (err error) {
	return storage.s3.CreateDirectory(threadIndex, dir)
}

func (storage *WasabiStorage) GetFileInfo(
	threadIndex int, filePath string,
) (exist bool, isDir bool, size int64, err error) {
	return storage.s3.GetFileInfo(threadIndex, filePath)
}

func (storage *WasabiStorage) DownloadFile(
	threadIndex int, filePath string, chunk *Chunk,
) (err error) {
	return storage.s3.DownloadFile(threadIndex, filePath, chunk)
}

func (storage *WasabiStorage) UploadFile(
	threadIndex int, filePath string, content []byte,
) (err error) {
	return storage.s3.UploadFile(threadIndex, filePath, content)
}

func (storage *WasabiStorage) IsCacheNeeded() bool {
	return storage.s3.IsCacheNeeded()
}

func (storage *WasabiStorage) IsMoveFileImplemented() bool {
	// This is implemented locally since S3 does a copy and delete
	return true
}

func (storage *WasabiStorage) IsStrongConsistent() bool {
	// Wasabi has it, S3 doesn't.
	return true
}

func (storage *WasabiStorage) IsFastListing() bool {
	return storage.s3.IsFastListing()
}

func (storage *WasabiStorage) EnableTestMode() {
}
