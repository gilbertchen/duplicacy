// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"io"
	"os"
	"fmt"
	"bytes"
	"time"
	"sync"
	"strconv"
	"strings"
	"net/url"
	"net/http"
	"math/rand"
	"io/ioutil"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/base64"
)

type B2Error struct {
	Status  int
	Code    string
	Message string
}

func (err *B2Error) Error() string {
	return fmt.Sprintf("%d %s", err.Status, err.Message)
}

type B2UploadArgument struct {
	URL   string
	Token string
}

var B2AuthorizationURL = "https://api.backblazeb2.com/b2api/v1/b2_authorize_account"

type B2Client struct {
	HTTPClient         *http.Client

	AccountID          string
	ApplicationKeyID   string
	ApplicationKey     string
	BucketName         string
	BucketID           string
	StorageDir         string

	Lock               sync.Mutex
	AuthorizationToken string
	APIURL             string
	DownloadURL        string
	IsAuthorized       bool

	UploadURLs         []string
	UploadTokens       []string

	Threads            int
	MaximumRetries     int
	TestMode           bool

	LastAuthorizationTime int64
}

// URL encode the given path but keep the slashes intact
func B2Escape(path string) string {
	var components []string
	for _, c := range strings.Split(path, "/") {
		components = append(components, url.QueryEscape(c))
	}
	return strings.Join(components, "/")
}

func NewB2Client(applicationKeyID string, applicationKey string, storageDir string, threads int) *B2Client {

	for storageDir != "" && storageDir[0] == '/' {
		storageDir = storageDir[1:]
	}

	if storageDir != "" && storageDir[len(storageDir) - 1] != '/' {
		storageDir += "/"
	}

	maximumRetries := 10
	if value, found := os.LookupEnv("DUPLICACY_B2_RETRIES"); found && value != "" {
		maximumRetries, _ = strconv.Atoi(value)
		LOG_INFO("B2_RETRIES", "Setting maximum retries for B2 to %d", maximumRetries)
	}

	client := &B2Client{
		HTTPClient:       http.DefaultClient,
		ApplicationKeyID: applicationKeyID,
		ApplicationKey:   applicationKey,
		StorageDir:       storageDir,
		UploadURLs:       make([]string, threads),
		UploadTokens:     make([]string, threads),
		Threads:          threads,
		MaximumRetries:   maximumRetries,
	}
	return client
}

func (client *B2Client) getAPIURL() string {
	client.Lock.Lock()
	defer client.Lock.Unlock()
	return client.APIURL
}

func (client *B2Client) getDownloadURL() string {
	client.Lock.Lock()
	defer client.Lock.Unlock()
	return client.DownloadURL
}

func (client *B2Client) retry(retries int, response *http.Response) int {
	if response != nil {
		if backoffList, found := response.Header["Retry-After"]; found && len(backoffList) > 0 {
			retryAfter, _ := strconv.Atoi(backoffList[0])
			if retryAfter >= 1 {
				time.Sleep(time.Duration(retryAfter) * time.Second)
				return 1
			}
		}
	}

	if retries >= client.MaximumRetries + 1 {
		return 0
	}
	retries++
	delay := 1 << uint(retries)
	if delay > 64 {
		delay = 64
	}
	delayInSeconds := (rand.Float32() + 1.0) * float32(delay) / 2.0

	time.Sleep(time.Duration(delayInSeconds) * time.Second)
	return retries
}

func (client *B2Client) call(threadIndex int, requestURL string, method string, requestHeaders map[string]string, input interface{}) (
	                         io.ReadCloser, http.Header, int64, error) {

	var response *http.Response

	retries := 0
	for {
		var inputReader io.Reader
		isUpload := false

		switch input.(type) {
		default:
			jsonInput, err := json.Marshal(input)
			if err != nil {
				return nil, nil, 0, err
			}
			inputReader = bytes.NewReader(jsonInput)
		case int:
			inputReader = bytes.NewReader([]byte(""))
		case []byte:
			isUpload = true
			inputReader = bytes.NewReader(input.([]byte))
		case *RateLimitedReader:
			isUpload = true
			rateLimitedReader := input.(*RateLimitedReader)
			rateLimitedReader.Reset()
			inputReader = rateLimitedReader
		}


		if isUpload {
			if client.UploadURLs[threadIndex] == "" || client.UploadTokens[threadIndex] == "" {
				err := client.getUploadURL(threadIndex)
				if err != nil {
					return nil, nil, 0, err
				}
			}
			requestURL = client.UploadURLs[threadIndex]
		}

		request, err := http.NewRequest(method, requestURL, inputReader)
		if err != nil {
			return nil, nil, 0, err
		}

		if requestURL == B2AuthorizationURL {
			request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(client.ApplicationKeyID+":"+client.ApplicationKey)))
		} else if isUpload {
			request.ContentLength, _ = strconv.ParseInt(requestHeaders["Content-Length"], 10, 64)
			request.Header.Set("Authorization", client.UploadTokens[threadIndex])
		} else {
			client.Lock.Lock()
			request.Header.Set("Authorization", client.AuthorizationToken)
			client.Lock.Unlock()
		}

		if requestHeaders != nil {
			for key, value := range requestHeaders {
				request.Header.Set(key, value)
			}
		}

		if client.TestMode {
			r := rand.Float32()
			if r < 0.5 && isUpload {
				request.Header.Set("X-Bz-Test-Mode", "fail_some_uploads")
			} else if r < 0.75 {
				request.Header.Set("X-Bz-Test-Mode", "expire_some_account_authorization_tokens")
			} else {
				request.Header.Set("X-Bz-Test-Mode", "force_cap_exceeded")
			}
		}

		response, err = client.HTTPClient.Do(request)
		if err != nil {

			// Don't retry when the first authorization request fails
			if requestURL == B2AuthorizationURL && !client.IsAuthorized {
				return nil, nil, 0, err
			}

			LOG_TRACE("BACKBLAZE_CALL", "[%d] URL request '%s' returned an error: %v", threadIndex, requestURL, err)

			retries = client.retry(retries, response)
			if retries <= 0 {
				return nil, nil, 0, err
			}

			// Clear the upload url to requrest a new one on retry
			if isUpload {
				client.UploadURLs[threadIndex] = ""
				client.UploadTokens[threadIndex] = ""
			}
			continue

		}

		if response.StatusCode < 300 {
			return response.Body, response.Header, response.ContentLength, nil
		}

		e := &B2Error{}
		if err := json.NewDecoder(response.Body).Decode(e); err != nil {
			LOG_TRACE("BACKBLAZE_CALL", "[%d] URL request '%s %s' returned status code %d", threadIndex, method, requestURL, response.StatusCode)
		} else {
			LOG_TRACE("BACKBLAZE_CALL", "[%d] URL request '%s %s' returned %d %s", threadIndex, method, requestURL, response.StatusCode, e.Message)
		}

		response.Body.Close()

		if response.StatusCode == 401 {
			if requestURL == B2AuthorizationURL {
				return nil, nil, 0, fmt.Errorf("Authorization failure")
			}

			// Attempt authorization again.  If authorization is actually not done, run the random backoff
			_, allowed := client.AuthorizeAccount(threadIndex)
			if allowed {
				continue
			}
		} else if response.StatusCode == 403 {
			if !client.TestMode {
				return nil, nil, 0, fmt.Errorf("B2 cap exceeded")
			}
			continue
		} else if response.StatusCode == 404 {
			if http.MethodHead == method {
				return nil, nil, 0, nil
			}
		} else if response.StatusCode == 416 {
			if http.MethodHead == method {
				// 416 Requested Range Not Satisfiable
				return nil, nil, 0, fmt.Errorf("URL request '%s' returned %d %s", requestURL, response.StatusCode, e.Message)
			}
		}

		retries = client.retry(retries, response)
		if retries <= 0 {
			return nil, nil, 0, fmt.Errorf("URL request '%s' returned %d %s", requestURL, response.StatusCode, e.Message)
		}

		if isUpload {
			client.UploadURLs[threadIndex] = ""
			client.UploadTokens[threadIndex] = ""
		}
	}

}

type B2AuthorizeAccountOutput struct {
	AccountID          string
	AuthorizationToken string
	APIURL             string
	DownloadURL        string
}

func (client *B2Client) AuthorizeAccount(threadIndex int) (err error, allowed bool) {
	client.Lock.Lock()
	defer client.Lock.Unlock()

	// Don't authorize if the previous one was done less than 30 seconds ago
	if client.LastAuthorizationTime != 0 && client.LastAuthorizationTime > time.Now().Unix() - 30 {
		return nil, false
	}

	readCloser, _, _, err := client.call(threadIndex, B2AuthorizationURL, http.MethodPost, nil, make(map[string]string))
	if err != nil {
		return err, true
	}

	defer readCloser.Close()

	output := &B2AuthorizeAccountOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return err, true
	}

	// The account id may be different from the application key id so we're getting the account id from the returned
	// json object here, which is needed by the b2_list_buckets call.
	client.AccountID = output.AccountID

	client.AuthorizationToken = output.AuthorizationToken
	client.APIURL = output.APIURL
	client.DownloadURL = output.DownloadURL
	client.IsAuthorized = true

	client.LastAuthorizationTime = time.Now().Unix()

	return nil, true
}

type ListBucketOutput struct {
	AccountID  string
	BucketID   string
	BucketName string
	BucketType string
}

func (client *B2Client) FindBucket(bucketName string) (err error) {

	input := make(map[string]string)
	input["accountId"] = client.AccountID
	input["bucketName"] = bucketName

	url := client.getAPIURL() + "/b2api/v1/b2_list_buckets"

	readCloser, _, _, err := client.call(0, url, http.MethodPost, nil, input)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	output := make(map[string][]ListBucketOutput, 0)

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return err
	}

	for _, bucket := range output["buckets"] {
		if bucket.BucketName == bucketName {
			client.BucketName = bucket.BucketName
			client.BucketID = bucket.BucketID
			break
		}
	}

	if client.BucketID == "" {
		return fmt.Errorf("Bucket %s not found", bucketName)
	}

	return nil
}

type B2Entry struct {
	FileID          string
	FileName        string
	Action          string
	Size            int64
	UploadTimestamp int64
}

type B2ListFileNamesOutput struct {
	Files        []*B2Entry
	NextFileName string
	NextFileId   string
}

func (client *B2Client) ListFileNames(threadIndex int, startFileName string, singleFile bool, includeVersions bool) (files []*B2Entry, err error) {

	maxFileCount := 1000
	if singleFile {
		if includeVersions {
			maxFileCount = 4
			if client.TestMode {
				maxFileCount = 1
			}
		} else {
			maxFileCount = 1
		}
	} else if client.TestMode {
		maxFileCount = 10
	}

	input := make(map[string]interface{})
	input["bucketId"] = client.BucketID
	input["startFileName"] = client.StorageDir + startFileName
	input["maxFileCount"] = maxFileCount
	input["prefix"] = client.StorageDir

	for {
		url := client.getAPIURL() + "/b2api/v1/b2_list_file_names"
		requestHeaders := map[string]string{}
		requestMethod := http.MethodPost
		var requestInput interface{}
		requestInput = input
		if includeVersions {
			url = client.getAPIURL() + "/b2api/v1/b2_list_file_versions"
		} else if singleFile {
			// handle a single file with no versions as a special case to download the last byte of the file
			url = client.getDownloadURL() + "/file/" + client.BucketName + "/" + B2Escape(client.StorageDir + startFileName)
			// requesting byte -1 works for empty files where 0-0 fails with a 416 error
			requestHeaders["Range"] = "bytes=-1"
			// HEAD request
			requestMethod = http.MethodHead
			requestInput = 0
		}
		var readCloser io.ReadCloser
		var responseHeader http.Header
		var err error
		readCloser, responseHeader, _, err = client.call(threadIndex, url, requestMethod, requestHeaders, requestInput)
		if err != nil {
			return nil, err
		}

		if readCloser != nil {
			defer readCloser.Close()
		}

		output := B2ListFileNamesOutput{}

		if singleFile && !includeVersions {
			if responseHeader == nil {
				LOG_DEBUG("BACKBLAZE_LIST", "%s did not return headers", url)
				return []*B2Entry{}, nil
			}
			requiredHeaders := []string{
				"x-bz-file-id",
				"x-bz-file-name",
			}
			missingKeys := []string{}
			for _, headerKey := range requiredHeaders {
				if "" == responseHeader.Get(headerKey) {
					missingKeys = append(missingKeys, headerKey)
				}
			}
			if len(missingKeys) > 0 {
				return nil, fmt.Errorf("%s missing headers: %s", url, missingKeys)
			}
			// construct the B2Entry from the response headers of the download request
			fileID := responseHeader.Get("x-bz-file-id")
			fileName := responseHeader.Get("x-bz-file-name")
			fileAction := "upload"
			// byte range that is returned: "bytes #-#/#
			rangeString := responseHeader.Get("Content-Range")
			// total file size; 1 if file has content, 0 if it's empty
			lengthString := responseHeader.Get("Content-Length")
			var fileSize int64
			if "" != rangeString {
				fileSize, _ = strconv.ParseInt(rangeString[strings.Index(rangeString, "/")+1:], 0, 64)
			} else if "" != lengthString {
				// this should only execute if the requested file is empty and the range request didn't result in a Content-Range header
				fileSize, _ = strconv.ParseInt(lengthString, 0, 64)
				if fileSize != 0 {
					return nil, fmt.Errorf("%s returned non-zero file length", url)
				}
			} else {
				return nil, fmt.Errorf("could not parse headers returned by %s", url)
			}
			fileUploadTimestamp, _ := strconv.ParseInt(responseHeader.Get("X-Bz-Upload-Timestamp"), 0, 64)

			return []*B2Entry{{fileID, fileName[len(client.StorageDir):], fileAction, fileSize, fileUploadTimestamp}}, nil
		}

		if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
			return nil, err
		}

		ioutil.ReadAll(readCloser)

		for _, file := range output.Files {
			file.FileName = file.FileName[len(client.StorageDir):]
			if singleFile {
				if file.FileName == startFileName {
					files = append(files, file)
					if !includeVersions {
						output.NextFileName = ""
						break
					}
				} else {
					output.NextFileName = ""
					break
				}
			} else {
				if strings.HasPrefix(file.FileName, startFileName) {
					files = append(files, file)
				} else {
					output.NextFileName = ""
					break
				}
			}
		}

		if len(output.NextFileName) == 0 {
			break
		}

		input["startFileName"] = output.NextFileName
		if includeVersions {
			input["startFileId"] = output.NextFileId
		}
	}

	return files, nil
}

func (client *B2Client) DeleteFile(threadIndex int, fileName string, fileID string) (err error) {

	input := make(map[string]string)
	input["fileName"] = client.StorageDir + fileName
	input["fileId"] = fileID

	url := client.getAPIURL() + "/b2api/v1/b2_delete_file_version"
	readCloser, _, _, err := client.call(threadIndex, url, http.MethodPost, make(map[string]string), input)
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

type B2HideFileOutput struct {
	FileID string
}

func (client *B2Client) HideFile(threadIndex int, fileName string) (fileID string, err error) {

	input := make(map[string]string)
	input["bucketId"] = client.BucketID
	input["fileName"] = client.StorageDir + fileName

	url := client.getAPIURL() + "/b2api/v1/b2_hide_file"
	readCloser, _, _, err := client.call(threadIndex, url, http.MethodPost, make(map[string]string), input)
	if err != nil {
		return "", err
	}

	defer readCloser.Close()

	output := &B2HideFileOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return "", err
	}

	readCloser.Close()
	return output.FileID, nil
}

func (client *B2Client) DownloadFile(threadIndex int, filePath string) (io.ReadCloser, int64, error) {

	url := client.getDownloadURL() + "/file/" + client.BucketName + "/" + B2Escape(client.StorageDir + filePath)

	readCloser, _, len, err := client.call(threadIndex, url, http.MethodGet, make(map[string]string), 0)
	return readCloser, len, err
}

type B2GetUploadArgumentOutput struct {
	BucketID           string
	UploadURL          string
	AuthorizationToken string
}

func (client *B2Client) getUploadURL(threadIndex int) error {
	input := make(map[string]string)
	input["bucketId"] = client.BucketID

	url := client.getAPIURL() + "/b2api/v1/b2_get_upload_url"
	readCloser, _, _, err := client.call(threadIndex, url, http.MethodPost, make(map[string]string), input)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	output := &B2GetUploadArgumentOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return err
	}

	client.UploadURLs[threadIndex] = output.UploadURL
	client.UploadTokens[threadIndex] = output.AuthorizationToken

	return nil
}

func (client *B2Client) UploadFile(threadIndex int, filePath string, content []byte, rateLimit int) (err error) {

	hasher := sha1.New()
	hasher.Write(content)
	hash := hex.EncodeToString(hasher.Sum(nil))

	headers := make(map[string]string)
	headers["X-Bz-File-Name"] = B2Escape(client.StorageDir + filePath)
	headers["Content-Length"] = fmt.Sprintf("%d", len(content))
	headers["Content-Type"] = "application/octet-stream"
	headers["X-Bz-Content-Sha1"] = hash

	readCloser, _, _, err := client.call(threadIndex, "", http.MethodPost, headers, CreateRateLimitedReader(content, rateLimit))
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}
