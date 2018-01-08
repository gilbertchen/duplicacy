// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
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
	ApplicationKey     string
	AuthorizationToken string
	APIURL             string
	DownloadURL        string
	BucketName         string
	BucketID           string

	UploadURL   string
	UploadToken string

	TestMode bool
}

func NewB2Client(accountID string, applicationKey string) *B2Client {
	client := &B2Client{
		HTTPClient:     http.DefaultClient,
		AccountID:      accountID,
		ApplicationKey: applicationKey,
	}
	return client
}

func (client *B2Client) retry(backoff int, response *http.Response) int {
	if response != nil {
		if backoffList, found := response.Header["Retry-After"]; found && len(backoffList) > 0 {
			retryAfter, _ := strconv.Atoi(backoffList[0])
			if retryAfter >= 1 {
				time.Sleep(time.Duration(retryAfter) * time.Second)
				return 0
			}
		}
	}
	if backoff == 0 {
		backoff = 1
	} else {
		backoff *= 2
	}
	time.Sleep(time.Duration(backoff) * time.Second)
	return backoff
}

func (client *B2Client) call(url string, method string, requestHeaders map[string]string, input interface{}) (io.ReadCloser, http.Header, int64, error) {

	switch method {
	case http.MethodGet:
		break
	case http.MethodHead:
		break
	case http.MethodPost:
		break
	default:
		return nil, nil, 0, fmt.Errorf("unhandled http request method: " + method)
	}

	var response *http.Response

	backoff := 0
	for i := 0; i < 8; i++ {
		var inputReader *bytes.Reader

		switch input.(type) {
		default:
			jsonInput, err := json.Marshal(input)
			if err != nil {
				return nil, nil, 0, err
			}
			inputReader = bytes.NewReader(jsonInput)
		case []byte:
			inputReader = bytes.NewReader(input.([]byte))
		case int:
			inputReader = bytes.NewReader([]byte(""))
		}

		request, err := http.NewRequest(method, url, inputReader)
		if err != nil {
			return nil, nil, 0, err
		}

		if url == B2AuthorizationURL {
			request.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(client.AccountID+":"+client.ApplicationKey)))
		} else {
			request.Header.Set("Authorization", client.AuthorizationToken)
		}

		if requestHeaders != nil {
			for key, value := range requestHeaders {
				request.Header.Set(key, value)
			}
		}

		if client.TestMode {
			r := rand.Float32()
			if r < 0.5 {
				request.Header.Set("X-Bz-Test-Mode", "expire_some_account_authorization_tokens")
			} else {
				request.Header.Set("X-Bz-Test-Mode", "force_cap_exceeded")
			}
		}

		response, err = client.HTTPClient.Do(request)
		if err != nil {
			if url != B2AuthorizationURL {
				LOG_DEBUG("BACKBLAZE_CALL", "URL request '%s' returned an error: %v", url, err)
				backoff = client.retry(backoff, response)
				continue
			}
			return nil, nil, 0, err
		}

		if response.StatusCode < 300 {
			return response.Body, response.Header, response.ContentLength, nil
		}

		LOG_DEBUG("BACKBLAZE_CALL", "URL request '%s' returned status code %d", url, response.StatusCode)

		io.Copy(ioutil.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode == 401 {
			if url == B2AuthorizationURL {
				return nil, nil, 0, fmt.Errorf("Authorization failure")
			}
			client.AuthorizeAccount()
			continue
		} else if response.StatusCode == 403 {
			if !client.TestMode {
				return nil, nil, 0, fmt.Errorf("B2 cap exceeded")
			}
			continue
		} else if response.StatusCode == 404 {
			if http.MethodHead == method {
				LOG_DEBUG("BACKBLAZE_CALL", "URL request '%s' returned status code %d", url, response.StatusCode)
				return nil, nil, 0, nil
			}
		} else if response.StatusCode == 416 {
			if http.MethodHead == method {
				// 416 Requested Range Not Satisfiable
				return nil, nil, 0, fmt.Errorf("URL request '%s' returned status code %d", url, response.StatusCode)
			}
		} else if response.StatusCode == 429 || response.StatusCode == 408 {
			backoff = client.retry(backoff, response)
			continue
		} else if response.StatusCode >= 500 && response.StatusCode <= 599 {
			backoff = client.retry(backoff, response)
			continue
		} else {
			LOG_INFO("BACKBLAZE_CALL", "URL request '%s' returned status code %d", url, response.StatusCode)
			backoff = client.retry(backoff, response)
			continue
		}

		defer response.Body.Close()

		e := &B2Error{}

		if err := json.NewDecoder(response.Body).Decode(e); err != nil {
			return nil, nil, 0, err
		}

		return nil, nil, 0, e
	}

	return nil, nil, 0, fmt.Errorf("Maximum backoff reached")
}

type B2AuthorizeAccountOutput struct {
	AccountID          string
	AuthorizationToken string
	APIURL             string
	DownloadURL        string
}

func (client *B2Client) AuthorizeAccount() (err error) {

	readCloser, _, _, err := client.call(B2AuthorizationURL, http.MethodPost, nil, make(map[string]string))
	if err != nil {
		return err
	}

	defer readCloser.Close()

	output := &B2AuthorizeAccountOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return err
	}

	client.AuthorizationToken = output.AuthorizationToken
	client.APIURL = output.APIURL
	client.DownloadURL = output.DownloadURL

	return nil
}

type ListBucketOutput struct {
	AccoundID  string
	BucketID   string
	BucketName string
	BucketType string
}

func (client *B2Client) FindBucket(bucketName string) (err error) {

	input := make(map[string]string)
	input["accountId"] = client.AccountID

	url := client.APIURL + "/b2api/v1/b2_list_buckets"

	readCloser, _, _, err := client.call(url, http.MethodPost, nil, input)
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

func (client *B2Client) ListFileNames(startFileName string, singleFile bool, includeVersions bool) (files []*B2Entry, err error) {

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
	input["startFileName"] = startFileName
	input["maxFileCount"] = maxFileCount

	for {
		url := client.APIURL + "/b2api/v1/b2_list_file_names"
		requestHeaders := map[string]string{}
		requestMethod := http.MethodPost
		var requestInput interface{}
		requestInput = input
		if includeVersions {
			url = client.APIURL + "/b2api/v1/b2_list_file_versions"
		} else if singleFile {
			// handle a single file with no versions as a special case to download the last byte of the file
			url = client.DownloadURL + "/file/" + client.BucketName + "/" + startFileName
			// requesting byte -1 works for empty files where 0-0 fails with a 416 error
			requestHeaders["Range"] = "bytes=-1"
			// HEAD request
			requestMethod = http.MethodHead
			requestInput = 0
		}
		var readCloser io.ReadCloser
		var responseHeader http.Header
		var err error
		readCloser, responseHeader, _, err = client.call(url, requestMethod, requestHeaders, requestInput)
		if err != nil {
			return nil, err
		}

		if readCloser != nil {
			defer readCloser.Close()
		}

		output := B2ListFileNamesOutput{}

		if singleFile && !includeVersions {
			if responseHeader == nil {
				LOG_DEBUG("BACKBLAZE_LIST", "b2_download_file_by_name did not return headers")
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
				return nil, fmt.Errorf("b2_download_file_by_name missing headers: %s", missingKeys)
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
					return nil, fmt.Errorf("b2_download_file_by_name returned non-zero file length")
				}
			} else {
				return nil, fmt.Errorf("could not parse b2_download_file_by_name headers")
			}
			fileUploadTimestamp, _ := strconv.ParseInt(responseHeader.Get("X-Bz-Upload-Timestamp"), 0, 64)

			return []*B2Entry{&B2Entry{fileID, fileName, fileAction, fileSize, fileUploadTimestamp}}, nil
		}

		if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
			return nil, err
		}

		ioutil.ReadAll(readCloser)

		if startFileName == "" {
			files = append(files, output.Files...)
		} else {
			for _, file := range output.Files {
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

func (client *B2Client) DeleteFile(fileName string, fileID string) (err error) {

	input := make(map[string]string)
	input["fileName"] = fileName
	input["fileId"] = fileID

	url := client.APIURL + "/b2api/v1/b2_delete_file_version"
	readCloser, _, _, err := client.call(url, http.MethodPost, make(map[string]string), input)
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

type B2HideFileOutput struct {
	FileID string
}

func (client *B2Client) HideFile(fileName string) (fileID string, err error) {

	input := make(map[string]string)
	input["bucketId"] = client.BucketID
	input["fileName"] = fileName

	url := client.APIURL + "/b2api/v1/b2_hide_file"
	readCloser, _, _, err := client.call(url, http.MethodPost, make(map[string]string), input)
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

func (client *B2Client) DownloadFile(filePath string) (io.ReadCloser, int64, error) {

	url := client.DownloadURL + "/file/" + client.BucketName + "/" + filePath

	readCloser, _, len, err := client.call(url, http.MethodGet, make(map[string]string), 0)
	return readCloser, len, err
}

type B2GetUploadArgumentOutput struct {
	BucketID           string
	UploadURL          string
	AuthorizationToken string
}

func (client *B2Client) getUploadURL() error {
	input := make(map[string]string)
	input["bucketId"] = client.BucketID

	url := client.APIURL + "/b2api/v1/b2_get_upload_url"
	readCloser, _, _, err := client.call(url, http.MethodPost, make(map[string]string), input)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	output := &B2GetUploadArgumentOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return err
	}

	client.UploadURL = output.UploadURL
	client.UploadToken = output.AuthorizationToken

	return nil
}

func (client *B2Client) UploadFile(filePath string, content []byte, rateLimit int) (err error) {

	hasher := sha1.New()
	hasher.Write(content)
	hash := hex.EncodeToString(hasher.Sum(nil))

	headers := make(map[string]string)
	headers["X-Bz-File-Name"] = filePath
	headers["Content-Type"] = "application/octet-stream"
	headers["X-Bz-Content-Sha1"] = hash

	var response *http.Response

	backoff := 0
	for i := 0; i < 8; i++ {

		if client.UploadURL == "" || client.UploadToken == "" {
			err = client.getUploadURL()
			if err != nil {
				return err
			}
		}

		request, err := http.NewRequest("POST", client.UploadURL, CreateRateLimitedReader(content, rateLimit))
		if err != nil {
			return err
		}
		request.ContentLength = int64(len(content))

		request.Header.Set("Authorization", client.UploadToken)
		request.Header.Set("X-Bz-File-Name", filePath)
		request.Header.Set("Content-Type", "application/octet-stream")
		request.Header.Set("X-Bz-Content-Sha1", hash)

		for key, value := range headers {
			request.Header.Set(key, value)
		}

		if client.TestMode {
			r := rand.Float32()
			if r < 0.8 {
				request.Header.Set("X-Bz-Test-Mode", "fail_some_uploads")
			} else if r < 0.9 {
				request.Header.Set("X-Bz-Test-Mode", "expire_some_account_authorization_tokens")
			} else {
				request.Header.Set("X-Bz-Test-Mode", "force_cap_exceeded")
			}
		}

		response, err = client.HTTPClient.Do(request)
		if err != nil {
			LOG_DEBUG("BACKBLAZE_UPLOAD", "URL request '%s' returned an error: %v", client.UploadURL, err)
			backoff = client.retry(backoff, response)
			client.UploadURL = ""
			client.UploadToken = ""
			continue
		}

		io.Copy(ioutil.Discard, response.Body)
		response.Body.Close()

		if response.StatusCode < 300 {
			return nil
		}

		LOG_DEBUG("BACKBLAZE_UPLOAD", "URL request '%s' returned status code %d", client.UploadURL, response.StatusCode)

		if response.StatusCode == 401 {
			LOG_INFO("BACKBLAZE_UPLOAD", "Re-authorization required")
			client.UploadURL = ""
			client.UploadToken = ""
			continue
		} else if response.StatusCode == 403 {
			if !client.TestMode {
				return fmt.Errorf("B2 cap exceeded")
			}
			continue
		} else {
			LOG_INFO("BACKBLAZE_UPLOAD", "URL request '%s' returned status code %d", client.UploadURL, response.StatusCode)
			backoff = client.retry(backoff, response)
			client.UploadURL = ""
			client.UploadToken = ""
		}
	}

	return fmt.Errorf("Maximum backoff reached")
}
