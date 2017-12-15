// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

type OneDriveError struct {
	Status  int
	Message string
}

func (err OneDriveError) Error() string {
	return fmt.Sprintf("%d %s", err.Status, err.Message)
}

type OneDriveErrorResponse struct {
	Error OneDriveError `json:"error"`
}

var OneDriveRefreshTokenURL = "https://duplicacy.com/one_refresh"
var OneDriveAPIURL = "https://api.onedrive.com/v1.0"

type OneDriveClient struct {
	HTTPClient *http.Client

	TokenFile string
	Token     *oauth2.Token
	TokenLock *sync.Mutex

	IsConnected bool
	TestMode    bool
}

func NewOneDriveClient(tokenFile string) (*OneDriveClient, error) {

	description, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	token := new(oauth2.Token)
	if err := json.Unmarshal(description, token); err != nil {
		return nil, err
	}

	client := &OneDriveClient{
		HTTPClient: http.DefaultClient,
		TokenFile:  tokenFile,
		Token:      token,
		TokenLock:  &sync.Mutex{},
	}

	client.RefreshToken(false)

	return client, nil
}

func (client *OneDriveClient) call(url string, method string, input interface{}, contentType string) (io.ReadCloser, int64, error) {

	var response *http.Response

	backoff := 1
	for i := 0; i < 8; i++ {

		LOG_DEBUG("ONEDRIVE_CALL", "%s %s", method, url)

		var inputReader io.Reader

		switch input.(type) {
		default:
			jsonInput, err := json.Marshal(input)
			if err != nil {
				return nil, 0, err
			}
			inputReader = bytes.NewReader(jsonInput)
		case []byte:
			inputReader = bytes.NewReader(input.([]byte))
		case int:
			inputReader = nil
		case *bytes.Buffer:
			inputReader = bytes.NewReader(input.(*bytes.Buffer).Bytes())
		case *RateLimitedReader:
			input.(*RateLimitedReader).Reset()
			inputReader = input.(*RateLimitedReader)
		}

		request, err := http.NewRequest(method, url, inputReader)
		if err != nil {
			return nil, 0, err
		}

		if reader, ok := inputReader.(*RateLimitedReader); ok {
			request.ContentLength = reader.Length()
		}

		if url != OneDriveRefreshTokenURL {
			client.TokenLock.Lock()
			request.Header.Set("Authorization", "Bearer "+client.Token.AccessToken)
			client.TokenLock.Unlock()
		}
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}

		response, err = client.HTTPClient.Do(request)
		if err != nil {
			if client.IsConnected {
				if strings.Contains(err.Error(), "TLS handshake timeout") {
					// Give a long timeout regardless of backoff when a TLS timeout happens, hoping that
					// idle connections are not to be reused on reconnect.
					retryAfter := time.Duration(rand.Float32()*60000 + 180000)
					LOG_INFO("ONEDRIVE_RETRY", "TLS handshake timeout; retry after %d milliseconds", retryAfter)
					time.Sleep(retryAfter * time.Millisecond)
				} else {
					// For all other errors just blindly retry until the maximum is reached
					retryAfter := time.Duration(rand.Float32() * 1000.0 * float32(backoff))
					LOG_INFO("ONEDRIVE_RETRY", "%v; retry after %d milliseconds", err, retryAfter)
					time.Sleep(retryAfter * time.Millisecond)
				}
				backoff *= 2
				continue
			}
			return nil, 0, err
		}

		client.IsConnected = true

		if response.StatusCode < 400 {
			return response.Body, response.ContentLength, nil
		}

		defer response.Body.Close()

		errorResponse := &OneDriveErrorResponse{
			Error: OneDriveError{Status: response.StatusCode},
		}

		if response.StatusCode == 401 {

			if url == OneDriveRefreshTokenURL {
				return nil, 0, OneDriveError{Status: response.StatusCode, Message: "Authorization error when refreshing token"}
			}

			err = client.RefreshToken(true)
			if err != nil {
				return nil, 0, err
			}
			continue
		} else if response.StatusCode > 401 && response.StatusCode != 404 {
			retryAfter := time.Duration(rand.Float32() * 1000.0 * float32(backoff))
			LOG_INFO("ONEDRIVE_RETRY", "Response code: %d; retry after %d milliseconds", response.StatusCode, retryAfter)
			time.Sleep(retryAfter * time.Millisecond)
			backoff *= 2
			continue
		} else {
			if err := json.NewDecoder(response.Body).Decode(errorResponse); err != nil {
				return nil, 0, OneDriveError{Status: response.StatusCode, Message: fmt.Sprintf("Unexpected response")}
			}

			errorResponse.Error.Status = response.StatusCode
			return nil, 0, errorResponse.Error
		}
	}

	return nil, 0, fmt.Errorf("Maximum number of retries reached")
}

func (client *OneDriveClient) RefreshToken(force bool) (err error) {
	client.TokenLock.Lock()
	defer client.TokenLock.Unlock()

	if !force && client.Token.Valid() {
		return nil
	}

	readCloser, _, err := client.call(OneDriveRefreshTokenURL, "POST", client.Token, "")
	if err != nil {
		return fmt.Errorf("failed to refresh the access token: %v", err)
	}

	defer readCloser.Close()

	if err = json.NewDecoder(readCloser).Decode(client.Token); err != nil {
		return err
	}

	description, err := json.Marshal(client.Token)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(client.TokenFile, description, 0644)
	if err != nil {
		return err
	}

	return nil
}

type OneDriveEntry struct {
	ID     string
	Name   string
	Folder map[string]interface{}
	Size   int64
}

type OneDriveListEntriesOutput struct {
	Entries  []OneDriveEntry `json:"value"`
	NextLink string          `json:"@odata.nextLink"`
}

func (client *OneDriveClient) ListEntries(path string) ([]OneDriveEntry, error) {

	entries := []OneDriveEntry{}

	url := OneDriveAPIURL + "/drive/root:/" + path + ":/children"
	if path == "" {
		url = OneDriveAPIURL + "/drive/root/children"
	}
	if client.TestMode {
		url += "?top=8"
	} else {
		url += "?top=1000"
	}
	url += "&select=name,size,folder"

	for {
		readCloser, _, err := client.call(url, "GET", 0, "")
		if err != nil {
			return nil, err
		}

		defer readCloser.Close()

		output := &OneDriveListEntriesOutput{}

		if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
			return nil, err
		}

		entries = append(entries, output.Entries...)

		url = output.NextLink
		if url == "" {
			break
		}
	}

	return entries, nil
}

func (client *OneDriveClient) GetFileInfo(path string) (string, bool, int64, error) {

	url := OneDriveAPIURL + "/drive/root:/" + path
	url += "?select=id,name,size,folder"

	readCloser, _, err := client.call(url, "GET", 0, "")
	if err != nil {
		if e, ok := err.(OneDriveError); ok && e.Status == 404 {
			return "", false, 0, nil
		} else {
			return "", false, 0, err
		}
	}

	defer readCloser.Close()

	output := &OneDriveEntry{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return "", false, 0, err
	}

	return output.ID, len(output.Folder) != 0, output.Size, nil
}

func (client *OneDriveClient) DownloadFile(path string) (io.ReadCloser, int64, error) {

	url := OneDriveAPIURL + "/drive/items/root:/" + path + ":/content"

	return client.call(url, "GET", 0, "")
}

func (client *OneDriveClient) UploadFile(path string, content []byte, rateLimit int) (err error) {

	url := OneDriveAPIURL + "/drive/root:/" + path + ":/content"

	readCloser, _, err := client.call(url, "PUT", CreateRateLimitedReader(content, rateLimit), "application/octet-stream")

	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *OneDriveClient) DeleteFile(path string) error {

	url := OneDriveAPIURL + "/drive/root:/" + path

	readCloser, _, err := client.call(url, "DELETE", 0, "")
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *OneDriveClient) MoveFile(path string, parent string) error {

	url := OneDriveAPIURL + "/drive/root:/" + path

	parentReference := make(map[string]string)
	parentReference["path"] = "/drive/root:/" + parent

	parameters := make(map[string]interface{})
	parameters["parentReference"] = parentReference

	readCloser, _, err := client.call(url, "PATCH", parameters, "application/json")
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *OneDriveClient) CreateDirectory(path string, name string) error {

	url := OneDriveAPIURL + "/root/children"

	if path != "" {

		parentID, isDir, _, err := client.GetFileInfo(path)
		if err != nil {
			return err
		}

		if parentID == "" {
			return fmt.Errorf("The path '%s' does not exist", path)
		}

		if !isDir {
			return fmt.Errorf("The path '%s' is not a directory", path)
		}

		url = OneDriveAPIURL + "/drive/items/" + parentID + "/children"
	}

	parameters := make(map[string]interface{})
	parameters["name"] = name
	parameters["folder"] = make(map[string]int)

	readCloser, _, err := client.call(url, "POST", parameters, "application/json")
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}
