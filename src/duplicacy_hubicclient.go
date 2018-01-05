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
	"net"
	"net/http"
	net_url "net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

type HubicError struct {
	Status  int
	Message string
}

func (err HubicError) Error() string {
	return fmt.Sprintf("%d %s", err.Status, err.Message)
}

var HubicRefreshTokenURL = "https://duplicacy.com/hubic_refresh"
var HubicCredentialURL = "https://api.hubic.com/1.0/account/credentials"

type HubicCredential struct {
	Token    string
	Endpoint string
	Expires  time.Time
}

type HubicClient struct {
	HTTPClient *http.Client

	TokenFile string
	Token     *oauth2.Token
	TokenLock *sync.Mutex

	Credential     HubicCredential
	CredentialLock *sync.Mutex

	TestMode bool
}

func NewHubicClient(tokenFile string) (*HubicClient, error) {

	description, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	token := new(oauth2.Token)
	if err := json.Unmarshal(description, token); err != nil {
		return nil, fmt.Errorf("%v: %s", err, description)
	}

	client := &HubicClient{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout:   60 * time.Second,
				ResponseHeaderTimeout: 300 * time.Second,
				ExpectContinueTimeout: 10 * time.Second,
			},
		},
		TokenFile:      tokenFile,
		Token:          token,
		TokenLock:      &sync.Mutex{},
		CredentialLock: &sync.Mutex{},
	}

	err = client.RefreshToken(false)
	if err != nil {
		return nil, err
	}

	err = client.GetCredential()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (client *HubicClient) call(url string, method string, input interface{}, extraHeader map[string]string) (io.ReadCloser, int64, string, error) {

	var response *http.Response

	backoff := 1
	for i := 0; i < 11; i++ {

		LOG_DEBUG("HUBIC_CALL", "%s %s", method, url)

		//fmt.Printf("%s %s\n", method, url)

		var inputReader io.Reader

		switch input.(type) {
		default:
			jsonInput, err := json.Marshal(input)
			if err != nil {
				return nil, 0, "", err
			}
			inputReader = bytes.NewReader(jsonInput)
		case []byte:
			inputReader = bytes.NewReader(input.([]byte))
		case int:
			inputReader = bytes.NewReader([]byte(""))
		case *bytes.Buffer:
			inputReader = bytes.NewReader(input.(*bytes.Buffer).Bytes())
		case *RateLimitedReader:
			input.(*RateLimitedReader).Reset()
			inputReader = input.(*RateLimitedReader)
		}

		request, err := http.NewRequest(method, url, inputReader)
		if err != nil {
			return nil, 0, "", err
		}

		if reader, ok := inputReader.(*RateLimitedReader); ok {
			request.ContentLength = reader.Length()
		}

		if url == HubicCredentialURL {
			client.TokenLock.Lock()
			request.Header.Set("Authorization", "Bearer "+client.Token.AccessToken)
			client.TokenLock.Unlock()
		} else if url != HubicRefreshTokenURL {
			client.CredentialLock.Lock()
			request.Header.Set("X-Auth-Token", client.Credential.Token)
			client.CredentialLock.Unlock()
		}

		for key, value := range extraHeader {
			request.Header.Set(key, value)
		}

		response, err = client.HTTPClient.Do(request)
		if err != nil {
			if url != HubicCredentialURL {
				retryAfter := time.Duration((0.5 + rand.Float32()) * 1000.0 * float32(backoff))
				LOG_INFO("HUBIC_CALL", "%s %s returned an error: %v; retry after %d milliseconds", method, url, err, retryAfter)
				time.Sleep(retryAfter * time.Millisecond)
				backoff *= 2
				continue
			}
			return nil, 0, "", err
		}

		contentType := ""
		if len(response.Header["Content-Type"]) > 0 {
			contentType = response.Header["Content-Type"][0]
		}

		if response.StatusCode < 400 {
			return response.Body, response.ContentLength, contentType, nil
		}

		/*buffer := bytes.NewBufferString("")
		  io.Copy(buffer, response.Body)
		  fmt.Printf("%s\n", buffer.String())*/

		response.Body.Close()

		if response.StatusCode == 401 {

			if url == HubicRefreshTokenURL {
				return nil, 0, "", HubicError{Status: response.StatusCode, Message: "Authorization error when refreshing token"}
			}

			if url == HubicCredentialURL {
				return nil, 0, "", HubicError{Status: response.StatusCode, Message: "Authorization error when retrieving credentials"}
			}

			err = client.RefreshToken(true)
			if err != nil {
				return nil, 0, "", err
			}

			err = client.GetCredential()
			if err != nil {
				return nil, 0, "", err
			}
			continue
		} else if response.StatusCode >= 500 && response.StatusCode < 600 {
			retryAfter := time.Duration((0.5 + rand.Float32()) * 1000.0 * float32(backoff))
			LOG_INFO("HUBIC_RETRY", "Response status: %d; retry after %d milliseconds", response.StatusCode, retryAfter)
			time.Sleep(retryAfter * time.Millisecond)
			backoff *= 2
			continue
		} else if response.StatusCode == 408 {
			retryAfter := time.Duration((0.5 + rand.Float32()) * 1000.0 * float32(backoff))
			LOG_INFO("HUBIC_RETRY", "Response status: %d; retry after %d milliseconds", response.StatusCode, retryAfter)
			time.Sleep(retryAfter * time.Millisecond)
			backoff *= 2
			continue
		} else {
			return nil, 0, "", HubicError{Status: response.StatusCode, Message: "Hubic API error"}
		}
	}

	return nil, 0, "", fmt.Errorf("Maximum number of retries reached")
}

func (client *HubicClient) RefreshToken(force bool) (err error) {
	client.TokenLock.Lock()
	defer client.TokenLock.Unlock()

	if !force && client.Token.Valid() {
		return nil
	}

	readCloser, _, _, err := client.call(HubicRefreshTokenURL, "POST", client.Token, nil)
	if err != nil {
		return err
	}

	defer readCloser.Close()

	if err = json.NewDecoder(readCloser).Decode(&client.Token); err != nil {
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

func (client *HubicClient) GetCredential() (err error) {
	client.CredentialLock.Lock()
	defer client.CredentialLock.Unlock()

	readCloser, _, _, err := client.call(HubicCredentialURL, "GET", 0, nil)
	if err != nil {
		return err
	}

	buffer := bytes.NewBufferString("")
	io.Copy(buffer, readCloser)
	readCloser.Close()

	if err = json.NewDecoder(buffer).Decode(&client.Credential); err != nil {
		return fmt.Errorf("%v (response: %s)", err, buffer)
	}

	return nil
}

type HubicEntry struct {
	Name   string `json:"name"`
	Size   int64  `json:"bytes"`
	Type   string `json:"content_type"`
	Subdir string `json:"subdir"`
}

func (client *HubicClient) ListEntries(path string) ([]HubicEntry, error) {

	if len(path) > 0 && path[len(path)-1] != '/' {
		path += "/"
	}

	count := 1000
	if client.TestMode {
		count = 8
	}

	marker := ""

	var entries []HubicEntry

	for {

		client.CredentialLock.Lock()
		url := client.Credential.Endpoint + "/default"
		client.CredentialLock.Unlock()
		url += fmt.Sprintf("?format=json&limit=%d&delimiter=%%2f", count)
		if path != "" {
			url += "&prefix=" + net_url.QueryEscape(path)
		}
		if marker != "" {
			url += "&marker=" + net_url.QueryEscape(marker)
		}

		readCloser, _, _, err := client.call(url, "GET", 0, nil)
		if err != nil {
			return nil, err
		}

		defer readCloser.Close()

		var output []HubicEntry

		if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
			return nil, err
		}

		for _, entry := range output {
			if entry.Subdir == "" {
				marker = entry.Name
			} else {
				marker = entry.Subdir
				for len(entry.Subdir) > 0 && entry.Subdir[len(entry.Subdir)-1] == '/' {
					entry.Subdir = entry.Subdir[:len(entry.Subdir)-1]
				}
				entry.Name = entry.Subdir
				entry.Type = "application/directory"
			}
			if path != "" && strings.HasPrefix(entry.Name, path) {
				entry.Name = entry.Name[len(path):]
			}
			entries = append(entries, entry)
		}
		if len(output) < count {
			break
		}
	}

	return entries, nil
}

func (client *HubicClient) GetFileInfo(path string) (bool, bool, int64, error) {

	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	client.CredentialLock.Lock()
	url := client.Credential.Endpoint + "/default/" + path
	client.CredentialLock.Unlock()

	readCloser, size, contentType, err := client.call(url, "HEAD", 0, nil)
	if err != nil {
		if e, ok := err.(HubicError); ok && e.Status == 404 {
			return false, false, 0, nil
		} else {
			return false, false, 0, err
		}
	}

	readCloser.Close()

	return true, contentType == "application/directory", size, nil
}

func (client *HubicClient) DownloadFile(path string) (io.ReadCloser, int64, error) {

	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	client.CredentialLock.Lock()
	url := client.Credential.Endpoint + "/default/" + path
	client.CredentialLock.Unlock()

	readCloser, size, _, err := client.call(url, "GET", 0, nil)
	return readCloser, size, err
}

func (client *HubicClient) UploadFile(path string, content []byte, rateLimit int) (err error) {

	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	client.CredentialLock.Lock()
	url := client.Credential.Endpoint + "/default/" + path
	client.CredentialLock.Unlock()

	header := make(map[string]string)
	header["Content-Type"] = "application/octet-stream"

	readCloser, _, _, err := client.call(url, "PUT", CreateRateLimitedReader(content, rateLimit), header)

	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *HubicClient) DeleteFile(path string) error {

	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	client.CredentialLock.Lock()
	url := client.Credential.Endpoint + "/default/" + path
	client.CredentialLock.Unlock()

	readCloser, _, _, err := client.call(url, "DELETE", 0, nil)

	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *HubicClient) MoveFile(from string, to string) error {

	for len(from) > 0 && from[len(from)-1] == '/' {
		from = from[:len(from)-1]
	}

	for len(to) > 0 && to[len(to)-1] == '/' {
		to = to[:len(to)-1]
	}

	client.CredentialLock.Lock()
	url := client.Credential.Endpoint + "/default/" + from
	client.CredentialLock.Unlock()

	header := make(map[string]string)
	header["Destination"] = "default/" + to

	readCloser, _, _, err := client.call(url, "COPY", 0, header)

	if err != nil {
		return err
	}

	readCloser.Close()

	return client.DeleteFile(from)
}

func (client *HubicClient) CreateDirectory(path string) error {

	for len(path) > 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	client.CredentialLock.Lock()
	url := client.Credential.Endpoint + "/default/" + path
	client.CredentialLock.Unlock()

	header := make(map[string]string)
	header["Content-Type"] = "application/directory"

	readCloser, _, _, err := client.call(url, "PUT", "", header)

	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}
