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
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

type ACDError struct {
	Status  int
	Message string `json:"message"`
}

func (err ACDError) Error() string {
	return fmt.Sprintf("%d %s", err.Status, err.Message)
}

var ACDRefreshTokenURL = "https://duplicacy.com/acd_refresh"

type ACDClient struct {
	HTTPClient *http.Client

	TokenFile string
	Token     *oauth2.Token
	TokenLock *sync.Mutex

	ContentURL  string
	MetadataURL string

	TestMode bool
}

func NewACDClient(tokenFile string) (*ACDClient, error) {

	description, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	token := new(oauth2.Token)
	if err := json.Unmarshal(description, token); err != nil {
		return nil, err
	}

	client := &ACDClient{
		HTTPClient: http.DefaultClient,
		TokenFile:  tokenFile,
		Token:      token,
		TokenLock:  &sync.Mutex{},
	}

	client.GetEndpoint()

	return client, nil
}

func (client *ACDClient) call(url string, method string, input interface{}, contentType string) (io.ReadCloser, int64, error) {

	//LOG_DEBUG("ACD_CALL", "%s %s", method, url)

	var response *http.Response

	backoff := 1
	for i := 0; i < 8; i++ {
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
			inputReader = bytes.NewReader([]byte(""))
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

		if url != ACDRefreshTokenURL {
			client.TokenLock.Lock()
			request.Header.Set("Authorization", "Bearer "+client.Token.AccessToken)
			client.TokenLock.Unlock()
		}
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}

		response, err = client.HTTPClient.Do(request)
		if err != nil {
			return nil, 0, err
		}

		if response.StatusCode < 400 {
			return response.Body, response.ContentLength, nil
		}

		if response.StatusCode == 404 {
			buffer := new(bytes.Buffer)
			buffer.ReadFrom(response.Body)
			response.Body.Close()
			return nil, 0, ACDError{Status: response.StatusCode, Message: buffer.String()}
		}

		if response.StatusCode == 400 {
			defer response.Body.Close()

			e := &ACDError{
				Status: response.StatusCode,
			}

			if err := json.NewDecoder(response.Body).Decode(e); err == nil {
				return nil, 0, e
			} else {
				return nil, 0, ACDError{Status: response.StatusCode, Message: "Bad input parameter"}
			}
		}

		response.Body.Close()

		if response.StatusCode == 401 {

			if url == ACDRefreshTokenURL {
				return nil, 0, ACDError{Status: response.StatusCode, Message: "Unauthorized"}
			}

			err = client.RefreshToken()
			if err != nil {
				return nil, 0, err
			}

			continue
		} else if response.StatusCode == 403 {
			return nil, 0, ACDError{Status: response.StatusCode, Message: "Forbidden"}
		} else if response.StatusCode == 404 {
			return nil, 0, ACDError{Status: response.StatusCode, Message: "Resource not found"}
		} else if response.StatusCode == 409 {
			return nil, 0, ACDError{Status: response.StatusCode, Message: "Conflict"}
		} else if response.StatusCode == 411 {
			return nil, 0, ACDError{Status: response.StatusCode, Message: "Length required"}
		} else if response.StatusCode == 412 {
			return nil, 0, ACDError{Status: response.StatusCode, Message: "Precondition failed"}
		} else if response.StatusCode == 429 || response.StatusCode == 500 {
			reason := "Too many requests"
			if response.StatusCode == 500 {
				reason = "Internal server error"
			}
			retryAfter := time.Duration(rand.Float32() * 1000.0 * float32(backoff))
			LOG_INFO("ACD_RETRY", "%s; retry after %d milliseconds", reason, retryAfter)
			time.Sleep(retryAfter * time.Millisecond)
			backoff *= 2
			continue
		} else if response.StatusCode == 503 {
			return nil, 0, ACDError{Status: response.StatusCode, Message: "Service unavailable"}
		} else {
			return nil, 0, ACDError{Status: response.StatusCode, Message: "Unknown error"}
		}
	}

	return nil, 0, fmt.Errorf("Maximum number of retries reached")
}

func (client *ACDClient) RefreshToken() (err error) {

	client.TokenLock.Lock()
	defer client.TokenLock.Unlock()

	readCloser, _, err := client.call(ACDRefreshTokenURL, "POST", client.Token, "")
	if err != nil {
		return err
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

type ACDGetEndpointOutput struct {
	CustomerExists bool   `json:"customerExists"`
	ContentURL     string `json:"contentUrl"`
	MetadataURL    string `json:"metadataUrl"`
}

func (client *ACDClient) GetEndpoint() (err error) {

	readCloser, _, err := client.call("https://drive.amazonaws.com/drive/v1/account/endpoint", "GET", 0, "")
	if err != nil {
		return err
	}

	defer readCloser.Close()

	output := &ACDGetEndpointOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return err
	}

	client.ContentURL = output.ContentURL
	client.MetadataURL = output.MetadataURL

	return nil
}

type ACDEntry struct {
	Name string `json:"name"`
	ID   string `json:"id"`
	Size int64  `json:"size"`
	Kind string `json:"kind"`
}

type ACDListEntriesOutput struct {
	Count     int        `json:"count"`
	NextToken string     `json:"nextToken"`
	Entries   []ACDEntry `json:"data"`
}

func (client *ACDClient) ListEntries(parentID string, listFiles bool, listDirectories bool) ([]ACDEntry, error) {

	startToken := ""

	entries := []ACDEntry{}

	for {

		url := client.MetadataURL + "nodes/" + parentID + "/children?"

		if listFiles && !listDirectories {
			url += "filters=kind:FILE&"
		} else if !listFiles && listDirectories {
			url += "filters=kind:FOLDER&"
		}

		if startToken != "" {
			url += "startToken=" + startToken + "&"
		}

		if client.TestMode {
			url += "limit=8"
		} else {
			url += "limit=200"
		}

		readCloser, _, err := client.call(url, "GET", 0, "")
		if err != nil {
			return nil, err
		}

		defer readCloser.Close()

		output := &ACDListEntriesOutput{}

		if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
			return nil, err
		}

		entries = append(entries, output.Entries...)

		startToken = output.NextToken
		if startToken == "" {
			break
		}
	}

	return entries, nil
}

func (client *ACDClient) ListByName(parentID string, name string) (string, bool, int64, error) {

	url := client.MetadataURL + "nodes"

	if parentID == "" {
		url += "?filters=Kind:FOLDER+AND+isRoot:true"
	} else {
		url += "/" + parentID + "/children?filters=name:" + name
	}

	readCloser, _, err := client.call(url, "GET", 0, "")
	if err != nil {
		return "", false, 0, err
	}

	defer readCloser.Close()

	output := &ACDListEntriesOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return "", false, 0, err
	}

	if len(output.Entries) == 0 {
		return "", false, 0, nil
	}

	return output.Entries[0].ID, output.Entries[0].Kind == "FOLDER", output.Entries[0].Size, nil
}

func (client *ACDClient) DownloadFile(fileID string) (io.ReadCloser, int64, error) {

	url := client.ContentURL + "nodes/" + fileID + "/content"

	return client.call(url, "GET", 0, "")
}

func (client *ACDClient) UploadFile(parentID string, name string, content []byte, rateLimit int) (fileID string, err error) {

	url := client.ContentURL + "nodes?suppress=deduplication"

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	metadata := make(map[string]interface{})
	metadata["name"] = name
	metadata["kind"] = "FILE"
	metadata["parents"] = []string{parentID}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}

	err = writer.WriteField("metadata", string(metadataJSON))
	if err != nil {
		return "", err
	}

	part, err := writer.CreateFormFile("content", name)
	if err != nil {
		return "", err
	}

	_, err = part.Write(content)
	if err != nil {
		return "", err
	}

	writer.Close()

	var input interface{}
	input = body
	if rateLimit > 0 {
		input = CreateRateLimitedReader(body.Bytes(), rateLimit)
	}

	readCloser, _, err := client.call(url, "POST", input, writer.FormDataContentType())

	if err != nil {
		return "", err
	}

	defer readCloser.Close()

	entry := ACDEntry{}
	if err = json.NewDecoder(readCloser).Decode(&entry); err != nil {
		return "", err
	}

	return entry.ID, nil
}

func (client *ACDClient) DeleteFile(fileID string) error {

	url := client.MetadataURL + "trash/" + fileID

	readCloser, _, err := client.call(url, "PUT", 0, "")
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *ACDClient) MoveFile(fileID string, fromParentID string, toParentID string) error {

	url := client.MetadataURL + "nodes/" + toParentID + "/children"

	parameters := make(map[string]string)
	parameters["fromParent"] = fromParentID
	parameters["childId"] = fileID

	readCloser, _, err := client.call(url, "POST", parameters, "")
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *ACDClient) CreateDirectory(parentID string, name string) (string, error) {

	url := client.MetadataURL + "nodes"

	parameters := make(map[string]interface{})
	parameters["name"] = name
	parameters["kind"] = "FOLDER"
	parameters["parents"] = []string{parentID}

	readCloser, _, err := client.call(url, "POST", parameters, "")
	if err != nil {
		return "", err
	}

	defer readCloser.Close()

	entry := ACDEntry{}
	if err = json.NewDecoder(readCloser).Decode(&entry); err != nil {
		return "", err
	}

	return entry.ID, nil
}
