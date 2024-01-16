// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"context"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"strconv"
	"sync"
	"time"
	"path/filepath"

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

type OneDriveClient struct {
	HTTPClient *http.Client

	TokenFile string
	Token     *oauth2.Token
	OAConfig  *oauth2.Config
	TokenLock *sync.Mutex

	IsConnected bool
	TestMode    bool

	IsBusiness   bool
	MaxBatchReqs int

	RefreshTokenURL string
	APIURL string

	BatchURL string
	StripBatchPrefixURL string
}

func NewOneDriveClient(
	tokenFile string, 
	isBusiness bool, 
	max_batch_requests int, 
	client_id string, 
	client_secret string, 
	drive_id string,
) (*OneDriveClient, error) {

	description, err := ioutil.ReadFile(tokenFile)
	if err != nil {
		return nil, err
	}

	token := new(oauth2.Token)
	if err := json.Unmarshal(description, token); err != nil {
		return nil, err
	}

	client := &OneDriveClient{
		HTTPClient:   http.DefaultClient,
		TokenFile:    tokenFile,
		Token:        token,
		OAConfig:	  nil,
		TokenLock:    &sync.Mutex{},
		IsBusiness:   isBusiness,
		MaxBatchReqs: max_batch_requests,
	}

	if (client_id != "") {
	    oneOauthConfig := oauth2.Config{
	        ClientID: 		client_id,
	        ClientSecret: 	client_secret,
	        Scopes:       	[]string{"Files.ReadWrite", "offline_access"},
	        Endpoint: oauth2.Endpoint{
	            AuthURL:  "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
	            TokenURL: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
	        },
	    }

		client.OAConfig = &oneOauthConfig
	}

	if isBusiness {
		client.RefreshTokenURL = "https://duplicacy.com/odb_refresh"
		client.APIURL = "https://graph.microsoft.com/v1.0/me/drive"
		if drive_id != "" {
			client.APIURL = "https://graph.microsoft.com/v1.0/drives/"+drive_id
		}
		client.BatchURL = "https://graph.microsoft.com/v1.0/$batch"
		client.StripBatchPrefixURL = "https://graph.microsoft.com/v1.0/"
	} else {
		client.RefreshTokenURL = "https://duplicacy.com/one_refresh"
		client.APIURL = "https://api.onedrive.com/v1.0/drive"
		client.BatchURL = "<Batching is only supported by Graph API>"
	}

	client.RefreshToken(false)

	return client, nil
}

func (client *OneDriveClient) call(url string, method string, input interface{}, contentType string) (io.ReadCloser, int64, error) {

	var response *http.Response

	backoff := 1
	for i := 0; i < 12; i++ {

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
			request.Header.Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", reader.Length() - 1, reader.Length()))
		}

		if url != client.RefreshTokenURL {
			client.TokenLock.Lock()
			request.Header.Set("Authorization", "Bearer "+client.Token.AccessToken)
			client.TokenLock.Unlock()
		}
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}

		request.Header.Set("User-Agent", "ISV|Acrosync|Duplicacy/2.0")

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
				if backoff > 256 {
					backoff = 256
				}
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

			if url == client.RefreshTokenURL {
				return nil, 0, OneDriveError{Status: response.StatusCode, Message: "Authorization error when refreshing token"}
			}

			err = client.RefreshToken(true)
			if err != nil {
				return nil, 0, err
			}
			continue
		} else if response.StatusCode == 409 {
			return nil, 0, OneDriveError{Status: response.StatusCode, Message: "Conflict"}
		} else if response.StatusCode > 401 && response.StatusCode != 404 {
			delay := int((rand.Float32() * 0.5 + 0.5) * 1000.0 * float32(backoff))
			if backoffList, found := response.Header["Retry-After"]; found && len(backoffList) > 0 {
				retryAfter, _ := strconv.Atoi(backoffList[0])
				if retryAfter * 1000 > delay {
					delay = retryAfter * 1000
				}
			}

			LOG_INFO("ONEDRIVE_RETRY", "Response code: %d; retry after %d milliseconds", response.StatusCode, delay)
			time.Sleep(time.Duration(delay) * time.Millisecond)
			backoff *= 2
			if backoff > 256 {
				backoff = 256
			}
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

	if (client.OAConfig == nil) {
		readCloser, _, err := client.call(client.RefreshTokenURL, "POST", client.Token, "")
		if err != nil {
			return fmt.Errorf("failed to refresh the access token: %v", err)
		}

		defer readCloser.Close()

		if err = json.NewDecoder(readCloser).Decode(client.Token); err != nil {
			return err
		}
	} else {
		ctx := context.Background()
		tokenSource := client.OAConfig.TokenSource(ctx, client.Token)
	    token, err := tokenSource.Token()
	    if err != nil {
	        return fmt.Errorf("failed to refresh the access token: %v", err)
	    }
	    client.Token = token
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


type OneDriveListReqItem struct {
	Path string
	URL  string
}

type OneDriveEntry struct {
	ID     string
	Name   string
	Folder map[string]interface{}
	Size   int64
}

type ErrorResponse struct {
	Code     string	`json:"code"`
	Message  string `json:"message"`
}

type OneDriveListEntriesOutput struct {
	Entries  []OneDriveEntry `json:"value"`
	NextLink string          `json:"@odata.nextLink"`
	Error	 ErrorResponse	 `json:"error"`
}

func (client *OneDriveClient) ListEntries(path string) ([]OneDriveEntry, error) {

	entries := []OneDriveEntry{}

	url := client.APIURL + "/root:/" + path + ":/children"
	if path == "" {
		url = client.APIURL + "/root/children"
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

func (client *OneDriveClient) ListPathToURL(path string) (url string) {

	url = client.APIURL + "/root:/" + path + ":/children"
	if path == "" {
		url = client.APIURL + "/root/children"
	}
	if client.TestMode {
		url += "?top=8"
	} else {
		url += "?top=1000"
	}
	url += "&select=name,size,folder"

	return url
}


type BatchRequestItem struct {
	Id 		string	`json:"id"`
	Method	string	`json:"method"`
	URL		string	`json:"url"`
}

type BatchRequest struct {
	Requests	[]BatchRequestItem `json:"requests"`
}

type BatchResponseItem struct {
	Id 		string						`json:"id"`
	Status	int							`json:"status"`
	Headers	map[string]string			`json:"headers"`
	Body	OneDriveListEntriesOutput	`json:"body"`
}

type BatchResponse struct {
	Responses []BatchResponseItem `json:"responses"`
}


func (client *OneDriveClient) ListEntriesBatch(
	prefix string, 
	batchReqs []OneDriveListReqItem,
) (
	entriesPerReq []OneDriveListEntriesOutput, 
	newReqs []OneDriveListReqItem,
	err error,
) {
	nReqs := len(batchReqs)

	entriesPerReq = make([]OneDriveListEntriesOutput, nReqs, nReqs)
	newReqs = make([]OneDriveListReqItem, 0, 0)

	for i, req := range batchReqs {
		if req.URL == "" {
			batchReqs[i].URL = client.ListPathToURL(prefix + "/" + batchReqs[i].Path)
		}
	}

	if client.IsBusiness && nReqs > 1 {
		// OneDrive Business uses Graph API which supports batching
	    breq := BatchRequest{}
	    breq.Requests = make([]BatchRequestItem, len(batchReqs), len(batchReqs))
	    for i, req := range batchReqs {
		    breq.Requests[i].Id = strconv.Itoa(i+1)
		    breq.Requests[i].Method = "GET"
		    breq.Requests[i].URL = req.URL[len(client.StripBatchPrefixURL):]
	    }
		tracestr := fmt.Sprintf("Batch payload: %d requests", len(breq.Requests))
		for _, req := range breq.Requests {
			tracestr = tracestr + fmt.Sprintf("\n\t\t%s %s", req.Method, req.URL)
		}
		LOG_TRACE("ONEDRIVE_BATCH", tracestr)

		readCloser, _, err := client.call(client.BatchURL, "POST", breq, "application/json")
		if err != nil {
			return nil, nil, err
		}

		defer readCloser.Close()
	
		bresp := &BatchResponse{}

		if err = json.NewDecoder(readCloser).Decode(&bresp); err != nil {
			return nil, nil, err
		}
		if len(bresp.Responses) != len(batchReqs) {
			return nil, nil, fmt.Errorf("Batch response length mismatch with requests length: %d != %d", len(bresp.Responses), len(batchReqs))
		}
		throttleDelay := 0

	    for _, resp := range bresp.Responses {
		    nresp, err := strconv.Atoi(resp.Id)
			if err != nil {
				return nil, nil, err
			}
			nresp-- // ids are 1-based in the response payload

			if resp.Status == 200 { // success
				entriesPerReq[nresp] = resp.Body
				if entriesPerReq[nresp].NextLink != "" {
					newReqs = append(
						newReqs, 
						OneDriveListReqItem{
							Path: batchReqs[nresp].Path, 
							URL:  entriesPerReq[nresp].NextLink,
						},
					)
				}
			} else if resp.Status == 429 { // throttled
				var backoff int
				backoffList, found := resp.Headers["Retry-After"]
				if found && len(backoffList)>0 {
					backoff, _ = strconv.Atoi(backoffList)
					backoff *= 1000 // s to ms
				} else {
					backoff = 300000 // 5 minutes by default
				}
				if backoff > throttleDelay {
					throttleDelay = backoff
				}
				LOG_INFO("ONEDRIVE_RETRY", "Batch item response code: %d; suggested retry is %d milliseconds", resp.Status, backoff)
				// Retry the same URL
				newReqs = append(newReqs, batchReqs[nresp])
			} else if resp.Status == 400 || resp.Status == 401 { 
				// Some errors are expected, e.g. unauthorized / expired token
				// Retry the same URL
				newReqs = append(newReqs, batchReqs[nresp])
			} else { // unexpected error
				errmsg := resp.Body.Error.Message
				//LOG_TRACE("ONEDRIVE_BATCH", "Unexpected batch response error %d: %s / %s", resp.Status, http.StatusText(resp.Status), errmsg)
				return nil, nil, fmt.Errorf("Unexpected batch response error %d: %s / %s", resp.Status, http.StatusText(resp.Status), errmsg)
			}
	    }

   		if throttleDelay > 0 {
			LOG_INFO("ONEDRIVE_RETRY", "Batch request throttled; retry after %d milliseconds", throttleDelay)
			time.Sleep(time.Duration(throttleDelay) * time.Millisecond)
			throttleDelay = 0
		}

	} else {
		// Run without batching
		for i, req := range batchReqs {
			readCloser, _, err := client.call(req.URL, "GET", 0, "")
			if err != nil {
				return nil, nil, err
			}

			defer readCloser.Close()

			if err = json.NewDecoder(readCloser).Decode(&entriesPerReq[i]); err != nil {
				return nil, nil, err
			}
			if entriesPerReq[i].NextLink != "" {
				newReqs = append(
					newReqs, 
					OneDriveListReqItem{
						Path: batchReqs[i].Path, 
						URL:  entriesPerReq[i].NextLink,
					},
				)
			}
		}
	}

	return entriesPerReq, newReqs, nil
}

func (client *OneDriveClient) GetFileInfo(path string) (string, bool, int64, error) {

	url := client.APIURL + "/root:/" + path
	if path == "" { url = client.APIURL + "/root" }
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

	url := client.APIURL + "/items/root:/" + path + ":/content"

	return client.call(url, "GET", 0, "")
}

func (client *OneDriveClient) UploadFile(path string, content []byte, rateLimit int) (err error) {

	// Upload file using the simple method; this is only possible for OneDrive Personal or if the file
	// is smaller than 4MB for OneDrive Business
	if !client.IsBusiness || (client.TestMode && rand.Int() % 2 == 0) {
		url := client.APIURL + "/root:/" + path + ":/content"

		readCloser, _, err := client.call(url, "PUT", CreateRateLimitedReader(content, rateLimit), "application/octet-stream")
		if err != nil {
			return err
		}

		readCloser.Close()
		return nil
	}

	// For large files, create an upload session first
	uploadURL, err := client.CreateUploadSession(path)
	if err != nil {
		return err
	}

	return client.UploadFileSession(uploadURL, content, rateLimit)
}

func (client *OneDriveClient) CreateUploadSession(path string) (uploadURL string, err error) {

	type CreateUploadSessionItem struct {
		ConflictBehavior string `json:"@microsoft.graph.conflictBehavior"`
		Name string `json:"name"`
	}

	input := map[string]interface{} {
		"item": CreateUploadSessionItem {
			ConflictBehavior: "replace",
			Name: filepath.Base(path),
		},
	}

	readCloser, _, err := client.call(client.APIURL + "/root:/" + path + ":/createUploadSession", "POST", input, "application/json")
	if err != nil {
		return "", err
	}

	type CreateUploadSessionOutput struct {
		UploadURL string `json:"uploadUrl"`
	}

	output := &CreateUploadSessionOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return "", err
	}

	readCloser.Close()
	return output.UploadURL, nil
}

func (client *OneDriveClient) UploadFileSession(uploadURL string, content []byte, rateLimit int) (err error) {

	readCloser, _, err := client.call(uploadURL, "PUT", CreateRateLimitedReader(content, rateLimit), "")
	if err != nil {
		return err
	}
	type UploadFileSessionOutput struct {
		Size int `json:"size"`
	}
	output := &UploadFileSessionOutput{}

	if err = json.NewDecoder(readCloser).Decode(&output); err != nil {
		return fmt.Errorf("Failed to complete the file upload session: %v", err)
	}

	if output.Size != len(content) {
		return fmt.Errorf("Uploaded %d bytes out of %d bytes", output.Size, len(content))
	}

	readCloser.Close()
	return nil
}

func (client *OneDriveClient) DeleteFile(path string) error {

	url := client.APIURL + "/root:/" + path

	readCloser, _, err := client.call(url, "DELETE", 0, "")
	if err != nil {
		return err
	}

	readCloser.Close()
	return nil
}

func (client *OneDriveClient) MoveFile(path string, parent string) error {

	url := client.APIURL + "/root:/" + path

	parentReference := make(map[string]string)
	parentReference["path"] = "/root:/" + parent

	parameters := make(map[string]interface{})
	parameters["parentReference"] = parentReference

	readCloser, _, err := client.call(url, "PATCH", parameters, "application/json")
	if err != nil {
		if e, ok := err.(OneDriveError); ok && e.Status == 400 {
			// The destination directory doesn't exist; trying to create it...
			dir := filepath.Dir(parent)
			if dir == "." {
				dir = ""
			}
			client.CreateDirectory(dir, filepath.Base(parent))
			readCloser, _, err = client.call(url, "PATCH", parameters, "application/json")
			if err != nil {
				return nil
			}

		}

		return err
	}

	readCloser.Close()
	return nil
}

func (client *OneDriveClient) CreateDirectory(path string, name string) error {

	url := client.APIURL + "/root/children"

	if path != "" {

		pathID, isDir, _, err := client.GetFileInfo(path)
		if err != nil {
			return err
		}

		if pathID == "" {
			dir := filepath.Dir(path)
			if dir != "." {
				// The parent directory doesn't exist; trying to create it...
				client.CreateDirectory(dir, filepath.Base(path))
				isDir = true
			}
		}

		if !isDir {
			return fmt.Errorf("The path '%s' is not a directory", path)
		}

		url = client.APIURL + "/root:/" + path + ":/children"
	}

	parameters := make(map[string]interface{})
	parameters["name"] = name
	parameters["folder"] = make(map[string]int)

	readCloser, _, err := client.call(url, "POST", parameters, "application/json")
	if err != nil {
		if e, ok := err.(OneDriveError); ok && e.Status == 409 {
			// This error usually means the directory already exists
			LOG_TRACE("ONEDRIVE_MKDIR", "The directory '%s/%s' already exists", path, name)
			return nil
		}
		return err
	}

	readCloser.Close()
	return nil
}
