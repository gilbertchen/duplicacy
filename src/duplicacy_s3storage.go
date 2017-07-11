// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

type S3Storage struct {
    RateLimitedStorage

    client *s3.S3
    bucket string
    storageDir string
    numberOfThreads int
}

// CreateS3Storage creates a amazon s3 storage object.
func CreateS3Storage(regionName string, endpoint string, bucketName string, storageDir string,
                     accessKey string, secretKey string, threads int, isMinioCompatible bool) (storage *S3Storage, err error) {

    token := ""
    
    auth := credentials.NewStaticCredentials(accessKey, secretKey, token)

    if regionName == "" && endpoint == "" {
        defaultRegionConfig := &aws.Config {
            Region: aws.String("us-east-1"),
            Credentials: auth,
        }
        
        s3Client := s3.New(session.New(defaultRegionConfig))

        response, err := s3Client.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: aws.String(bucketName)})

        if err != nil {
            return nil, err
        }
    
        regionName = "us-east-1"
        if response.LocationConstraint != nil {
            regionName = *response.LocationConstraint
        }
    }
   
    config := &aws.Config {
        Region: aws.String(regionName),
        Credentials: auth,
        Endpoint: aws.String(endpoint),
        S3ForcePathStyle: aws.Bool(isMinioCompatible),
        DisableSSL: aws.Bool(isMinioCompatible),
    }    
    
    if len(storageDir) > 0 && storageDir[len(storageDir) - 1] != '/' {
        storageDir += "/"
    }

    storage = &S3Storage {
        client: s3.New(session.New(config)),
        bucket: bucketName,
        storageDir: storageDir,
        numberOfThreads: threads,
    }
    
    return storage, nil
}

// ListFiles return the list of files and subdirectories under 'dir' (non-recursively)
func (storage *S3Storage) ListFiles(threadIndex int, dir string) (files []string, sizes []int64, err error) {
    if len(dir) > 0 && dir[len(dir) - 1] != '/' {
        dir += "/"
    }

    if dir == "snapshots/" {
        dir = storage.storageDir + dir
        input := s3.ListObjectsInput {
            Bucket: aws.String(storage.bucket),
            Prefix: aws.String(dir),
            Delimiter: aws.String("/"),
            MaxKeys: aws.Int64(1000),
        }
        
        output, err := storage.client.ListObjects(&input)
        if err != nil {
            return nil, nil, err
        }
        
        for _, subDir := range output.CommonPrefixes {
            files = append(files, (*subDir.Prefix)[len(dir):])
        }
        return files, nil, nil
    } else {
        dir = storage.storageDir + dir
        marker := ""
        for {
            input := s3.ListObjectsInput {
                Bucket: aws.String(storage.bucket),
                Prefix: aws.String(dir),
                MaxKeys: aws.Int64(1000),
                Marker: aws.String(marker),
            }

            output, err := storage.client.ListObjects(&input)
            if err != nil {
                return nil, nil, err
            }

            for _, object := range output.Contents {
                files = append(files, (*object.Key)[len(dir):])
                sizes = append(sizes, *object.Size)
            }

            if !*output.IsTruncated {
                break
            }

            marker = *output.Contents[len(output.Contents) - 1].Key            
        }
        return files, sizes, nil
    }    

}

// DeleteFile deletes the file or directory at 'filePath'.
func (storage *S3Storage) DeleteFile(threadIndex int, filePath string) (err error) {
    input := &s3.DeleteObjectInput {
        Bucket: aws.String(storage.bucket),
        Key: aws.String(storage.storageDir + filePath),
    }
    _, err = storage.client.DeleteObject(input)
    return err
}

// MoveFile renames the file.
func (storage *S3Storage) MoveFile(threadIndex int, from string, to string) (err error) {

    input := &s3.CopyObjectInput {
        Bucket: aws.String(storage.bucket),
        CopySource: aws.String(storage.bucket + "/" + storage.storageDir + from),
        Key: aws.String(storage.storageDir + to),
    }

    _, err = storage.client.CopyObject(input)
    if err != nil {
        return err
    }
  
    return storage.DeleteFile(threadIndex, from)

}

// CreateDirectory creates a new directory.
func (storage *S3Storage) CreateDirectory(threadIndex int, dir string) (err error) {
    return nil
}

// GetFileInfo returns the information about the file or directory at 'filePath'.
func (storage *S3Storage) GetFileInfo(threadIndex int, filePath string) (exist bool, isDir bool, size int64, err error) {

    input := &s3.HeadObjectInput {
        Bucket: aws.String(storage.bucket),
        Key: aws.String(storage.storageDir + filePath),
    }

    output, err := storage.client.HeadObject(input)
    if err != nil {
        if e, ok := err.(awserr.RequestFailure); ok && (e.StatusCode() == 403 || e.StatusCode() == 404) {
            return false, false, 0, nil
        } else {
            return false, false, 0, err
        }    
    }
    
    if output == nil || output.ContentLength == nil {
        return false, false, 0, nil
    } else {
        return true, false, *output.ContentLength, nil
    }
}

// FindChunk finds the chunk with the specified id.  If 'isFossil' is true, it will search for chunk files with
// the suffix '.fsl'.
func (storage *S3Storage) FindChunk(threadIndex int, chunkID string, isFossil bool) (filePath string, exist bool, size int64, err error) {

    filePath = "chunks/" + chunkID
    if isFossil {
        filePath += ".fsl"
    }

    exist, _, size, err = storage.GetFileInfo(threadIndex, filePath)

    if err != nil {
        return "", false, 0, err
    } else {
        return filePath, exist, size, err
    }

}

// DownloadFile reads the file at 'filePath' into the chunk.
func (storage *S3Storage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) (err error) {

   input := &s3.GetObjectInput {
        Bucket: aws.String(storage.bucket),
        Key: aws.String(storage.storageDir + filePath),
    }
    
    output, err := storage.client.GetObject(input)
    if err != nil {
        return err
    }
    
    defer output.Body.Close()
    
    _, err = RateLimitedCopy(chunk, output.Body, storage.DownloadRateLimit / len(storage.bucket))
    return err

}

// UploadFile writes 'content' to the file at 'filePath'.
func (storage *S3Storage) UploadFile(threadIndex int, filePath string, content []byte) (err error) {

    input := &s3.PutObjectInput {
        Bucket: aws.String(storage.bucket),
        Key: aws.String(storage.storageDir + filePath),
        ACL: aws.String(s3.ObjectCannedACLPrivate),
        Body: CreateRateLimitedReader(content, storage.UploadRateLimit / len(storage.bucket)),
        ContentType: aws.String("application/duplicacy"),
    }
    
    _, err = storage.client.PutObject(input)
    return err
}

// If a local snapshot cache is needed for the storage to avoid downloading/uploading chunks too often when
// managing snapshots.
func (storage *S3Storage) IsCacheNeeded () (bool) { return true }

// If the 'MoveFile' method is implemented.
func (storage *S3Storage) IsMoveFileImplemented() (bool) { return true }

// If the storage can guarantee strong consistency.
func (storage *S3Storage) IsStrongConsistent() (bool) { return false }

// If the storage supports fast listing of files names.
func (storage *S3Storage) IsFastListing() (bool) { return true }

// Enable the test mode.
func (storage *S3Storage) EnableTestMode() {}
