// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
    "encoding/json"
    "bytes"
    "os"
    "fmt"
    "hash"
    "runtime"
    "runtime/debug"
    "sync/atomic"
    "crypto/rand"
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"

    blake2 "github.com/minio/blake2b-simd"
)

// If encryption is turned off, use this key for HMAC-SHA256 or chunk ID generation etc.
var DEFAULT_KEY = []byte("duplicacy")

// The new default compression level is 100.  However, in the early versions we use the
// standard zlib levels of -1 to 9.
var DEFAULT_COMPRESSION_LEVEL = 100

type Config struct {
    CompressionLevel int                   `json:"compression-level"`
    AverageChunkSize int                   `json:"average-chunk-size"`
    MaximumChunkSize int                   `json:"max-chunk-size"`
    MinimumChunkSize int                   `json:"min-chunk-size"`

    ChunkSeed []byte                       `json:"chunk-seed"`

    // Use HMAC-SHA256(hashKey, plaintext) as the chunk hash.
    // Use HMAC-SHA256(idKey, chunk hash) as the file name of the chunk
    // For chunks, use HMAC-SHA256(chunkKey, chunk hash) as the encryption key
    // For files, use HMAC-SHA256(fileKey, file path) as the encryption key

    // the HMAC-SHA256 key of the chunk data
    HashKey  []byte                        `json:"-"`

    // used to generate an id from the chunk hash
    IDKey    []byte                        `json:"-"`

    // for encrypting a chunk
    ChunkKey []byte                        `json:"-"`

    // for encrypting a non-chunk file
    FileKey  []byte                        `json:"-"`

    chunkPool chan *Chunk                  `json:"-"`
    numberOfChunks int32
}

    // Create an alias to avoid recursive calls on Config.MarshalJSON
type aliasedConfig Config

type jsonableConfig struct {
    *aliasedConfig
    ChunkSeed string `json:"chunk-seed"`
    HashKey   string `json:"hash-key"`
    IDKey     string `json:"id-key"`
    ChunkKey  string `json:"chunk-key"`
    FileKey   string `json:"file-key"`
}

func (config *Config) MarshalJSON() ([] byte, error) {

    return json.Marshal(&jsonableConfig {
        aliasedConfig: (*aliasedConfig)(config),
        ChunkSeed:     hex.EncodeToString(config.ChunkSeed),
        HashKey:       hex.EncodeToString(config.HashKey),
        IDKey:         hex.EncodeToString(config.IDKey),
        ChunkKey:      hex.EncodeToString(config.ChunkKey),
        FileKey:       hex.EncodeToString(config.FileKey),
    })
}

func (config *Config) UnmarshalJSON(description []byte) (err error) {

    aliased := &jsonableConfig {
        aliasedConfig: (*aliasedConfig)(config),
    }

    if err = json.Unmarshal(description, &aliased); err != nil {
        return err
    }

    if config.ChunkSeed, err = hex.DecodeString(aliased.ChunkSeed); err != nil {
        return fmt.Errorf("Invalid representation of the chunk seed in the config")
    }
    if config.HashKey, err = hex.DecodeString(aliased.HashKey); err != nil {
        return fmt.Errorf("Invalid representation of the hash key in the config")
    }
    if config.IDKey, err = hex.DecodeString(aliased.IDKey); err != nil {
        return fmt.Errorf("Invalid representation of the id key in the config")
    }
    if config.ChunkKey, err = hex.DecodeString(aliased.ChunkKey); err != nil {
        return fmt.Errorf("Invalid representation of the chunk key in the config")
    }
    if config.FileKey, err = hex.DecodeString(aliased.FileKey); err != nil {
        return fmt.Errorf("Invalid representation of the file key in the config")
    }

    return nil
}

func (config *Config) IsCompatiableWith(otherConfig *Config) bool {

    return config.CompressionLevel == otherConfig.CompressionLevel &&
           config.AverageChunkSize == otherConfig.AverageChunkSize &&
           config.MaximumChunkSize == otherConfig.MaximumChunkSize &&
           config.MinimumChunkSize == otherConfig.MinimumChunkSize &&
           bytes.Equal(config.ChunkSeed, otherConfig.ChunkSeed)    &&
           bytes.Equal(config.HashKey, otherConfig.HashKey)
}

func (config *Config) Print() {

    LOG_INFO("CONFIG_INFO", "Compression level: %d", config.CompressionLevel)
    LOG_INFO("CONFIG_INFO", "Average chunk size: %d", config.AverageChunkSize)
    LOG_INFO("CONFIG_INFO", "Maximum chunk size: %d", config.MaximumChunkSize)
    LOG_INFO("CONFIG_INFO", "Minimum chunk size: %d", config.MinimumChunkSize)
    LOG_INFO("CONFIG_INFO", "Chunk seed: %x", config.ChunkSeed)
}

func CreateConfigFromParameters(compressionLevel int, averageChunkSize int, maximumChunkSize int, mininumChunkSize int,
                  isEncrypted bool, copyFrom *Config) (config *Config) {

    config = &Config {
        CompressionLevel: compressionLevel,
        AverageChunkSize: averageChunkSize,
        MaximumChunkSize: maximumChunkSize,
        MinimumChunkSize: mininumChunkSize,
    }

    if isEncrypted {
        // Randomly generate keys
        keys := make([]byte, 32 * 5)
        _, err := rand.Read(keys)
        if err != nil {
            LOG_ERROR("CONFIG_KEY", "Failed to generate random keys: %v", err)
            return nil
        }

        config.ChunkSeed = keys[:32]
        config.HashKey   = keys[32:64]
        config.IDKey     = keys[64:96]
        config.ChunkKey  = keys[96:128]
        config.FileKey   = keys[128:]
    } else {
        config.ChunkSeed = DEFAULT_KEY
        config.HashKey   = DEFAULT_KEY
        config.IDKey     = DEFAULT_KEY
    }

    if copyFrom != nil {
        config.CompressionLevel = copyFrom.CompressionLevel

        config.AverageChunkSize = copyFrom.AverageChunkSize
        config.MaximumChunkSize = copyFrom.MaximumChunkSize
        config.MinimumChunkSize = copyFrom.MinimumChunkSize

        config.ChunkSeed        = copyFrom.ChunkSeed
        config.HashKey          = copyFrom.HashKey
    }

    config.chunkPool = make(chan *Chunk, runtime.NumCPU() * 16)

    return config
}

func CreateConfig() (config *Config) {
    return &Config {
        HashKey: DEFAULT_KEY,
        IDKey: DEFAULT_KEY,
        CompressionLevel: DEFAULT_COMPRESSION_LEVEL,
        chunkPool: make(chan *Chunk, runtime.NumCPU() * 16),
    }
}

func (config *Config) GetChunk() (chunk *Chunk) {
    select {
    case chunk = <- config.chunkPool :
    default:
        numberOfChunks := atomic.AddInt32(&config.numberOfChunks, 1)
        if numberOfChunks >= int32(runtime.NumCPU() * 16) {
            LOG_WARN("CONFIG_CHUNK", "%d chunks have been allocated", numberOfChunks)
            if _, found := os.LookupEnv("DUPLICACY_CHUNK_DEBUG"); found {
                debug.PrintStack()
            }
        }
        chunk = CreateChunk(config, true)
    }
    return chunk
}

func (config *Config) PutChunk(chunk *Chunk){

    if chunk == nil {
        return
    }

    select {
    case config.chunkPool <- chunk:
    default:
        LOG_INFO("CHUNK_BUFFER", "Discarding a free chunk due to a full pool")
    }
}

func (config *Config) NewKeyedHasher(key []byte) hash.Hash {
    if config.CompressionLevel == DEFAULT_COMPRESSION_LEVEL {
        hasher, err := blake2.New(&blake2.Config{ Size: 32, Key:key })
        if err != nil {
            LOG_ERROR("HASH_KEY", "Invalid hash key: %x", key)
        }
        return hasher
    } else {
        return hmac.New(sha256.New, key)
    }
}

var SkipFileHash = false

func init() {
    if value, found := os.LookupEnv("DUPLICACY_SKIP_FILE_HASH"); found && value != "" && value != "0" {
        SkipFileHash = true
    }
}

// Implement a dummy hasher to be used when SkipFileHash is true.
type DummyHasher struct {
}

func (hasher *DummyHasher) Write(p []byte) (int, error) {
    return len(p), nil
}

func (hasher *DummyHasher) Sum(b []byte) []byte {
    return []byte("")
}

func (hasher *DummyHasher) Reset() {
}

func (hasher *DummyHasher) Size() int {
    return 0
}

func (hasher *DummyHasher) BlockSize() int {
    return 0
}

func (config *Config) NewFileHasher() hash.Hash {
    if SkipFileHash {
        return &DummyHasher {}
    } else if config.CompressionLevel == DEFAULT_COMPRESSION_LEVEL {
        hasher, _ := blake2.New(&blake2.Config{ Size: 32 })
        return hasher
    } else {
        return sha256.New()
    }
}

// Calculate the file hash using the corresponding hasher
func (config *Config) ComputeFileHash(path string, buffer []byte) string {

    file, err := os.Open(path)
      if err != nil {
        return ""
      }

    hasher := config.NewFileHasher()
    defer file.Close()

    count := 1
    for count > 0 {
        count, err = file.Read(buffer)
        hasher.Write(buffer[:count])
    }

      return hex.EncodeToString(hasher.Sum(nil))
}

// GetChunkIDFromHash creates a chunk id from the chunk hash.  The chunk id will be used as the name of the chunk
// file, so it is publicly exposed.  The chunk hash is the HMAC-SHA256 of what is contained in the chunk and should
// never be exposed.
func (config *Config) GetChunkIDFromHash(hash string) string {
    hasher := config.NewKeyedHasher(config.IDKey)
    hasher.Write([]byte(hash))
    return hex.EncodeToString(hasher.Sum(nil))
}

func DownloadConfig(storage Storage, password string) (config *Config, isEncrypted bool, err error) {
    // Although the default key is passed to the function call the key is not actually used since there is no need to
    // calculate the hash or id of the config file.
    configFile := CreateChunk(CreateConfig(), true)

    exist, _, _, err := storage.GetFileInfo(0, "config")
    if err != nil {
        return nil, false, err
    }

    if !exist {
        return nil, false, nil
    }

    err = storage.DownloadFile(0, "config", configFile)
    if err != nil {
        return nil, false, err
    }

    var masterKey []byte

    if len(password) > 0 {
        masterKey = GenerateKeyFromPassword(password)

        // Decrypt the config file.  masterKey == nil means no encryption.
        err = configFile.Decrypt(masterKey, "")
        if err != nil {
            return nil, false, fmt.Errorf("Failed to retrieve the config file: %v", err)
        }
    }

    config = CreateConfig()

    err = json.Unmarshal(configFile.GetBytes(), config)

    if err != nil {
        if bytes.Equal(configFile.GetBytes()[:9], []byte("duplicacy")) {
            return nil, true, fmt.Errorf("The storage is likely to have been initialized with a password before")
        } else {
            return nil, false, fmt.Errorf("Failed to parse the config file: %v", err)
        }
    }

    return config, false, nil

}

func UploadConfig(storage Storage, config *Config, password string) (bool) {

    // This is the key to encrypt the config file.
    var masterKey []byte

    if len(password) > 0 {

        if len(password) < 8 {
            LOG_ERROR("CONFIG_PASSWORD", "The password must be at least 8 characters")
            return false
        }

        masterKey = GenerateKeyFromPassword(password)
    }

    description, err := json.MarshalIndent(config, "", "    ")
    if err != nil {
        LOG_ERROR("CONFIG_MARSHAL", "Failed to marshal the config: %v", err)
        return false
    }

    // Although the default key is passed to the function call the key is not actually used since there is no need to
    // calculate the hash or id of the config file.
    chunk := CreateChunk(CreateConfig(), true)
    chunk.Write(description)

    if len(password) > 0 {
        // Encrypt the config file with masterKey.  If masterKey is nil then no encryption is performed.
        err = chunk.Encrypt(masterKey, "")

        if err != nil {
            LOG_ERROR("CONFIG_CREATE", "Failed to create the config file: %v", err)
            return false
        }
    }

    err = storage.UploadFile(0, "config", chunk.GetBytes())
    if err != nil {
        LOG_ERROR("CONFIG_INIT", "Failed to configure the storage: %v", err)
        return false
    }

    if IsTracing() {
        config.Print()
    }

    for _, subDir := range []string {"chunks", "snapshots"} {
        err = storage.CreateDirectory(0, subDir)
        if err != nil {
            LOG_ERROR("CONFIG_MKDIR", "Failed to create storage subdirectory: %v", err)
        }
    }

    return true
}

// ConfigStorage makes the general storage space available for storing duplicacy format snapshots.  In essence,
// it simply creates a file named 'config' that stores various parameters as well as a set of keys if encryption
// is enabled.
func ConfigStorage(storage Storage, compressionLevel int, averageChunkSize int, maximumChunkSize int,
                   minimumChunkSize int, password string, copyFrom *Config) bool {

    exist, _, _, err := storage.GetFileInfo(0, "config")
    if err != nil {
        LOG_ERROR("CONFIG_INIT", "Failed to check if there is an existing config file: %v", err)
        return false
    }

    if exist {
        LOG_INFO("CONFIG_EXIST", "The storage has already been configured")
        return false
    }


    config := CreateConfigFromParameters(compressionLevel, averageChunkSize, maximumChunkSize, minimumChunkSize, len(password) > 0,
                           copyFrom)
    if config == nil {
        return false
    }

    return UploadConfig(storage, config, password)
}
