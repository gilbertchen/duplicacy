// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
    "syscall"
    "unsafe"
    "io/ioutil"
    "encoding/json"
)

var keyringFile string

var (
    dllcrypt32  = syscall.NewLazyDLL("Crypt32.dll")
    dllkernel32 = syscall.NewLazyDLL("Kernel32.dll")

    procEncryptData = dllcrypt32.NewProc("CryptProtectData")
    procDecryptData = dllcrypt32.NewProc("CryptUnprotectData")
    procLocalFree   = dllkernel32.NewProc("LocalFree")
)

type DATA_BLOB struct {
    cbData uint32
    pbData *byte
}

func SetKeyringFile(path string) {
    keyringFile = path
}

func keyringEncrypt(value []byte) ([]byte, error) {

    dataIn := DATA_BLOB {
        pbData: &value[0],
        cbData: uint32(len(value)),
    }
    dataOut := DATA_BLOB {}

    r, _, err := procEncryptData.Call(uintptr(unsafe.Pointer(&dataIn)),
                                      0, 0, 0, 0, 0, uintptr(unsafe.Pointer(&dataOut)))
    if r == 0 {
        return nil, err
    }

    address := uintptr(unsafe.Pointer(dataOut.pbData))
    defer procLocalFree.Call(address)

    encryptedData := make([]byte, dataOut.cbData)
    for i := 0; i < len(encryptedData); i++ {
        encryptedData[i] = *(*byte)(unsafe.Pointer(uintptr(int(address) + i)))
    }
    return encryptedData, nil
}

func keyringDecrypt(value []byte) ([]byte, error) {

    dataIn := DATA_BLOB {
        pbData: &value[0],
        cbData: uint32(len(value)),
    }
    dataOut := DATA_BLOB {}

    r, _, err := procDecryptData.Call(uintptr(unsafe.Pointer(&dataIn)),
                                      0, 0, 0, 0, 0, uintptr(unsafe.Pointer(&dataOut)))
    if r == 0 {
        return nil, err
    }

    address := uintptr(unsafe.Pointer(dataOut.pbData))
    defer procLocalFree.Call(address)

    decryptedData := make([]byte, dataOut.cbData)
    for i := 0; i < len(decryptedData); i++ {
        address := int(uintptr(unsafe.Pointer(dataOut.pbData)))
        decryptedData[i] = *(*byte)(unsafe.Pointer(uintptr(int(address) + i)))
    }
    return decryptedData, nil
}

func keyringGet(key string) (value string) {
    if keyringFile == "" {
        LOG_DEBUG("KEYRING_NOT_INITIALIZED", "Keyring file not set")
        return ""
    }

    description, err := ioutil.ReadFile(keyringFile)
    if err != nil {
        LOG_DEBUG("KEYRING_READ", "Keyring file not read: %v", err)
        return ""
    }

    var keyring map[string][]byte
    err = json.Unmarshal(description, &keyring)
    if err != nil {
        LOG_DEBUG("KEYRING_PARSE", "Failed to parse the keyring storage file %s: %v", keyringFile, err)
        return ""
    }

    encryptedValue := keyring[key]

    if len(encryptedValue) == 0 {
        return ""
    }

    valueInBytes, err := keyringDecrypt(encryptedValue)
    if err != nil {
        LOG_DEBUG("KEYRING_DECRYPT", "Failed to decrypt the value: %v", err)
        return ""
    }

    return string(valueInBytes)
}

func keyringSet(key string, value string) bool {
    if value == "" {
        return false
    }
    if keyringFile == "" {
        LOG_DEBUG("KEYRING_NOT_INITIALIZED", "Keyring file not set")
        return false
    }

    keyring := make(map[string][]byte)

    description, err := ioutil.ReadFile(keyringFile)
    if err == nil {
        err = json.Unmarshal(description, &keyring)
        if err != nil {
            LOG_DEBUG("KEYRING_PARSE", "Failed to parse the keyring storage file %s: %v", keyringFile, err)
        }
    }

    if value == "" {
        keyring[key] = nil
    } else {
        encryptedValue, err := keyringEncrypt([]byte(value))
        if err != nil {
            LOG_DEBUG("KEYRING_ENCRYPT", "Failed to encrypt the value: %v", err)
            return false
        }
        keyring[key] = encryptedValue
    }

    description, err = json.MarshalIndent(keyring, "", "    ")
    if err != nil {
        LOG_DEBUG("KEYRING_MARSHAL", "Failed to marshal the keyring storage: %v", err)
        return false
    }

    err = ioutil.WriteFile(keyringFile, description, 0600)
    if err != nil {
        LOG_DEBUG("KEYRING_WRITE", "Failed to save the keyring storage to file %s: %v", keyringFile, err)
        return false
    }

    return true
}
