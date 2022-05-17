package duplicacy

import (
	"bytes"
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

type BackupFS struct {
	pathfs.FileSystem
	startingFolder *BackupFolder
	ourSnapshot    *Snapshot
	ourManager     *BackupManager
}

type BackupFolder struct {
	name       string
	fullPath   string
	ourFiles   map[string]*BackupFile
	ourFolders map[string]*BackupFolder
	inodeID    uint64
}

type BackupFile struct {
	name     string
	fullPath string
	inodeID  uint64
}

func (ourFS *BackupFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0755}, fuse.OK
	}

	returnPath, _, _, isFolder := pathfindFolder(ourFS.startingFolder, name)

	if returnPath == "" {
		return nil, fuse.ENOENT
	}

	var ourFile *Entry

	for _, file := range ourFS.ourSnapshot.Files {
		file_name := file.Path
		if string(file_name[len(file_name)-1]) == "/" {
			file_name = file_name[:len(file_name)-1]
		}
		if name == file_name {
			ourFile = file
			break
		}
	}

	if ourFile == nil {
		return nil, fuse.ENOENT
	}

	if isFolder {
		return &fuse.Attr{Mode: fuse.S_IFDIR | 0755, Mtime: uint64(ourFile.Time), Ctime: uint64(ourFile.Time)}, fuse.OK
	}

	return &fuse.Attr{Mode: fuse.S_IFREG | 0644, Size: uint64(ourFile.Size), Mtime: uint64(ourFile.Time), Ctime: uint64(ourFile.Time)}, fuse.OK
}

func (ourFS *BackupFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {

	r := []fuse.DirEntry{}
	ourFolder := ourFS.startingFolder
	if name != "" {
		_, parentFolder, ourName, _ := pathfindFolder(ourFS.startingFolder, name)
		ourFolder = parentFolder.ourFolders[ourName]
	}
	for _, returnFile := range ourFolder.ourFiles {
		r = append(r, fuse.DirEntry{Mode: fuse.S_IFREG | 0644,
			Name: returnFile.name,
			Ino:  returnFile.inodeID})
	}

	for _, returnFolder := range ourFolder.ourFolders {
		r = append(r, fuse.DirEntry{Mode: fuse.S_IFDIR | 0755,
			Name: returnFolder.name,
			Ino:  returnFolder.inodeID})
	}

	return r, fuse.OK
}

func (ourFS *BackupFS) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	if name == "" {
		return nil, fuse.ENOENT
	}
	//splitPath := strings.Split(name, "/")
	//ourName := splitPath[len(splitPath)-1]
	var ourFile *Entry

	for _, file := range ourFS.ourSnapshot.Files {
		if name == file.Path {
			ourFile = file
			break
		}
	}

	if ourFile == nil {
		return nil, fuse.ENOENT
	}

	chunkDownloader := CreateChunkDownloader(ourFS.ourManager.config, ourFS.ourManager.storage, nil, false, 1, false)
	chunkDownloader.AddFiles(ourFS.ourSnapshot, []*Entry{ourFile})
	for i := ourFile.StartChunk; i <= ourFile.EndChunk; i++ {
		chunkDownloader.taskList[i].needed = true
	}

	chunkDownloader.Prefetch(ourFile)

	var buffer bytes.Buffer

	for i := ourFile.StartChunk; i <= ourFile.EndChunk; i++ {
		chunk := chunkDownloader.WaitForChunk(i)
		buffer.Write(chunk.GetBytes())
	}
	f := nodefs.NewDataFile(buffer.Bytes())
	return &nodefs.WithFlags{
		File:      f,
		FuseFlags: fuse.FOPEN_NONSEEKABLE,
	}, fuse.OK
}

func pathfindFolder(defaultFolder *BackupFolder, ourPath string) (returnPath string, returnBackupFolder *BackupFolder, returnName string, returnFolder bool) {
	isFolder := false
	if string(ourPath[len(ourPath)-1]) == "/" {
		ourPath = ourPath[:len(ourPath)-1]
		isFolder = true
	}
	splitPath := strings.Split(ourPath, "/")
	ourName := splitPath[len(splitPath)-1]
	cutPath := splitPath[:len(splitPath)-1]
	var ourFolder *BackupFolder = defaultFolder
	for _, pathPart := range cutPath {
		ourFolder = ourFolder.ourFolders[pathPart]
	}
	if _, ok := ourFolder.ourFolders[ourName]; ok {
		isFolder = true
	} else if _, ok := ourFolder.ourFiles[ourName]; !ok {
		ourPath = ""
	}
	return ourPath, ourFolder, ourName, isFolder
}

func MountFileSystem(fsPath string, revision int, manager *BackupManager) (workdir string, cleanup func()) {
	remoteSnapshot := manager.SnapshotManager.DownloadSnapshot(manager.snapshotID, revision)
	manager.SnapshotManager.DownloadSnapshotContents(remoteSnapshot, []string{}, true)

	defaultFolder := &BackupFolder{name: "", fullPath: "", ourFolders: make(map[string]*BackupFolder), ourFiles: make(map[string]*BackupFile)}

	for i, file := range remoteSnapshot.Files {
		ourPath, ourFolder, ourName, isFolder := pathfindFolder(defaultFolder, string(file.Path))

		if isFolder {
			ourFolder.ourFolders[ourName] = &BackupFolder{name: ourName, fullPath: ourPath, ourFolders: make(map[string]*BackupFolder), ourFiles: make(map[string]*BackupFile), inodeID: uint64(i)}
		} else {
			ourFolder.ourFiles[ourName] = &BackupFile{name: ourName, fullPath: ourPath, inodeID: uint64(i)}
		}
	}

	//Create and serve a new DefaultFileSystem
	fs := &BackupFS{
		FileSystem:     pathfs.NewDefaultFileSystem(),
		startingFolder: defaultFolder,
		ourSnapshot:    remoteSnapshot,
		ourManager:     manager,
	}
	pathFS := pathfs.NewPathNodeFs(fs, nil)
	opts := nodefs.NewOptions()

	state, _, err := nodefs.MountRoot(fsPath, pathFS.Root(), opts)
	if err != nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "MountNodeFileSystem failed: %v", err)
	}
	state.Serve()
	/*if err := state.WaitMount(); err != nil {

		LOG_INFO("MOUNTING_FILESYSTEM", "WaitMount failed: %v", err)
	}*/
	return fsPath, func() {
		state.Unmount()
	}
}
