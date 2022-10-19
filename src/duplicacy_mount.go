package duplicacy

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/winfsp/cgofuse/fuse"
)

type node_t struct {
	stat      fuse.Stat_t
	chld      map[string]*node_t
	opencnt   int
	chunkInfo *mountChunkInfo
}

type BackupFS struct {
	fuse.FileSystemBase
	snapshots       map[int]*Snapshot
	manager         *BackupManager
	lock            sync.Mutex
	ino             uint64
	root            *node_t
	openmap         map[uint64]*node_t
	initedRevisions map[int]bool
	chunkCache      *lru.TwoQueueCache
}

func (self *BackupFS) Open(path string, flags int) (errc int, fh uint64) {
	defer self.synchronize()()
	return self.openNode(path, false)
}

func (self *BackupFS) Opendir(path string) (errc int, fh uint64) {
	defer self.synchronize()()
	return self.openNode(path, true)
}

func (self *BackupFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	defer self.synchronize()()
	_, err := self.initRevision(path)
	if err != nil {
		return fuse.ENOENT
	}

	node := self.getNode(path, fh)
	if nil == node {
		return fuse.ENOENT
	}
	*stat = node.stat
	return 0
}

func (self *BackupFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	defer self.synchronize()()
	node := self.getNode(path, fh)
	if nil == node {
		return fuse.ENOENT
	}

	if node.stat.Size == 0 {
		return 0
	}

	revision, err := self.initRevision(path)
	if err != nil {
		return fuse.ENOENT
	}

	if node.chunkInfo == nil {
		return 0
	}

	snapshot := self.snapshots[revision]
	readBytes := self.readFileChunkCached(snapshot, node.chunkInfo, buff, ofst)
	LOG_INFO("MOUNTING_FILESYSTEM", "Read(%s, %d) -> %d", path, ofst, readBytes)
	return readBytes
}

func (self *BackupFS) Readdir(
	path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64,
) (errc int) {
	defer self.synchronize()()
	node := self.openmap[fh]
	fill(".", &node.stat, 0)
	fill("..", nil, 0)
	for name, chld := range node.chld {
		if !fill(name, &chld.stat, 0) {
			break
		}
	}
	return 0
}

func (self *BackupFS) synchronize() func() {
	self.lock.Lock()
	return func() {
		self.lock.Unlock()
	}
}

func (self *BackupFS) getNode(path string, fh uint64) *node_t {
	if ^uint64(0) == fh {
		_, _, node := self.lookupNode(path, nil)
		return node
	} else {
		return self.openmap[fh]
	}
}

func (self *BackupFS) openNode(path string, dir bool) (int, uint64) {
	_, _, node := self.lookupNode(path, nil)
	if nil == node {
		return fuse.ENOENT, ^uint64(0)
	}
	if !dir && fuse.S_IFDIR == node.stat.Mode&fuse.S_IFMT {
		return fuse.EISDIR, ^uint64(0)
	}
	if dir && fuse.S_IFDIR != node.stat.Mode&fuse.S_IFMT {
		return fuse.ENOTDIR, ^uint64(0)
	}
	node.opencnt++
	if 1 == node.opencnt {
		self.openmap[node.stat.Ino] = node
	}
	return 0, node.stat.Ino
}

func (self *BackupFS) lookupNode(path string, ancestor *node_t) (prnt *node_t, name string, node *node_t) {
	prnt = self.root
	name = ""
	node = self.root
	for _, c := range mountPathSplit(path) {
		if "" != c {
			prnt, name = node, c
			if node == nil {
				return
			}
			node = node.chld[c]
			if nil != ancestor && node == ancestor {
				name = "" // special case loop condition
				return
			}
		}
	}
	return
}

func (self *BackupFS) makeNode(path string, mode uint32, tmsp fuse.Timespec) (*node_t, int) {
	prnt, name, node := self.lookupNode(path, nil)
	if nil == prnt {
		return nil, fuse.ENOENT
	}
	if nil != node {
		return nil, fuse.EEXIST
	}
	self.ino++
	node = mountMakeNode(self.ino, mode, tmsp)
	prnt.chld[name] = node
	return node, 0
}

func (self *BackupFS) initRoot() error {
	if self.root != nil {
		return nil
	}

	self.openmap = map[uint64]*node_t{}

	revisions, err := self.manager.SnapshotManager.ListSnapshotRevisions(self.manager.snapshotID)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "Failed to list all revisions for snapshot %s: %v", self.manager.snapshotID, err)
		return errors.New("snapshot_revision_list_failed")
	}

	LOG_INFO("MOUNTING_FILESYSTEM", "Found %v revisions", len(revisions))

	alreadyCreated := make(map[string]bool)

	const DIR_MODE = fuse.S_IFDIR | 00555

	self.ino++
	self.root = mountMakeNode(self.ino, DIR_MODE, fuse.Timespec{})
	self.snapshots = make(map[int]*Snapshot)

	for _, revision := range revisions {
		snapshot := self.manager.SnapshotManager.DownloadSnapshot(self.manager.snapshotID, revision)
		self.snapshots[revision] = snapshot

		creationTime := time.Unix(snapshot.StartTime, 0)

		year := strconv.Itoa(creationTime.Year())
		yearPath := fmt.Sprintf("/%s", year)
		if !alreadyCreated[yearPath] {
			date := time.Date(creationTime.Year(), 1, 1, 0, 0, 0, 0, time.Local)
			self.makeNode(yearPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})
			alreadyCreated[yearPath] = true
		}

		month := fmt.Sprintf("%02d", int(creationTime.Month()))
		monthPath := fmt.Sprintf("%s/%s", yearPath, month)
		if !alreadyCreated[monthPath] {
			date := time.Date(creationTime.Year(), creationTime.Month(), 1, 0, 0, 0, 0, time.Local)
			self.makeNode(monthPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})
			alreadyCreated[monthPath] = true
		}

		day := fmt.Sprintf("%02d", creationTime.Day())
		dayPath := fmt.Sprintf("%s/%s", monthPath, day)
		if !alreadyCreated[dayPath] {
			date := time.Date(creationTime.Year(), creationTime.Month(), creationTime.Day(), 0, 0, 0, 0, time.Local)
			self.makeNode(dayPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})
			alreadyCreated[dayPath] = true
		}

		dirname := fmt.Sprintf(
			"%02d%02d.%d",
			creationTime.Hour(), creationTime.Minute(), revision)
		dirPath := fmt.Sprintf("%s/%s", dayPath, dirname)
		date := time.Date(creationTime.Year(), creationTime.Month(), creationTime.Day(), creationTime.Hour(), creationTime.Minute(), 0, 0, time.Local)
		self.makeNode(dirPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})
	}

	return nil
}

func (self *BackupFS) initRevision(path string) (ret int, err error) {
	initErr := self.initRoot()
	if initErr != nil {
		err = initErr
		return
	}

	if self.initedRevisions == nil {
		self.initedRevisions = map[int]bool{}
	}

	ret = 0

	components := mountPathSplit(path)
	if len(components) < 5 {
		return
	}

	dirComponents := strings.Split(components[4], ".")
	if len(dirComponents) != 2 {
		return
	}

	revision, err := strconv.Atoi(dirComponents[1])
	if err != nil {
		return
	}

	ret = revision

	if self.initedRevisions[revision] {
		return
	}

	LOG_INFO("MOUNTING_FILESYSTEM", "initRevision %d", revision)

	snapshot := self.snapshots[revision]
	if snapshot == nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "Snapshot revision not found")
		return
	}

	if !self.manager.SnapshotManager.DownloadSnapshotSequences(snapshot) {
		LOG_INFO("MOUNTING_FILESYSTEM", "Snapshot sequences download failed")
		return
	}

	root := strings.Join(components[:5], "/")

	snapshot.ListRemoteFiles(
		self.manager.config,
		self.manager.SnapshotManager.chunkOperator,
		func(entry *Entry) bool {
			if entry.Mode&0o20000000000 == 0o20000000000 {
				self.makeNode(fmt.Sprintf("%s/%s", root, entry.Path), fuse.S_IFDIR|(entry.Mode&00777), fuse.Timespec{Sec: entry.Time})
			} else {
				node, err := self.makeNode(fmt.Sprintf("%s/%s", root, entry.Path), entry.Mode, fuse.Timespec{Sec: entry.Time})
				if err == 0 {
					node.stat.Size = entry.Size
					node.chunkInfo = &mountChunkInfo{
						StartChunk:  entry.StartChunk,
						StartOffset: entry.StartOffset,
						EndChunk:    entry.EndChunk,
						EndOffset:   entry.EndOffset,
					}
				}
			}
			return true
		})

	self.initedRevisions[revision] = true

	return
}

func (self *BackupFS) readFileChunkCached(snapshot *Snapshot, chunkInfo *mountChunkInfo, buff []byte, ofst int64) int {
	self.manager.SnapshotManager.CreateChunkOperator(false, 1, false)

	params, err := calculateChunkReadParams(snapshot.ChunkLengths, chunkInfo, ofst)
	if err != nil {
		return -1
	}

	hash := snapshot.ChunkHashes[params.chunkIndex]

	chunkSize := params.end - params.start
	if len(buff) < chunkSize {
		params.end = params.start + len(buff)
	}

	cacheData, ok := self.chunkCache.Get(hash)
	if ok {
		data, ok := cacheData.([]byte)
		if !ok {
			return -1
		}
		return copy(buff, data[params.start:params.end])
	}

	LOG_INFO("MOUNTING_FILESYSTEM", "downloading chunk %s", hex.EncodeToString([]byte(hash)))
	chunk := self.manager.SnapshotManager.chunkOperator.Download(hash, 0, false)
	chunkData := chunk.GetBytes()
	self.chunkCache.Add(hash, chunkData)
	return copy(buff, chunkData[params.start:params.end])
}

type mountReadParams struct {
	chunkIndex int
	start      int
	end        int
}

type mountChunkInfo struct {
	StartChunk  int
	StartOffset int
	EndChunk    int
	EndOffset   int
}

func calculateChunkReadParams(chunkLengths []int, file *mountChunkInfo, ofst int64) (params mountReadParams, err error) {
	if ofst < 0 {
		err = errors.New("ofst cannot be negative")
		return
	}

	ofst += int64(file.StartOffset)
	lstart := ofst
	totalLen := int64(0)

	if len(chunkLengths) == 0 {
		err = errors.New("chunkLenghts cannot be empty")
		return
	}

	if len(chunkLengths) <= file.EndChunk {
		err = errors.New("chunkLenghts is not big enough")
		return
	}

	for params.chunkIndex = file.StartChunk; params.chunkIndex <= file.EndChunk; params.chunkIndex++ {
		chunkLen := int64(chunkLengths[params.chunkIndex])
		totalLen += chunkLen
		if ofst < totalLen {
			break
		}

		lstart -= chunkLen
	}

	if totalLen == 0 {
		err = errors.New("no data in chunks")
		return
	}

	params.end = chunkLengths[params.chunkIndex]
	if params.chunkIndex == file.EndChunk {
		params.end = file.EndOffset
		if params.end > chunkLengths[params.chunkIndex] {
			err = errors.New("no data in chunks")
		}
	}

	params.start = int(lstart)
	if lstart > int64(params.end) {
		params.start = params.end
	}

	return
}

func mountMakeNode(ino uint64, mode uint32, tmsp fuse.Timespec) *node_t {
	self := node_t{
		stat: fuse.Stat_t{
			Ino:      ino,
			Mode:     mode,
			Nlink:    1,
			Mtim:     tmsp,
			Birthtim: tmsp,
		},
	}
	if fuse.S_IFDIR == self.stat.Mode&fuse.S_IFMT {
		self.chld = map[string]*node_t{}
	}
	return &self
}

func mountPathSplit(path string) []string {
	return strings.Split(path, "/")
}

func MountFileSystem(fsPath string, manager *BackupManager) {
	LOG_INFO("MOUNTING_FILESYSTEM", "Mounting snapshot %s on %s", manager.snapshotID, fsPath)

	fs := BackupFS{
		manager: manager,
	}

	chunkCache, err := lru.New2Q(50)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "Failed to init cache: %v", err)
		return
	}
	fs.chunkCache = chunkCache

	host := fuse.NewFileSystemHost(&fs)
	host.SetCapReaddirPlus(true)
	host.Mount(fsPath, nil)

	return
}
