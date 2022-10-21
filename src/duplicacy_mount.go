package duplicacy

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cvilsmeier/sqinn-go/sqinn"
	lru "github.com/hashicorp/golang-lru"
	"github.com/winfsp/cgofuse/fuse"
)

type node_t struct {
	stat      fuse.Stat_t
	chld      map[string]*node_t
	opencnt   int
	chunkInfo *mountChunkInfo
}

type mountRevisionInfo struct {
	revision int
	snapshot *Snapshot
	inited   bool
	dbInited bool
	lock     sync.Mutex
}

type BackupFS struct {
	fuse.FileSystemBase
	revisions   map[string]*mountRevisionInfo
	manager     *BackupManager
	lock        sync.Mutex
	managerLock sync.Mutex
	ino         uint64
	inoLock     sync.Mutex
	root        *node_t
	openmap     map[uint64]*node_t
	chunkCache  *lru.TwoQueueCache
	tmpDir      string
	dbProcess   *sqinn.Sqinn
	dbLock      sync.Mutex
}

func (self *BackupFS) Open(path string, flags int) (errc int, fh uint64) {
	return self.openNode(path, false)
}

func (self *BackupFS) Opendir(path string) (errc int, fh uint64) {
	return self.openNode(path, true)
}

func (self *BackupFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	revision, err := self.initRevision(path)
	if err != nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "initRevision failed: %v", err)
		return -int(fuse.ENOENT)
	}

	if revision != nil && self.revisions[path] == nil {
		// get revision item
	} else {
		node := self.getNode(path, fh)
		if nil == node {
			return -int(fuse.ENOENT)
		}
		*stat = node.stat
	}

	return 0
}

func (self *BackupFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	node := self.getNode(path, fh)
	if nil == node {
		return fuse.ENOENT
	}

	if node.stat.Size == 0 {
		return 0
	}

	mountRevision, err := self.initRevision(path)
	if err != nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "initRevision failed: %v", err)
		return fuse.ENOENT
	}

	if node.chunkInfo == nil {
		return 0
	}

	snapshot := mountRevision.snapshot
	readBytes, err := self.readFileChunkCached(snapshot, node.chunkInfo, buff, ofst)
	if err != nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "error reading file %s: %v", path, err)
		return fuse.EIO
	}
	return readBytes
}

func (self *BackupFS) Readdir(
	path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64,
) (errc int) {
	// revision, err := self.initRevision(path)
	// if err != nil {
	// 	LOG_INFO("MOUNTING_FILESYSTEM", "error initing revision: %v", err)
	// 	return -int(fuse.ENOENT)
	// }

	// if revision != nil {
	// 	// populate revision
	// } else {
	self.lock.Lock()
	defer self.lock.Unlock()

	node := self.openmap[fh]
	fill(".", &node.stat, 0)
	fill("..", nil, 0)
	for name, chld := range node.chld {
		if !fill(name, &chld.stat, 0) {
			break
		}
	}
	// }
	return 0
}

func (self *BackupFS) getNode(path string, fh uint64) *node_t {
	self.lock.Lock()
	defer self.lock.Unlock()

	if ^uint64(0) == fh {
		_, _, node := self.lookupNode(path, nil)
		return node
	} else {
		return self.openmap[fh]
	}
}

func (self *BackupFS) openNode(path string, dir bool) (int, uint64) {
	self.lock.Lock()
	defer self.lock.Unlock()

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
	for _, c := range splitMountPath(path) {
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
	node = makeMountNode(self.nextIno(), mode, tmsp)

	prnt.chld[name] = node
	return node, 0
}

func (self *BackupFS) nextIno() uint64 {
	self.inoLock.Lock()
	defer self.inoLock.Unlock()
	self.ino++
	ino := self.ino
	return ino
}

func (self *BackupFS) initRoot() (err error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	if self.root != nil {
		return
	}

	self.openmap = map[uint64]*node_t{}

	self.managerLock.Lock()
	defer self.managerLock.Unlock()

	revisions, err := self.manager.SnapshotManager.ListSnapshotRevisions(self.manager.snapshotID)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "Failed to list all revisions for snapshot %s: %v", self.manager.snapshotID, err)
		return
	}

	LOG_DEBUG("MOUNTING_FILESYSTEM", "Creating root structure for %v revisions", len(revisions))

	alreadyCreated := make(map[string]bool)

	const DIR_MODE = fuse.S_IFDIR | 00555

	self.root = makeMountNode(self.nextIno(), DIR_MODE, fuse.Timespec{})
	self.revisions = make(map[string]*mountRevisionInfo)

	for _, revision := range revisions {
		snapshot := self.manager.SnapshotManager.DownloadSnapshot(self.manager.snapshotID, revision)

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
			date := time.Date(
				creationTime.Year(), creationTime.Month(), creationTime.Day(),
				0, 0, 0, 0, time.Local)
			self.makeNode(dayPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})
			alreadyCreated[dayPath] = true
		}

		dirname := fmt.Sprintf(
			"%02d%02d.%d",
			creationTime.Hour(), creationTime.Minute(), revision)
		dirPath := fmt.Sprintf("%s/%s", dayPath, dirname)
		date := time.Date(
			creationTime.Year(), creationTime.Month(), creationTime.Day(),
			creationTime.Hour(), creationTime.Minute(), 0, 0, time.Local)
		self.makeNode(dirPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})

		rootPath := fmt.Sprintf("%s/%s", dirPath, "browse")
		self.revisions[rootPath] = &mountRevisionInfo{
			revision: revision,
			snapshot: snapshot,
		}
		self.makeNode(rootPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})
	}

	return
}

func (self *BackupFS) withRevisionDb(revision *mountRevisionInfo, withDb func(*sqinn.Sqinn)) (err error) {
	self.dbLock.Lock()
	defer self.dbLock.Unlock()

	err = self.dbProcess.Open(filepath.Join(self.tmpDir, fmt.Sprintf("%d.db", revision.revision)))
	if err != nil {
		return
	}
	defer self.dbProcess.Close()

	if !revision.dbInited {
		_, err = self.dbProcess.ExecOne(`
			CREATE TABLE nodes (
				id INTEGER PRIMARY KEY NOT NULL,
				name VARCHAR(20),
				mode INTEGER,
				timestamp INTEGER,
				startChunk INTEGER,
				startOffset INTEGER,
				endChunk INTEGER,
				endOffset INTEGER
			);`)
		if err != nil {
			return
		}
		_, err = self.dbProcess.ExecOne(`CREATE UNIQUE INDEX name_idx ON nodes (name);`)
		if err != nil {
			return
		}
		revision.dbInited = true
	}

	withDb(self.dbProcess)
	return
}

func (self *BackupFS) initRevision(path string) (mountRevision *mountRevisionInfo, retErr error) {
	retErr = self.initRoot()
	if retErr != nil {
		return
	}

	components := splitMountPath(path)
	if len(components) < 6 {
		return
	}

	if components[5] != "browse" {
		return
	}

	revisionRoot := strings.Join(components[:6], "/")
	mountRevision, ok := self.revisions[revisionRoot]
	if !ok {
		retErr = errors.New(fmt.Sprintf("revision not found for root %v", revisionRoot))
		return
	}

	mountRevision.lock.Lock()
	defer mountRevision.lock.Unlock()

	if mountRevision.inited {
		return
	}

	LOG_DEBUG("MOUNTING_FILESYSTEM", "initRevision %d", mountRevision.revision)

	snapshot := mountRevision.snapshot
	if snapshot == nil {
		retErr = errors.New("snapshot revision not found")
		return
	}

	self.managerLock.Lock()
	defer self.managerLock.Unlock()

	if !self.manager.SnapshotManager.DownloadSnapshotSequences(snapshot) {
		retErr = errors.New("snapshot sequences download failed")
		return
	}

	retErr = self.withRevisionDb(mountRevision, func(db *sqinn.Sqinn) {
		_, err := db.ExecOne("BEGIN;")
		if err != nil {
			LOG_ERROR("MOUNTING_FILESYSTEM", "failed to start transaction")
			return
		}

		entriesAdded := 0
		snapshot.ListRemoteFiles(
			self.manager.config,
			self.manager.SnapshotManager.chunkOperator,
			func(entry *Entry) bool {
				name, err := nameOrHash(entry.Path)
				if err != nil {
					LOG_ERROR("MOUNTING_FILESYSTEM", "error hashing file name: %v", name)
					return false
				}

				query := `
				INSERT INTO 
					nodes (id, name, mode, timestamp, startChunk, startOffset, endChunk, endOffset)
					values (?, ?, ?, ?, ?, ?, ?, ?);
				`
				params := make([]interface{}, 8)
				params[0] = int64(self.nextIno())
				params[1] = name
				params[3] = entry.Time

				if entry.Mode&0o20000000000 == 0o20000000000 {
					params[2] = int(fuse.S_IFDIR | (entry.Mode & 00777))
					params[4] = 0
					params[5] = 0
					params[6] = 0
					params[7] = 0
				} else {
					params[2] = int(fuse.S_IFREG | (entry.Mode & 00777))
					params[4] = entry.StartChunk
					params[5] = entry.StartOffset
					params[6] = entry.EndChunk
					params[7] = entry.EndOffset
				}

				_, err = db.Exec(query, 1, 8, params)
				if err != nil {
					LOG_ERROR("MOUNTING_FILESYSTEM", "error creating entry for: %v, %v", entry.Path, err)
					return false
				}
				entriesAdded++
				return true
			})

		db.ExecOne("COMMIT;")

		if entriesAdded == 0 {
			retErr = errors.New("failed to init revision, no entries were added from the remote file list")
			return
		}

		LOG_DEBUG("MOUNTING_FILESYSTEM", "added %d entries to revision", entriesAdded)
		mountRevision.inited = true
	})

	return
}

func (self *BackupFS) downloadChunk(hash string) []byte {
	self.managerLock.Lock()
	defer self.managerLock.Unlock()

	chunk := self.manager.SnapshotManager.chunkOperator.Download(hash, 0, false)
	return chunk.GetBytes()
}

func (self *BackupFS) readFileChunkCached(snapshot *Snapshot, chunkInfo *mountChunkInfo, buff []byte, ofst int64) (read int, retErr error) {
	newbuff := new(bytes.Buffer)

	// need to fill supplied buffer as much as possible
	for newbuff.Len() < len(buff) {
		params, err := calculateChunkReadParams(snapshot.ChunkLengths, chunkInfo, ofst)
		if err != nil {
			retErr = err
			return
		}

		hash := snapshot.ChunkHashes[params.chunkIndex]

		chunkSize := params.end - params.start
		readLen := len(buff) - newbuff.Len()
		if readLen < chunkSize {
			params.end = params.start + readLen
		}

		var data []byte
		cacheData, ok := self.chunkCache.Get(hash)
		if ok {
			catCacheData, ok := cacheData.([]byte)
			if !ok {
				retErr = errors.New("got invalid data from cache")
				return
			}
			data = catCacheData
		} else {
			LOG_DEBUG("MOUNTING_FILESYSTEM", "downloading chunk %x", hash)
			data = self.downloadChunk(hash)
			self.chunkCache.Add(hash, data)
		}

		newbuff.Write(data[params.start:params.end])

		if params.chunkIndex == chunkInfo.EndChunk {
			break
		}

		ofst += int64(len(data[params.start:params.end]))
	}

	read = copy(buff, newbuff.Bytes())
	return
}

func (self *BackupFS) initMain() error {
	dir, err := ioutil.TempDir("", "duplicacy-mount")
	if err != nil {
		return err
	}
	self.tmpDir = dir
	LOG_DEBUG("MOUNTING_FILESYSTEM", "Using tempdir: %s", self.tmpDir)

	chunkCache, err := lru.New2Q(40)
	if err != nil {
		return err
	}
	self.chunkCache = chunkCache

	self.dbProcess, err = sqinn.Launch(sqinn.Options{})
	if err != nil {
		return err
	}

	self.manager.SnapshotManager.CreateChunkOperator(false, 3, false)

	return nil
}

func (self *BackupFS) cleanup() {
	self.dbProcess.Terminate()
	os.RemoveAll(self.tmpDir)
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
		if ofst < totalLen || file.EndChunk == params.chunkIndex {
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
			err = errors.New("file endoffset greater than chunk length")
			return
		}
	}

	params.start = int(lstart)
	if lstart > int64(params.end) {
		params.start = params.end
	}

	return
}

func makeMountNode(ino uint64, mode uint32, tmsp fuse.Timespec) *node_t {
	uid, gid, _ := fuse.Getcontext()

	node := node_t{
		stat: fuse.Stat_t{
			Ino:      ino,
			Uid:      uid,
			Gid:      gid,
			Mode:     mode,
			Nlink:    1,
			Mtim:     tmsp,
			Birthtim: tmsp,
		},
	}
	if fuse.S_IFDIR == node.stat.Mode&fuse.S_IFMT {
		node.chld = map[string]*node_t{}
	}
	return &node
}

func splitMountPath(path string) []string {
	return strings.Split(path, "/")
}

func nameOrHash(name string) (ret string, err error) {
	if len(name) <= 20 {
		ret = name
		return
	}

	hasher := sha1.New()
	_, err = io.WriteString(hasher, name)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "error hashing file name: %v", name)
		return
	}

	ret = string(hasher.Sum(nil))
	return
}

func MountFileSystem(fsPath string, manager *BackupManager) {
	LOG_INFO("MOUNTING_FILESYSTEM", "Mounting snapshot %s on %s", manager.snapshotID, fsPath)

	fs := BackupFS{
		manager: manager,
	}
	err := fs.initMain()
	if err != nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "Failed to init: %v", err)
		fs.cleanup()
		return
	}
	defer fs.cleanup()

	host := fuse.NewFileSystemHost(&fs)
	host.SetCapReaddirPlus(true)
	host.Mount(fsPath, nil)
	if !host.Unmount() {
		if runtime.GOOS != "windows" {
			_, err := exec.Command("fusermount", "-u", fsPath).Output()
			if err != nil {
				LOG_INFO("MOUNTING_FILESYSTEM", "Could not unmount (%v), please do it manually.", err)
			}
		}
	}

	return
}
