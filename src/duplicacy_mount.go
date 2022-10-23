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
	stat    fuse.Stat_t
	chld    map[string]*node_t
	opencnt int
}

type backupMountFile struct {
	stat      fuse.Stat_t
	chunkInfo mountChunkInfo
}

type mountEntryList = map[string]map[string]*backupMountFile

type mountRevisionInfo struct {
	revision     int
	snapshot     *Snapshot
	inited       bool
	dbInited     bool
	lock         sync.Mutex
	root         string
	entriesCache *mountEntryList
	diskCache    bool
}

func (self *mountRevisionInfo) getPathParts(path string) (parent string, name string) {
	components := splitMountPath(path[len(self.root):])
	componentsLen := len(components)

	if componentsLen == 1 {
		return components[0], ""
	}

	if componentsLen == 1 {
		parent = ""
		name = components[0]
	} else {
		parent = strings.Trim(strings.Join(components[:componentsLen-1], "/"), "/")
		name = components[componentsLen-1]
	}

	return
}

func (self *mountRevisionInfo) addEntries(
	backupFs *BackupFS, entriesToAdd *mountEntryList,
) (err error) {
	if self.diskCache {
		err = backupFs.withRevisionDb(self, func(db *sqinn.Sqinn) (err error) {
			_, err = db.ExecOne("BEGIN;")
			if err != nil {
				err = errors.New("failed to start transaction")
				return
			}

			for parent, entries := range *entriesToAdd {
				for name, entry := range entries {
					query := `
					INSERT INTO
						nodes (id, parent, name, mode, timestamp, startChunk, startOffset, endChunk, endOffset, size)
						values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
					`
					params := make([]interface{}, 10)
					params[0] = int64(entry.stat.Ino)
					params[1] = parent
					params[2] = name
					params[3] = int64(entry.stat.Mode)
					params[4] = entry.stat.Mtim.Sec
					params[5] = entry.chunkInfo.StartChunk
					params[6] = entry.chunkInfo.StartOffset
					params[7] = entry.chunkInfo.EndChunk
					params[8] = entry.chunkInfo.EndOffset
					params[9] = entry.stat.Size

					_, err = db.Exec(query, 1, 10, params)
					if err != nil {
						err = errors.New(fmt.Sprintf("error creating entry for: %s/%s, %v", parent, name, err))
						return
					}
				}
			}

			db.ExecOne("COMMIT;")
			return
		})
		if err != nil {
			return
		}
	} else {
		self.entriesCache = entriesToAdd
	}

	return
}

func (self *mountRevisionInfo) readDir(
	backupFs *BackupFS, path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	fh uint64,
) (err error) {
	var parent string
	mparent, name := self.getPathParts(path)

	if self.root == path {
		// loading browse dir
		node := backupFs.openmap[fh]
		fill(".", &node.stat, 0)
		fill("..", nil, 0)
		parent = ""
	} else {
		if mparent == "" {
			parent, err = nameOrHash(name)
		} else {
			parent, err = nameOrHash(fmt.Sprintf("%s/%s", mparent, name))
		}

		if err != nil {
			return
		}
	}

	if self.diskCache {
		err = backupFs.withRevisionDb(self, func(db *sqinn.Sqinn) (err error) {
			params := make([]interface{}, 1)
			params[0] = parent
			res, err := db.Query(
				"SELECT id, name, mode, timestamp, size FROM nodes WHERE parent = ?;",
				params,
				[]byte{sqinn.ValInt64, sqinn.ValText, sqinn.ValInt64, sqinn.ValInt64, sqinn.ValInt64})

			if err != nil {
				return
			}

			uid, gid, _ := fuse.Getcontext()
			const INO, NAME, MODE, TIMESTAMP, SIZE = 0, 1, 2, 3, 4
			for _, row := range res {
				stat := fuse.Stat_t{
					Ino:      uint64(row.Values[INO].AsInt64()),
					Size:     row.Values[SIZE].AsInt64(),
					Mode:     uint32(row.Values[MODE].AsInt64()),
					Mtim:     fuse.Timespec{Sec: row.Values[TIMESTAMP].AsInt64()},
					Birthtim: fuse.Timespec{Sec: row.Values[TIMESTAMP].AsInt64()},
					Nlink:    1,
					Uid:      uid,
					Gid:      gid,
				}
				name := row.Values[NAME].AsString()
				LOG_DEBUG("MOUNTING_FILESYSTEM", "Populating dir with %s: %v", name, stat)
				if !fill(name, &stat, 0) {
					err = errors.New("error filling entry")
					break
				}
			}

			return
		})
	} else {
		entryParent, ok := (*self.entriesCache)[parent]
		if !ok {
			err = errors.New("parent not found")
			return
		}
		for name, entry := range entryParent {
			LOG_DEBUG("MOUNTING_FILESYSTEM", "Populating dir with %s: %v", name, entry.stat)
			if !fill(name, &entry.stat, 0) {
				err = errors.New("error filling entry")
				break
			}
		}
	}

	return
}

func (self *mountRevisionInfo) getAttr(backupFs *BackupFS, path string) (backupFile *backupMountFile, err error) {
	oParent, name := self.getPathParts(path)
	parent, err := nameOrHash(oParent)
	if err != nil {
		return
	}

	if self.diskCache {
		err = backupFs.withRevisionDb(self, func(db *sqinn.Sqinn) (err error) {
			params := make([]interface{}, 2)
			params[0] = parent
			params[1] = name
			res, err := db.Query(
				"SELECT id, mode, timestamp, size, startChunk, startOffset, endChunk, endOffset FROM nodes WHERE parent = ? and name = ?;",
				params,
				[]byte{sqinn.ValInt64, sqinn.ValInt64, sqinn.ValInt64, sqinn.ValInt64, sqinn.ValInt, sqinn.ValInt, sqinn.ValInt, sqinn.ValInt})

			if err != nil {
				return
			}

			if len(res) == 0 {
				// err = errors.New("entry not found in db")
				return
			}

			row := res[0]

			const INO, MODE, TIMESTAMP, SIZE, START_CHUNK, START_OFFSET, END_CHUNK, END_OFFSET = 0, 1, 2, 3, 4, 5, 6, 7
			uid, gid, _ := fuse.Getcontext()
			backupFile = &backupMountFile{
				stat: fuse.Stat_t{
					Ino:      uint64(row.Values[INO].AsInt64()),
					Size:     row.Values[SIZE].AsInt64(),
					Mode:     uint32(row.Values[MODE].AsInt64()),
					Mtim:     fuse.Timespec{Sec: row.Values[TIMESTAMP].AsInt64()},
					Birthtim: fuse.Timespec{Sec: row.Values[TIMESTAMP].AsInt64()},
					Nlink:    1,
					Uid:      uid,
					Gid:      gid,
				},
				chunkInfo: mountChunkInfo{
					StartChunk:  row.Values[START_CHUNK].AsInt(),
					StartOffset: row.Values[START_OFFSET].AsInt(),
					EndChunk:    row.Values[END_CHUNK].AsInt(),
					EndOffset:   row.Values[END_OFFSET].AsInt(),
				},
			}
			return
		})
	} else {
		entryParent, ok := (*self.entriesCache)[parent]
		if !ok {
			// err = errors.New("entry not found")
			return
		}
		backupFile, ok = entryParent[name]
		if !ok {
			// err = errors.New("entry not found")
			return
		}
	}

	return
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
	options     *MountOptions
}

func (self *BackupFS) Open(path string, flags int) (errc int, fh uint64) {
	revision, err := self.getMountRevision(path)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "getMountRevision failed: %v", err)
		errc = -int(fuse.ENOENT)
		return
	}

	if revision == nil || revision.root == path {
		return self.openNode(path, false)
	}

	file, err := revision.getAttr(self, path)
	if err != nil || file == nil {
		errc = -int(fuse.ENOENT)
		fh = ^uint64(0)
		return
	}
	if fuse.S_IFDIR == file.stat.Mode&fuse.S_IFMT {
		errc = fuse.EISDIR
		fh = ^uint64(0)
		return
	}
	fh = file.stat.Ino
	return
}

func (self *BackupFS) Opendir(path string) (errc int, fh uint64) {
	revision, err := self.getMountRevision(path)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "getMountRevision failed: %v", err)
		errc = -int(fuse.ENOENT)
		return
	}

	if revision == nil || revision.root == path {
		return self.openNode(path, true)
	}

	file, err := revision.getAttr(self, path)
	if err != nil || file == nil {
		errc = -int(fuse.ENOENT)
		fh = ^uint64(0)
		return
	}
	if fuse.S_IFDIR != file.stat.Mode&fuse.S_IFMT {
		errc = fuse.ENOTDIR
		fh = ^uint64(0)
		return
	}
	fh = file.stat.Ino
	return
}

func (self *BackupFS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	revision, err := self.getMountRevision(path)
	if err != nil {
		LOG_INFO("MOUNTING_FILESYSTEM", "getMountRevision failed: %v", err)
		return -int(fuse.ENOENT)
	}

	if revision == nil || revision.root == path {
		node := self.getNode(path, fh)
		if nil == node {
			return -int(fuse.ENOENT)
		}
		*stat = node.stat
		return
	}

	file, err := revision.getAttr(self, path)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "getattr for revision file failed: %v", err)
		return -int(fuse.ENOENT)
	}
	if file == nil {
		return -int(fuse.ENOENT)
	}

	*stat = file.stat
	return
}

func (self *BackupFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	revision, err := self.getMountRevision(path)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "getMountRevision failed: %v", err)
		return -int(fuse.ENOENT)
	}

	if revision == nil || revision.root == path {
		return -int(fuse.ENOENT)
	}

	file, err := revision.getAttr(self, path)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "getattr for revision file failed: %v", err)
		return -int(fuse.ENOENT)
	}
	if file == nil {
		return -int(fuse.ENOENT)
	}

	readBytes, err := self.readFileChunkCached(revision.snapshot, &file.chunkInfo, buff, ofst)
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
	revision, err := self.getMountRevision(path)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "error initing revision: %v", err)
		return -int(fuse.ENOENT)
	}

	if revision == nil {
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
		return
	}

	err = revision.readDir(self, path, fill, fh)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "error getting file %s: %v", path, err)
		return -int(fuse.EBUSY)
	}
	return
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

	specificRevisions := strings.Split(self.options.Revisions, ",")
	specificRevisionsDb := make(map[int]bool)
	loadSpecific := false
	for _, specific := range specificRevisions {
		specificSplit := strings.Split(specific, "-")
		if len(specificSplit) == 1 {
			if specificSplit[0] == "" {
				break
			}
			revision, err := strconv.Atoi(strings.TrimSpace(specificSplit[0]))
			if err != nil {
				LOG_ERROR("MOUNTING_FILESYSTEM", "Invalid revision specified: %s", specificSplit[0])
				continue
			}
			specificRevisionsDb[revision] = true
			loadSpecific = true
		} else {
			rangeStart, err := strconv.Atoi(strings.TrimSpace(specificSplit[0]))
			if err != nil {
				LOG_ERROR("MOUNTING_FILESYSTEM", "Invalid start revision specified: %s", specificSplit[0])
			}
			rangeEnd, err := strconv.Atoi(strings.TrimSpace(specificSplit[1]))
			if err != nil {
				LOG_ERROR("MOUNTING_FILESYSTEM", "Invalid end revision specified: %s", specificSplit[1])
			}
			for i := rangeStart; i <= rangeEnd; i++ {
				specificRevisionsDb[i] = true
				loadSpecific = true
			}
		}
	}

	revisionsToLoad := revisions
	if loadSpecific {
		revisionsToLoad = []int{}
		for specific := range specificRevisionsDb {
			revisionsToLoad = append(revisionsToLoad, specific)
		}
	}

	LOG_DEBUG("MOUNTING_FILESYSTEM", "Creating root structure for %v revisions", len(revisionsToLoad))

	alreadyCreated := make(map[string]bool)

	const DIR_MODE = fuse.S_IFDIR | 00555

	self.root = makeMountNode(self.nextIno(), DIR_MODE, fuse.Timespec{})
	self.revisions = make(map[string]*mountRevisionInfo)

	for _, revision := range revisionsToLoad {
		snapshot := self.manager.SnapshotManager.DownloadSnapshot(self.manager.snapshotID, revision)

		creationTime := time.Unix(snapshot.StartTime, 0)

		var dirPath string
		if !self.options.Flat {
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
			dirPath = fmt.Sprintf("%s/%s", dayPath, dirname)
		} else {
			dirPath = fmt.Sprintf(
				"/%04d%02d%02d_%02d%02d.%d",
				creationTime.Year(), creationTime.Month(), creationTime.Day(),
				creationTime.Hour(), creationTime.Minute(), revision)
		}

		date := time.Date(
			creationTime.Year(), creationTime.Month(), creationTime.Day(),
			creationTime.Hour(), creationTime.Minute(), 0, 0, time.Local)
		self.makeNode(dirPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})

		rootPath := fmt.Sprintf("%s/%s", dirPath, "browse")
		self.revisions[rootPath] = &mountRevisionInfo{
			revision:  revision,
			snapshot:  snapshot,
			root:      rootPath,
			diskCache: self.options.DiskCache,
		}
		self.makeNode(rootPath, DIR_MODE, fuse.Timespec{Sec: date.Unix()})
	}

	return
}

func (self *BackupFS) withRevisionDb(revision *mountRevisionInfo, withDb func(*sqinn.Sqinn) error) (err error) {
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
				parent VARCHAR(20),
				name VARCHAR(20),
				mode INTEGER,
				timestamp INTEGER,
				size INTEGER,
				startChunk INTEGER,
				startOffset INTEGER,
				endChunk INTEGER,
				endOffset INTEGER
			);`)
		if err != nil {
			return
		}
		_, err = self.dbProcess.ExecOne(`CREATE UNIQUE INDEX parent_name_idx ON nodes (parent, name);`)
		if err != nil {
			return
		}
		revision.dbInited = true
	}

	err = withDb(self.dbProcess)
	return
}

func (self *BackupFS) downloadEntries(
	mountRevision *mountRevisionInfo,
) (entriesToAdd *mountEntryList, entriesTotal int, retErr error) {
	self.managerLock.Lock()
	defer self.managerLock.Unlock()

	if !self.manager.SnapshotManager.DownloadSnapshotSequences(mountRevision.snapshot) {
		retErr = errors.New("snapshot sequences download failed")
		return
	}

	if !self.manager.SnapshotManager.DownloadSnapshotSequences(mountRevision.snapshot) {
		retErr = errors.New("snapshot sequences download failed")
		return
	}

	entriesToAddContent := make(mountEntryList)
	entriesToAdd = &entriesToAddContent
	uid, gid, _ := fuse.Getcontext()
	mountRevision.snapshot.ListRemoteFiles(
		self.manager.config,
		self.manager.SnapshotManager.chunkOperator,
		func(entry *Entry) bool {
			components := splitMountPath(strings.Trim(entry.Path, "/"))
			componentsLen := len(components)

			var oParent string
			var name string
			if componentsLen == 1 {
				oParent = ""
				name = components[0]
			} else {
				oParent = strings.Join(components[:componentsLen-1], "/")
				name = components[componentsLen-1]
			}

			parent, err := nameOrHash(oParent)
			if err != nil {
				retErr = errors.New(fmt.Sprintf("error hashing file name: %v", oParent))
				return false
			}

			if (*entriesToAdd)[parent] == nil {
				(*entriesToAdd)[parent] = make(map[string]*backupMountFile)
			}

			mode := int(fuse.S_IFREG | (entry.Mode & 00777))
			if entry.Mode&0o20000000000 == 0o20000000000 {
				mode = int(fuse.S_IFDIR | (entry.Mode & 00777))
			}

			(*entriesToAdd)[parent][name] = &backupMountFile{
				stat: fuse.Stat_t{
					Ino:      uint64(self.nextIno()),
					Size:     entry.Size,
					Mode:     uint32(mode),
					Mtim:     fuse.Timespec{Sec: entry.Time},
					Birthtim: fuse.Timespec{Sec: entry.Time},
					Nlink:    1,
					Uid:      uid,
					Gid:      gid,
				},
				chunkInfo: mountChunkInfo{
					StartChunk:  entry.StartChunk,
					StartOffset: entry.StartOffset,
					EndChunk:    entry.EndChunk,
					EndOffset:   entry.EndOffset,
				},
			}
			entriesTotal++
			return true
		})

	if entriesTotal == 0 {
		retErr = errors.New("failed to init revision, no entries were added from the remote file list")
		return
	}

	return
}

func (self *BackupFS) getMountRevision(path string) (mountRevision *mountRevisionInfo, err error) {
	err = self.initRoot()
	if err != nil {
		return
	}

	baseLen := 6
	if self.options.Flat {
		baseLen = 3
	}

	components := splitMountPath(path)
	if len(components) < baseLen {
		return
	}

	if components[baseLen-1] != "browse" {
		return
	}

	revisionRoot := strings.Join(components[:baseLen], "/")
	mountRevision, ok := self.revisions[revisionRoot]
	if !ok {
		err = errors.New(fmt.Sprintf("revision not found for root %v", revisionRoot))
		return
	}

	mountRevision.lock.Lock()
	defer mountRevision.lock.Unlock()

	if mountRevision.inited {
		return
	}

	LOG_DEBUG("MOUNTING_FILESYSTEM", "getMountRevision %d", mountRevision.revision)

	if mountRevision.snapshot == nil {
		err = errors.New("snapshot revision not found")
		return
	}

	entriesToAdd, entriesTotal, err := self.downloadEntries(mountRevision)
	if err != nil {
		return
	}

	err = mountRevision.addEntries(self, entriesToAdd)
	if err != nil {
		return
	}

	LOG_DEBUG("MOUNTING_FILESYSTEM", "added %d entries to revision", entriesTotal)
	mountRevision.inited = true

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

	if self.options.DiskCache {
		self.dbProcess, err = sqinn.Launch(sqinn.Options{})
		if err != nil {
			return err
		}
	}

	self.manager.SnapshotManager.CreateChunkOperator(false, 3, false)

	return nil
}

func (self *BackupFS) cleanup() {
	if self.options.DiskCache {
		self.dbProcess.Terminate()
	}
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
	if len(name) <= 40 {
		ret = name
		return
	}

	hasher := sha1.New()
	_, err = io.WriteString(hasher, name)
	if err != nil {
		LOG_ERROR("MOUNTING_FILESYSTEM", "error hashing file name: %v", name)
		return
	}

	ret = fmt.Sprintf("%x", (hasher.Sum(nil)))
	return
}

type MountOptions struct {
	Flat      bool
	Revisions string
	DiskCache bool
}

func MountFileSystem(fsPath string, manager *BackupManager, options *MountOptions) {
	LOG_INFO("MOUNTING_FILESYSTEM", "Mounting snapshot %s on %s", manager.snapshotID, fsPath)

	fs := BackupFS{
		manager: manager,
		options: options,
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
