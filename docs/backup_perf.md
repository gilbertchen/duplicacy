# Where `backup` spends its time

Investigation into the performance of `duplicacy backup`, in the style of
`snapshot_perf.md`, `copy_perf.md`, `prune_perf.md`, `check_perf.md`,
`restore_perf.md` and `init_perf.md`. `backup` is the only heavy command without
a document of its own.

Four defects were found and one is implemented. The implemented one is the
smallest: the previous revision's metadata sequences were expanded on a one-thread
operator, so `-threads` did nothing for them, and `backup` was the last command
still shaped that way. The two the command is built around — the serial local
file walk and the serial packing loop — and the snapshot-cache clean that runs at
the end of every successful backup are recorded as not going to be implemented.
Several smaller items were measured and are recorded as examined and retained.

## Summary

`backup` is five phases:

1. the previous revision — a listing of `snapshots/<id>/` and the download of its
   snapshot file (`downloadLatestSnapshot`);
2. the previous revision's chunk and length sequences, expanded and turned into
   `chunkCache` (`DownloadSnapshotSequences`, `GetSnapshotChunks`);
3. the local file walk, on its own goroutine (`ListLocalFiles`), and the previous
   revision's file list, on another (`ListRemoteFiles`), merged into an
   `EntryList` as the local entries arrive;
4. the packing loop: every modified file is read, split into chunks and hashed
   (`ChunkMaker.AddData`), and each new chunk is handed to the chunk operator;
5. the metadata — the entry list is encoded and chunked again, and the snapshot
   file is written (`UploadSnapshot`), followed by `CleanSnapshotCache`.

Phases 1 and 2 are proportional to the last revision, not to the repository, and
are a handful of round trips. Phases 3 and 4 are the command: they scale with the
number of files, with the bytes, or with both. Phase 5's `CleanSnapshotCache`
scales with the snapshot cache, which is a local artifact that grows with the
number of cached snapshots.

The measurement that separates them is a profile. On 40 files of 10 MB that are
already incompressible, the CPU samples are 62% in `ChunkMaker.AddData` — 34% in
the BLAKE2 file hasher, 16% in LZ4 — and 19% in the operator's `UploadChunk`.
Everything else in `Backup` is below the profile's resolution.

| Fixture | fresh | re-backup, quick | re-backup, `-hash` | `-enum-only` |
| --- | --- | --- | --- | --- |
| 20,000 × 2 KB, ext4 | 0.30 s | 0.31 s | 0.56 s | 0.26 s |
| 100,000 × 512 B, ext4 | 1.9 s | 0.98 s | 1.72 s | 0.71 s |
| 40 × 10 MB, ext4 | 1.41 s | — | — | — |
| 2 × 1 GB, ext4 | 6.6 s | — | — | — |
| 250,000 × 512 B, ext4 | 5.4 s | 2.4 s | 4.7 s | 1.6 s |

`/usr/bin/time`, best of several runs, cache warm. `-enum-only` returns at
`src/duplicacy_backupmanager.go:350`, so it is phases 1-3 with the packing loop
and the upload left out; on a local repository the walk is the whole of it.

## The call path

`backupRepository` (`duplicacy/duplicacy_main.go:742`) parses the flags, creates
the storage with `-threads` (`:768`) and calls `BackupManager.Backup` (`:811`).

`Backup` (`src/duplicacy_backupmanager.go:131`) then does, in order:

```go
remoteSnapshot := manager.SnapshotManager.downloadLatestSnapshot(manager.snapshotID)              // :160
manager.SnapshotManager.DownloadSnapshotSequences(remoteSnapshot)                                 // :176
for _, chunkID := range manager.SnapshotManager.GetSnapshotChunks(remoteSnapshot, true) { ... }   // :178
allChunks, _ := manager.SnapshotManager.ListAllFiles(manager.storage, "chunks/")                  // :192
chunkOperator := CreateChunkOperator(manager.config, manager.storage, manager.snapshotCache, ...) // :232
go func() { localSnapshot.ListLocalFiles(shadowTop, ...) }()                                      // :238
go func() { remoteSnapshot.ListRemoteFiles(manager.config, chunkOperator, ...) }()                // :244
for { localEntry := <-localListingChannel; ... localEntryList.AddEntry(localEntry) }              // :277
fileChunkMaker := CreateFileChunkMaker(manager.config, false)                                     // :359
for i := range localEntryList.ModifiedEntries {                                                   // :473
    file, err := os.OpenFile(fullPath, os.O_RDONLY, 0)                                            // :477
    entry.Size, entry.Hash, addDataErr = fileChunkMaker.AddData(file, uploadChunkFunc)            // :486
}
fileChunkMaker.AddData(nil, uploadChunkFunc)                                                      // :497
chunkOperator.WaitForCompletion()                                                                 // :498
manager.SnapshotManager.CleanSnapshotCache(localSnapshot, nil)                                    // :555
chunkOperator.Stop()                                                                              // :626
```

`ListLocalFiles` (`src/duplicacy_snapshot.go:66`) drives `ListEntries`
(`src/duplicacy_entry.go:699`) over a stack of directories, one directory at a
time. `ListEntries` reads one directory with `ioutil.ReadDir` (`:708`), reads the
extended attributes of each entry (`:774`), and sends each file down the listing
channel while the subdirectories go back on the stack.

`AddData` (`src/duplicacy_chunkmaker.go:150`) is the packing loop: it reads from
the file into a two-chunk circular buffer, buzhashes it to find the chunk
boundaries, writes each chunk into a `Chunk` (which hashes it with BLAKE2 and
keeps a highwayhash checksum), and calls `sendChunk`. The uploader encrypts and
LZ4-compresses in `Chunk.Encrypt` and writes the file in `UploadChunk`
(`src/duplicacy_chunkoperator.go:565`).

## The local file walk is serial over directories — candidate #1 — **Not going to be implemented**

`ListLocalFiles` pops one directory at a time off a stack and calls `ListEntries`
on it before touching the next:

```go
for len(directories) > 0 {
    directory := directories[len(directories)-1]
    directories = directories[:len(directories)-1]
    subdirectories, skipped, err := ListEntries(top, directory.Path, ...)   // :84
    directories = append(directories, subdirectories...)
}
```

`-threads` does not reach it. The flag is spent on the chunk operator (`:232`),
whose worker count decides how many chunks are uploaded at once; the storage is
created with the same count (`duplicacy_main.go:768`) but the only thing
`FileStorage` does with it is divide the rate limits
(`src/duplicacy_filestorage.go:141`, `:207`). The walk itself has one goroutine.

Each directory costs three things the next one cannot start without: a
`getdents64` loop, a `newfstatat` per entry (`ioutil.ReadDir`), and a `listxattr`
per entry (`ReadAttributes`, `src/duplicacy_utils_others.go:50`). On a local
filesystem that is nothing; on a network filesystem each is a round trip, and the
directories cannot be overlapped because the walk is depth-first.

Measured with `-enum-only`, which is the walk and the merge and nothing else, the
whole command is the walk when the repository is on a mount that charges per
operation:

| Fixture | walk (`-enum-only`) | fresh backup |
| --- | --- | --- |
| 20,000 × 1 KB, repository on virtiofs, storage on ext4 | 24.3 s | — |
| 100,000 × 512 B, repository and storage on virtiofs | 116 s | 122 s |

On the 100,000-file fixture `strace -f -c` counts 100,200 `listxattr` calls at
365 µs each, 100,137 `newfstatat` at 83 µs, and 3,206 `getdents64` at 317 µs —
203,682 file operations, of which the `listxattr` calls alone are 36.7 s of the
46 s the walk spends inside syscalls. The same repository on ext4 walks in
0.71 s. The work is identical; only the mount decides.

A prototype that hands the directories to a worker pool instead of a stack was
measured against the same tree in Python, where the syscall sequence can be
issued from threads: 20,000 files over 100 directories go from 11.9 s to 6.7 s
with four workers. That is the 1.7x that per-directory round trips are worth, and
it is a ceiling rather than a promise: the number of directories bounds the
overlap, so a tree with one file per directory gains nothing.

**Not going to be implemented**, on three grounds. The result of the walk is a
single ordered stream — `EntryList.AddEntry` is called in path order, and the
incomplete-snapshot file is written in that order — so parallelising the walk
means either buffering and re-sorting the entries, which is what the on-disk
entry list exists to avoid, or accepting a different order. Second, the walk is
what `-enum-only` is: a version that overlapped the directories would change what
that mode measures, which is the one thing a diagnostic has to keep fixed.
Third, the win is bounded by the directory count and shows only on a slow
mount — the configuration whose real fix is to back up over a faster mount.

### The attribute read is not an escape hatch — **examined and retained**

`ListEntries` calls `entry.ReadAttributes(top)` for every entry (`:774`), which
on Linux is one `listxattr` (`src/duplicacy_utils_others.go:53`). It is the first
half of every walk, and the `listxattr` count equals the file count exactly:
20,200 calls for a fresh 20,000-file backup, 100,200 for the 100,000-file one. On
ext4 it is cheap (39 µs each, 0.80 s of a 3.10 s syscall total); on virtiofs it
is the single largest item in the walk.

A prototype that read the attributes only when `excludeByAttribute` is set
measured 2.35 s → 1.12 s on the 250,000-file fixture and 2.04 s → 0.88 s for
`-enum-only`, with the `listxattr` count going to zero. The saving is real and
the flag it keys off is off by default, which makes it look like a candidate
rather than a trade.

It is not one. The attributes are stored in the snapshot and put back by
`restore`: `ReadAttributes` is what fills `Entry.Attributes`, `EncodeMsgpack`
writes it into the file sequence (`src/duplicacy_entry.go:353-381`), and
`RestoreMetadata` restores it through `SetAttributesToFile` (`:563`). The
prototype was verified to lose them — a file carrying `user.test=VALUE1` restored
with an empty attribute list — because `ListEntries` is also the code that feeds
`UploadSnapshot`. The `excludeByAttribute` flag decides whether an attribute
*excludes* a file, not whether the attribute is *saved*, so a version that
skipped the read would silently stop backing up every extended attribute.

The only way to make this cheaper without losing data is to read the attributes
after the pack and once per file, or to have the exclusion pass and the save pass
share one read; both are bigger than the flag they would replace, and the saving
on a native filesystem is 0.3 s in a 2 s command.

## The packing loop is serial — candidate #2 — **Not going to be implemented**

`hashMode` is `remoteSnapshot.Revision == 0 || !quickMode` (`:168`), so a fresh
backup and every `-hash` re-backup pack the whole repository. The loop is a plain
`for` over `localEntryList.ModifiedEntries`:

```go
for i := range localEntryList.ModifiedEntries {
    entry := &localEntryList.ModifiedEntries[i]
    fullPath := joinPath(shadowTop, entry.Path)
    file, err := os.OpenFile(fullPath, os.O_RDONLY, 0)
    if err != nil { ... skippedFiles = append(...); continue }
    entry.Size, entry.Hash, addDataErr = fileChunkMaker.AddData(file, uploadChunkFunc)
    file.Close()
}
```

One file at a time: read, buzhash, hash, compress, hand to the operator. The
operator overlaps the compression and the upload across `-threads` while the
chunk maker is producing, but the read and the buzhash are serial, so the pack
goroutine is the producer for the whole repository.

Two profiles of the same 40 × 10 MB fixture show what that costs. On ext4 the
command is 1.40 s and the samples are 62% inside `AddData` (34% BLAKE2, 16% LZ4,
6% buzhash) and 19% in `UploadChunk` — the operator is not the bottleneck. Moving
the storage to tmpfs, where the upload is as cheap as it can be, leaves the wall
time flat across the thread count:

| Storage | `-threads 1` | `-threads 4` | `-threads 8` |
| --- | --- | --- | --- |
| ext4 | 1.65 s | 1.54 s | 1.41 s |
| tmpfs | 1.37 s | 1.54 s | 1.53 s |

1.17x from eight threads on ext4, and nothing on tmpfs, where the serial pack is
the whole command. The same shape holds on 2 × 1 GB of incompressible data: the
run is 6.6 s at one thread and 6.5 s at eight, so eight threads move it by 1.02x.
The profile puts 60% of the samples in `AddData` and the CPU fraction at 140%:
the pack is one goroutine and a third of the machine is idle behind it.

`DUPLICACY_SKIP_FILE_HASH=1` is the diagnostic. It makes `NewFileHasher`
(`src/duplicacy_config.go:395`) return the no-op `DummyHasher` (`:373`) for the
rest of the run (`:366-370`), and on the 40 × 10 MB fixture at one thread it
takes 1.41 s to 1.11 s: the file hasher is a fifth of the command, and it is paid
even for a chunk that already exists in the storage and is never uploaded.

**Not going to be implemented.** The shape is the one `restore_perf.md` examined
and retained for `RestoreFile`: a worker per file would need its own descriptor
and its own slice of the chunk-maker state, and `ChunkMaker` is one object with
one buffer, one hash sum and one current chunk. More than that, the order the
chunks are sent in is the order `AddUploadedChunk` numbers them
(`src/duplicacy_entrylist.go:153`), which is what makes an interrupted backup
resumable; overlapping the files would either break that numbering or need the
chunks buffered and re-numbered. The measured win is 1.17x on a fast local disk
and nothing where the pack is the bottleneck, which does not justify that.

## `CleanSnapshotCache` runs at the end of every successful backup — candidate #3 — **Not going to be implemented**

`Backup` ends with

```go
if !manager.config.dryRun {
    manager.SnapshotManager.CleanSnapshotCache(localSnapshot, nil)   // :555
}
```

`CleanSnapshotCache` (`src/duplicacy_snapshotmanager.go:523`) was written for
`prune`, where the cache has just been invalidated by deletions. `backup` calls
it with `allSnapshots == nil`, which is the mode that is *supposed* to be
conservative: the first thing it does is look for the `fossils` directory and
return immediately if it exists, because a deletion is in flight and every cached
snapshot will be needed.

When there is no `fossils` directory it does the work anyway. It walks the whole
cached `snapshots/` tree, parses every cached snapshot file, and for each one
walks its chunk sequence looking every chunk up twice — once in the cache and
once in the storage (`:593-594`). Then it lists the whole cached `chunks/` tree
and deletes anything the collected set does not reference.

The listing is what it costs on a quiet repository. On a 50-directory,
10,000-file fixture whose repository sits on ext4 and whose storage is a
virtiofs mount, a quick re-backup that fetches one metadata chunk and uploads
nothing issues 66 `newfstatat` calls under the cache's `chunks/` and 7 under its
`snapshots/`, against 8 operations on the storage proper — nine tenths of the
run's file operations. Skipping the call, purely to attribute the time, takes
that run from 0.27 s to 0.14 s.

It also works against the snapshot cache. `prune_perf.md` records that
`CleanSnapshotCache` "probes every chunk of every cached snapshot" as a
candidate, and `check_perf.md` changed the cache decision to follow the
filesystems so that a network storage whose repository is elsewhere keeps its
cache. That is the configuration where `backup` pays the most for the clean: the
cache exists to avoid reading metadata chunks from the storage, and the clean
re-derives the same set by reading the local cache and stat-ing the storage.

**Not going to be implemented**, because removing the call is a behaviour change
rather than a deletion. `prune` cleans with the real `allSnapshots` map and would
still do so, but a repository where only `backup` is ever run would keep every
cached chunk until the next `prune`, and `CleanSnapshotCache` is also what drops
a cached snapshot whose chunks have disappeared from the storage. The honest fix
is to decide the clean by the same filesystem rule the cache itself is now
decided by — clean only when the cache is worth keeping, and only when the
snapshot set has actually changed — and that decision belongs with whichever
document owns the cache rather than with `backup`.

## The previous revision's sequences are expanded on a one-thread operator — candidate #4 — **Implemented**

`backup` expands the previous revision's chunk and length sequences before the
walk starts (`:176`). Each goes through `SnapshotManager.DownloadSequence`
(`src/duplicacy_snapshotmanager.go:345`), which creates the snapshot manager's
operator with a hard-coded one thread:

```go
func (manager *SnapshotManager) DownloadSequence(sequence []string) (content []byte) {
    manager.CreateChunkOperator(false, false, 1, false)   // :346
    ...
}
```

This is the same defect `restore_perf.md` candidate #4 records and `prune` and
`check` avoid: the user's flag reached the chunk operator that `Backup` creates
at `:238` for the file chunks, but the manager's operator — the one the sequences
go through — was created with one thread. `DownloadSequence` submits every chunk
of the sequence at once and waits, so they overlap only as wide as that operator.

`backup` was the only command left with this shape. Every other command creates
the manager's operator itself before it expands, so that the thread count of the
expansion is a decision the command makes rather than the `1` in
`DownloadSequence`:

| Command | Who creates the manager's operator | Threads |
| --- | --- | --- |
| `restore` | `Restore` before the expansion (`src/duplicacy_backupmanager.go:701`) | `-threads` |
| `check` | `CheckSnapshots` (`src/duplicacy_snapshotmanager.go:1036`) | `-threads` |
| `prune` | `PruneSnapshots` (`:2224`) | `-threads` |
| `list` | `ListSnapshots` (`:921`) | 1 |
| `diff` | `Diff` (`:1875`) | 1 |
| `history` | `ShowHistory` (`:2097`) | 1 |
| `cat` | `RetrieveFile` (`:1741`) | 1 |
| **`backup`** | **nobody** — `DownloadSequence` created it at `:346` | 1 |

`list`, `diff`, `history` and `cat` create a one-thread operator deliberately:
they either take no `-threads` or read a single file, so the operator is there
for the sequence walk's `ListRemoteFiles` to have one. `backup` has no such
reason — the flag is documented as its "number of uploading threads" and the flag
value was already reaching the file chunks — so the one-thread expansion was the
defect `restore` had, and it was the only remaining instance.

The `1` in `DownloadSequence` is load-bearing and stays: it is what builds an
operator for a caller that did not, which is how `list -files`' walk gets a
non-nil `manager.chunkOperator` to hand to `ListRemoteFiles`. That is why the fix
belongs at the command, where `restore` put it, and not in `DownloadSequence`.

**Implemented** (`src/duplicacy_backupmanager.go:174-188`): `Backup` creates the
manager's operator with the user's `-threads` before expanding, with a deferred
`stopChunkOperator`, so `DownloadSequence` reuses it. The operator is created
inside the `remoteSnapshot.Revision > 0` branch, which is the only branch that
expands anything; a first backup has no previous revision, so it neither creates
nor stops an operator.

The value is small and it is fixed because the flag did nothing rather than for
the timing. A sequence is a single metadata chunk for any repository small enough
to build in a test — 2,000 files produce 168 file-sequence chunks but a
one-chunk `ChunkSequence` — so the expansion is 2 chunks even on the 100,000-file
fixture, and a 250,000-file repository expands all three sequences in 23
(`restore_perf.md`). It is on a slow mount that the per-chunk round trips show,
which is the same place candidate #1 is worst.

`TestBackupExpandsSequencesUnderThreads` (`src/duplicacy_backupmanager_test.go`)
pins it: a recording storage notes the thread count of the operator each chunk
download ran on, and the test fails with "Expected the sequences to be expanded
on an eight-thread operator, saw map[1:true]" when the `CreateChunkOperator` call
is removed. It also checks that a first backup creates no operator and that
`Backup` stops and clears the one it made.

## Candidate fixes

### Smaller items

- **The chunk-cache write-back is already guarded.** `UploadChunk` checks
  `IsCacheNeeded()` before saving a metadata chunk
  (`src/duplicacy_chunkoperator.go:574`), so a local backup does not write
  metadata chunks into the cache. Nothing to change.
- **The `nesting` probe is paid by backup too.** `CreateBackupManager` downloads
  the config, which ends in `SetNestingLevels` and probes for a file named
  `nesting` that nothing writes (`src/duplicacy_storage.go:121`). One round trip
  per run. This is the same item recorded in `init_perf.md` and `prune_perf.md`;
  it belongs to storage creation, not to `backup`.
- **`FileStorage.UploadFile` stats the parent directory of every chunk.**
  `uploadFile` does an `os.Lstat` on the parent
  (`src/duplicacy_filestorage.go:178`) and then `MkdirAll` (`:183`), which stats
  it again. On a fresh backup that is two `newfstatat` calls per chunk written
  into a directory that does not exist yet, and one per chunk afterwards.
  `copy_perf.md` and `prune_perf.md` record the same pair; it is a property of
  the file storage rather than of this command.
- **`GetSnapshotChunks` makes two passes over the referenced chunks.** At `:178`
  it maps the three sequences' hashes to ids, then, if `ChunkHashes` is still
  empty, expands `ChunkSequence` through `DownloadSequence` and walks the result
  (`src/duplicacy_snapshotmanager.go:821-834`). Both passes are over in-memory
  slices; the second is what populates the cache and does not re-download
  anything the first already saw. Below noise.
- **`loadIncompleteSnapshot` stats two files on every hash backup.** `:493` and
  `:497`, on the `.duplicacy/cache` path, once per run, and the answer is `nil`
  unless a previous run was interrupted. Local, and fixed in size.

### Deliberately not pursued

- **A chunk index, or a revision index.** As in `snapshot_perf.md` (candidate
  #7), `copy_perf.md` and `prune_perf.md`, this would change the storage format
  on disk. `backup`'s use of one would be the `chunks/` listing it already skips
  on the storages that offer fast listing, so it would buy nothing here.
- **Parallelising `ListAllFiles` on an initial backup.** It runs only when
  `IsFastListing()` is true, which is the storages for which listing is cheap,
  and even there it is the same per-directory walk the rest of the code does. The
  same conclusion as `prune_perf.md`.
- **`CleanSnapshotCache` probing every cached snapshot's chunks.** The same item
  `prune` records, and the same answer: it scales with the cache rather than with
  the repository, and the clean belongs to `prune`.
- **Doing less on a re-backup.** A quick re-backup of an unchanged tree still
  pays the whole local walk, because proving a file is unchanged needs its size
  and mtime. That is the flag's contract; `-enum-only` is the cheap way to ask
  for the walk alone.

## How to confirm on a given setup

- `duplicacy -d backup ...` sets DEBUG logging
  (`duplicacy/duplicacy_main.go:146-148`); `-v` sets TRACE. `BACKUP_START` ("Last
  backup at revision N found"), `BACKUP_INDEXING` ("Indexing `<top>`") and
  `BACKUP_END` bound the phases: the gap between the first two is the previous
  revision's listing and its sequence expansion, and everything from
  `BACKUP_INDEXING` to the `UPLOAD_PROGRESS` lines is the walk plus the pack.
- `backup -enum-only` runs the walk and the merge and stops, which is the way to
  time phase 3 on its own without writing anything to the storage. It is what
  candidate #1 is measured with.
- `backup -stats` prints `BACKUP_STATS`: the file and chunk counts say whether
  the run packed at all, and "Total running time" is the whole command. `-stats`
  also turns on the per-chunk `UPLOAD_PROGRESS` lines, whose rate is the upload
  speed and therefore says whether the operator or the pack is the bottleneck.
- `strace -f -c -e trace=listxattr,getdents64,newfstatat,openat,read` with the
  recipe in `docs/README.md`. On any backup the `listxattr` count equals the file
  count of the walk — 20,200 for a 20,000-file fresh backup, 20,200 for a quick
  re-backup, 100,200 for a 100,000-file one — and that 1:1 match is the attribute
  read, not a defect. Candidate #3's signature is the `newfstatat` count under
  the cache's `chunks/` and `snapshots/` directories on a run that uploads
  nothing: 66 and 7 on the 10,000-file fixture above, against 8 operations on the
  storage. Candidate #4's fix has no syscall signature either: the operator's
  thread count decides whether the metadata downloads overlap, so compare
  `backup -threads 1` with `-threads 8` on a storage where a metadata read is a
  round trip and a repository whose sequences are more than one chunk.
- `duplicacy -profile 127.0.0.1:6060 backup` serves a Go pprof endpoint
  (`duplicacy/duplicacy_main.go:159-164`); `curl
  'http://127.0.0.1:6060/debug/pprof/profile?seconds=1'` and
  `go tool pprof -top duplicacy_main cpu.prof` is how the 62%-in-`AddData`,
  34%-BLAKE2 split above was produced. It is the measurement that decides between
  candidates #1 and #2: a run dominated by `ChunkMaker.AddData` and
  `minio/blake2b-simd` is pack-bound, and `-threads` will not help it.
- `DUPLICACY_SKIP_FILE_HASH=1 backup` removes the per-file BLAKE2 for the run
  (`src/duplicacy_config.go:366-370`, read by `NewFileHasher` at `:395`). It is a
  diagnostic, not a setting: the snapshot it writes is unusable. On the 40 ×
  10 MB fixture it takes a fresh backup from 1.41 s to 1.11 s, which is the file
  hasher's share of candidate #2.
- Time a fresh backup against a quick re-backup of the same tree. On 20,000 files
  they are 0.30 s and 0.31 s, because both pay the walk and only one packs; on
  40 files of 10 MB the pair separates the phases.
- `go test ./src/ -vet=off` is the safety net for any change to this path.
  `TestBackupManager` (`src/duplicacy_backupmanager_test.go:179`) exercises
  backup and restore at one and several threads, quick and `-hash`;
  `TestSnapshotCacheSkipsSync` (`:690`) and `TestCorruptCachedChunkIsRefetched`
  (`:766`) cover the snapshot cache candidate #3 touches;
  `TestBackupExpandsSequencesUnderThreads` (`:1071`) covers candidate #4's fix,
  and `TestDownloadSequencesOverlapUnderThreads`
  (`src/duplicacy_snapshotmanager_test.go:2270`) covers the mechanism it relies
  on. `AGENTS.md` records `TestPersistRestore` as failing on an unmodified
  checkout for an unrelated reason.

## Implemented

| Candidate | Change | Files |
| --- | --- | --- |
| #4 | `Backup` creates the snapshot manager's chunk operator with `-threads` before expanding the previous revision's sequences, and stops it on the way out | `src/duplicacy_backupmanager.go` |

Covered by `TestBackupExpandsSequencesUnderThreads`
(`src/duplicacy_backupmanager_test.go`); it fails when the `CreateChunkOperator`
call is removed. `go test ./src/ -vet=off` passes, and the chunk tree, the
revision set, the `BACKUP_STATS` output and a `restore` from the resulting
storage are identical to the same run before the change.
