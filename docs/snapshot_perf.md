# Why listing snapshot revisions is slow

Investigation into the performance of `duplicacy list`. This document records
the findings and the candidate fixes; candidate fixes #1, #2 and #3 below have
since been implemented in `src/duplicacy_snapshotmanager.go`, #4 (the
parallel `list`) in both `src/duplicacy_snapshotmanager.go` and
`duplicacy/duplicacy_main.go`, #5 (the single `list -files` pass) in
`src/duplicacy_snapshotmanager.go`, #6 (the id listing) in
`src/duplicacy_b2storage.go`, `src/duplicacy_b2client.go` and
`src/duplicacy_azurestorage.go`, and #8 (the unread chunk sequence in
`list -files`) in `src/duplicacy_snapshotmanager.go`. Candidate #7 (the revision
index) is not going to be implemented, because it would change the storage
format on disk; the candidates that were measured and rejected are recorded
under "Deliberately not pursued".

## Summary

`duplicacy list` used to be a serial, one-operation-at-a-time loop. For every
revision of every snapshot id it performs an existence check and a file
download, with a chunk operator hard-coded to a single thread. Only the file
downloads are now overlapped, and only when `-threads N` (N > 1) is given;
everything else is still serial.

Against cloud storage each of those operations is a network round trip, so the
cost grows linearly at roughly two round trips per revision; fix #3 removed the
existence check when the revision was just listed, halving that, and fix #4
overlaps the remaining round trips on request.

Against local storage the same code runs, but each hop is a syscall instead of a
round trip: it is usually fine on a fast native filesystem (~3 ms per revision)
and painful on a slow local mount.

## The call path

`listSnapshots` (`duplicacy/duplicacy_main.go:899-950`) creates the storage,
calls `CreateBackupManager` (which downloads the config), calls
`SetupSnapshotCache`, and then hands off to `SnapshotManager.ListSnapshots`
(`src/duplicacy_snapshotmanager.go:657-765`).

`ListSnapshots` is a plain nested loop whose downloads are now optionally
overlapped (fix #4):

```go
manager.CreateChunkOperator(false, false, 1, false)   // threads == 1
...
for _, snapshotID = range snapshotIDs {
    revisions, err = manager.ListSnapshotRevisions(snapshotID)
    snapshots := manager.downloadSnapshots(snapshotID, revisions, listed, threads)
    for i, revision := range revisions {
        snapshot := snapshots[i]   // already downloaded
        ...
    }
}
```

With `threads == 1` (the default) the downloads happen one at a time, exactly as
before; the printing is serial in either case.

`revisionsToList` is empty unless `-r` was passed, so `ListSnapshotRevisions`
(`src/duplicacy_snapshotmanager.go:484-507`) always runs and always hits the
remote storage. The local snapshot cache is never consulted for the revision
list.

## Cost per revision

`DownloadSnapshot` (`src/duplicacy_snapshotmanager.go:209-242`) used to do two
operations for every revision, including revisions already present in the local
cache:

1. `manager.storage.GetFileInfo(0, snapshotPath)` — an unconditional existence
   check. The comment at `:214` explains this is deliberate, because the cache
   may hold a stale copy, but it costs a round trip per revision even when the
   file is already cached (`src/duplicacy_sftpstorage.go:242-263`,
   `src/duplicacy_b2storage.go:177-202`,
   `src/duplicacy_gcdstorage.go:741-772`). Fix #3 skips it when the revision
   came from `ListSnapshotRevisions`, which already enumerated the directory.
2. `manager.DownloadFile(snapshotPath, snapshotPath)` — a cache lookup that
   misses on the first run, then the actual download
   (`src/duplicacy_snapshotmanager.go:2610-2665`).

So the default `list` used to be approximately:

```
1 x ListFiles("snapshots/<id>/")
+ for each revision: (GetFileInfo + DownloadFile)
```

and is now:

```
1 x ListFiles("snapshots/<id>/")
+ for each revision: DownloadFile
```

Until fix #2 was implemented there was also a
`manager.storage.CreateDirectory(0, snapshotDir)` (plus the equivalent call on
the snapshot cache) at the top of `DownloadSnapshot`, and the same pair at the
top of `ListSnapshotRevisions`. Both are gone now; see the candidate fixes below
for why that was also a correctness fix and not only a performance one.

At 50-100 ms RTT and a few hundred revisions this is tens of seconds. With
`-all` it repeats for every snapshot id after a single `snapshots/` listing.

## The snapshot-file cache and where it is written

The cache **read** is guarded with `IsCacheNeeded()`:

```go
if manager.storage.IsCacheNeeded() {
    ... manager.snapshotCache.DownloadFile(0, path, manager.fileChunk) ...
}
```

and so is the cache **write** (`src/duplicacy_snapshotmanager.go:3101`), which
fix #1 below turned off for the storages that never read it back. What decided
the flag is the storage URL alone, and not the filesystem: a plain path and a
`flat://` URL got `false`, a `samba://` URL and a Windows UNC path `true`, and no
local path was ever asked about. The flag is still off for a local path, where a
cache read costs what the storage read costs — but it missed the case the cache is
actually for, because a share mounted at `/mnt/nas` or mapped to a drive letter is
spelled like a local disk. Those paths got `false` too, so every run re-read one
snapshot file per revision from the share.

`docs/check_perf.md` has since changed that decision to ask the filesystem rather
than the URL, caching a file storage only when the storage is on a network
filesystem and the repository is not — which is the only combination where a cache
read replaces a round trip. A local storage, and a remote storage whose repository
shares its filesystem, are left alone exactly as before. The measurements are in
that document.

Verified directly, on the local storage of that fixture:

- `list` leaves `.duplicacy/cache/default/snapshots/` empty, where before fix #1
  it held all 301 files. Corrupting anything there changes nothing, because
  nothing is written and nothing is read.
- Warm and cold-cache timings are indistinguishable: there is no cache to warm.

The cache therefore does not reduce round trips on a local backend; it only
removes the payload transfer for the backends where it is actually consulted,
which is the remote-storage case `docs/check_perf.md` now keys off the filesystem.

## Local storage measurements

Measured with a binary built from HEAD against a real repository with 301
revisions.

| Storage location | `list` (301 revisions) | per revision |
| --- | --- | --- |
| `/tmp` (native ext4) | 0.89 s warm, 0.96 s cold | ~3.0 ms |
| `/mnt/d` (WSL drvfs, still local) | 12.0 s | ~40 ms |

Scaling on ext4 with `list -r ...` confirms it is linear, with no fixed overhead
beyond roughly 10 ms of startup:

```
1 rev    0.03 s
1-10     0.06 s
1-50     0.17 s
1-100    0.31 s
1-200    0.60 s
1-301    0.88 s
```

Scaling on drvfs:

```
1 rev    0.10 s
1-10     0.43 s
1-50     1.98 s
1-100    3.84 s
1-200    7.83 s
1-301   12.13 s
```

Other commands, same repository:

```
                 ext4      drvfs
list             0.89 s    12.0 s
list -all        1.08 s    12.2 s
list -files      0.99 s    27.9 s
list -chunks     0.94 s      n/a
```

`list -files` nearly triples the time on a slow mount because it adds the
per-revision metadata-chunk reads.

The process is idle, not computing: 6% CPU, 66,009 voluntary context switches
and 12.08 s elapsed for `list` on drvfs.

### Syscall attribution on drvfs

Summing per-call wall time from `strace -f -T`:

| syscall | total | calls | per call |
| --- | --- | --- | --- |
| `fsync` | 4.19 s | 301 | 13.9 ms |
| `newfstatat` | 2.73 s | 1209 | 2.3 ms |
| `renameat` | 1.65 s | 301 | 5.5 ms |
| `openat` | 1.51 s | 622 | 2.4 ms |
| `mkdirat` | 1.24 s | 605 | 2.0 ms |

11.36 s of the 12.0 s is spent inside these filesystem calls. The counts are
identical on ext4 — only the per-call latency differs, which is why the same
code is roughly 13 times slower on drvfs. `fsync` alone is more than a third of
the total.

Per revision the loop emits: 4 `newfstatat`, 2 `openat` and, before fixes #1
and #2, 2 `mkdirat`, 1 `fsync` and 1 `renameat`.

The per-revision cost broke down as (line numbers as of the original
investigation, before fix #1 and fix #2):

```
DownloadSnapshot:
  storage.CreateDirectory  -> mkdirat (EEXIST)                    :213-214
  storage.GetFileInfo      -> newfstatat                           :220
  DownloadFile:
    cache read (SKIPPED, IsCacheNeeded() == false)                 :2626
    storage.DownloadFile   -> openat + read                        :2636
    snapshotCache.UploadFile -> openat + write + fsync + renameat  :2669
```

After fix #1 removed the cache write and fix #2 removed the `CreateDirectory`
calls, only `GetFileInfo` and the download remain:

```
DownloadSnapshot:
  storage.GetFileInfo      -> newfstatat                           :216
  DownloadFile:
    cache read (SKIPPED, IsCacheNeeded() == false)                 :2612
    storage.DownloadFile   -> openat + read                        :2622
```

Measured on the same 20-revision repository: `mkdirat` went from 44 to 2 calls
per `list`, and the cached snapshot files written went from 20 to 0.

After fix #3 the `GetFileInfo` is also gone for revisions that were just listed
by `ListSnapshotRevisions`:

```
ListSnapshotRevisions:
  storage.ListFiles("snapshots/<id>/") -> openat + read            :500
DownloadSnapshot (revision was listed, so no GetFileInfo):
  DownloadFile:
    cache read (SKIPPED, IsCacheNeeded() == false)                 :2627
    storage.DownloadFile   -> openat + read                        :2637
```

`DownloadSnapshot` still calls `GetFileInfo` for a revision the user named
explicitly with `-r`, because that revision was not enumerated and the check
protects against a stale snapshot cache entry.

Measured on a 60-revision local repository: `newfstatat` per `list` fell from 77
to 37 (one per revision), and on a slow drvfs mount `/usr/bin/time` went from
0.40 s to 0.27 s. On native ext4 the same repository is ~0.02 s either way,
where the syscall is a few microseconds.

## `list -files` and `list -chunks`

With `showFiles` (`src/duplicacy_snapshotmanager.go:903-942`) each revision gets:

- `DownloadSnapshotSequences` -> `DownloadSequence` -> `chunkOperator.Download`
  per metadata chunk (`src/duplicacy_snapshotmanager.go:339-345`). The operator
  has one worker, and every chunk download is a `FindChunk` followed by a
  `DownloadFile` (`src/duplicacy_chunkoperator.go:284-490`) — two serial
  operations per metadata chunk, per revision.
- `snapshot.ListRemoteFiles(...)`, one pass per sequence
  (`src/duplicacy_snapshot.go:107-207`). This used to be called twice for the
  file sequence, re-iterating and re-decoding every entry on the second call
  (`:727` and `:742` in the original report). The first pass is remote; the
  second was served from the cache, but it still cost a metadata chunk fetch per
  revision. Fix #5 removed that second call.

Against local storage these are a couple of syscalls rather than network round
trips, so they are proportionally cheaper than in the cloud case but still
strictly additive.

### `list -files` expanded the chunk sequence it never read

A snapshot names every chunk it references through three sequences — the file
list, the chunk hashes and the chunk lengths — and `list -files` was fetching
all three. Only two of them are read: `Entry.check`
(`src/duplicacy_entry.go:889-907`) validates each entry against
`snapshot.ChunkLengths`, and the entry itself carries what the file list prints.
`ChunkHashes` is loaded by `DownloadSnapshotSequence(snapshot, "chunks")` and
is read only by `-chunks`, which prints the chunk ids. That is fix #8.

The three sequences are stored as three different chunk sets, so dropping the
chunk sequence is one fewer `FindChunk` + `DownloadFile` per revision — a round
trip on cloud storage, and on a local mount a `newfstatat` + `openat` on the
storage plus the cache write and its `mkdirat`. The sequence is a separate set
of chunks from the other two even when the file list is unchanged, so this is
not absorbed by any sharing between revisions.

The old code and the new code are one call apart:

```go
// before: loads "chunks" and "lengths"
manager.DownloadSnapshotSequences(snapshot)

// after: loads only the sequence the file walk reads
manager.DownloadSnapshotSequence(snapshot, "lengths")
```

`-chunks` still expands the chunk sequence, through `GetSnapshotChunks`
(`src/duplicacy_snapshotmanager.go:742-770`), which fetches it when
`snapshot.ChunkHashes` is empty. `-files -chunks` therefore still reads all
three sequences once each, and the output is unchanged in every mode.

Measured on a 300-revision repository on the slow drvfs mount from the table
above, `/usr/bin/time`, `list -files` cold (cache removed before each run):

```
              cold      steady state
before   13.0-16.8 s    3.9-4.2 s
after     7.4-7.5 s      2.7-2.9 s
```

The cache shows the same halving directly: on the same fixture `list -files -r
1-100` leaves 227 chunk-cache entries before and 131 after, and `-r 1-200`
leaves 427 against 231. The counts are not exactly halved because a revision
whose file list is unchanged reuses the file and length chunks of the previous
one, while the chunk sequence is a third, separate set that is still unique per
revision.

On native ext4 the same 300-revision fixture is unaffected by the syscall count
and only drops with the cache write: cold `list -files` 0.19 s to 0.15 s, warm
0.12 s to 0.10 s.

`list -files`, `list -chunks`, `list -files -chunks`, `list`, `list -all`,
`list -r 5`, `list -files -r 5-20` and `list -files -threads 4` all produce
byte-identical output (after normalising the throughput and elapsed fields of the
progress lines) with exit code 0 either way.

## Backend-specific amplifiers

- **SFTP / SFTP-C** (`src/duplicacy_sftpstorage.go`): `ReadDir`, `Stat` + `Mkdir`
  and `Open` are all sequential on one multiplexed connection, and every call is
  wrapped in `retry()` which can reconnect and back off (`:135-165`). Worst
  ratio of round trips to payload.
- **GCD** (`src/duplicacy_gcdstorage.go`): path-to-id resolution uses
  `listByName` name queries. Before fix #2, `CreateDirectory` -> `GetFileInfo` ->
  `getIDFromPath`/`listByName`, and `ListFiles("snapshots/<id>/")` ->
  `getIDFromPath` again, turned the operations above into several Drive API
  queries per revision, plus Drive quota.
- **B2** (`src/duplicacy_b2storage.go:41-99` and
  `src/duplicacy_b2client.go:401-554`): the `snapshots` listing used to be a
  flat prefix scan with `maxFileCount = 1000` that paged through every snapshot
  file of every id and revision, deduplicating subdirectories client-side, so
  `list -all` on a large bucket paid for a full `snapshots/` traversal. It now
  sends a delimiter, and the service returns one `"folder"` entry per id
  instead.
- **Azure** (`src/duplicacy_azurestorage.go:55-120`): the same flat prefix scan,
  deduplicating the ids client-side without a delimiter. It now sends
  `Delimiter: "/"` for `snapshots/` and reads the `BlobPrefix` entries.
- **S3, GCS, Swift, WebDAV**: cheaper for the id listing (delimiter or
  CommonPrefixes) but still pay the per-revision `GetFileInfo` and
  `DownloadFile`.
- **OneDrive, Hubic**: `ListEntries` is non-recursive with 1000-item paging, so
  the id listing is fine.

## How to confirm on a given setup

- `duplicacy list -v` sets TRACE logging
  (`duplicacy/duplicacy_main.go:142-148`), printing the `LIST_FILES`,
  `SNAPSHOT_LIST_REVISIONS` and `DOWNLOAD_FILE` sequence; `-d` adds DEBUG.
  Timing the gap between consecutive `SNAPSHOT_INFO` lines shows whether the
  cost is the revision listing or the per-revision download.
- `/usr/bin/time -v duplicacy list`: if CPU percentage is low and elapsed is
  high, the time is in syscalls or round trips, not processing.
- Sum the per-call wall time of the `fsync`, `renameat`, `openat`, `newfstatat`
  and `mkdirat` calls with the `strace` recipe in `docs/README.md`. If `fsync`
  dominates, it is the snapshot-cache write.
- `list -r 1` versus a full `list` gives the per-revision marginal cost.
- `list -files` fetches two metadata chunks per revision, `list -files -chunks`
  three. `strace -f -e trace=openat duplicacy list -files -r 1-50` counts the
  `openat` calls under `chunks/` on the storage and under
  `.duplicacy/cache/<name>/chunks/`; on the 300-revision fixture the storage
  count drops from 127 to 81 for the same range when the unread sequence is no
  longer expanded.
- `list -threads N` overlaps the per-revision downloads. The output is
  independent of `N`, so `list` and `list -threads 8` can be diffed directly to
  confirm that; the benefit only shows up when the latency of a single download
  dominates, which is the round-trip case rather than the local one.
- Compare the same repository on a native filesystem against the mount in
  question: syscall counts will match while timings differ.

## Candidate fixes

Ordered roughly by expected benefit for local storage.

- **Do not write the snapshot cache when `IsCacheNeeded()` is false.** Mirror the
  read-side guard at `src/duplicacy_snapshotmanager.go:2669`. This removes the
  `fsync`/`renameat` pair and the cache `mkdirat`/`newfstatat` traffic per
  revision, which is the majority of the slow-mount cost. **Implemented**: the
  cache write in `DownloadFile` is now guarded by `IsCacheNeeded()`, matching the
  cache read and `UploadFile`.
- **Skip the redundant `CreateDirectory` on storage and cache** in
  `DownloadSnapshot` (`:213-214`) and `ListSnapshotRevisions` (`:494-499`). Each
  is an `EEXIST` `mkdirat` or a `Stat` round trip, and on a directory-based
  backend it has a visible side effect: listing a snapshot id that does not
  exist *creates* `snapshots/<id>`, which then shows up as a repository in
  `duplicacy info`, in `check -all` output and in `prune`. It also makes read
  commands fail on read-only storage, because `ListSnapshotRevisions` treats a
  failed `CreateDirectory` as fatal. **Implemented**: both calls are gone. The
  directory is created by `SnapshotManager.UploadFile` instead, which is the
  only place that needs it (Dropbox and similar backends cannot upload a file
  into a directory that does not exist). A missing snapshot directory now reads
  as an empty one: `FileStorage`, `SambaStorage` and `ACDStorage` already
  behaved that way, and `SFTPStorage`, `GCDStorage`, `DropboxStorage`,
  `WebDAVStorage`, `OneDriveStorage` and `HubicStorage` were changed to match.
- **Skip the `GetFileInfo` existence check** when the snapshot body is already
  cached, or fold it into the directory listing that `ListSnapshotRevisions`
  just performed. **Implemented**: `ListSnapshotRevisions` already enumerated the
  snapshot directory, so the revisions it returns are known to exist and
  `ListSnapshots`, `CheckSnapshots`, `ShowHistory`, `downloadLatestSnapshot`,
  `PruneSnapshots` and `CopySnapshots` all download them through the private
  `downloadSnapshot(..., listed = true)`, which skips the check. `DownloadSnapshot`
  keeps it, so a revision named with `-r` is still validated against the storage
  (the cache may hold a stale copy), and `copy` keeps its own destination check.
- **Parallelise the revision loop** and raise the chunk operator thread count for
  `list`; revisions are independent. This is the only change that helps when the
  filesystem itself is the bottleneck. **Implemented**: `list` has a `-threads`
  flag (default 1, so the on-the-wire behaviour is unchanged unless it is asked
  for) and the snapshot files are downloaded by that many workers through
  `downloadSnapshots`. Each worker owns a `Chunk` so the downloads don't share
  `manager.fileChunk`, and passes its own thread index because some backends keep
  a per-thread client or nested directory; to keep those indexes in range,
  `list` now creates its storage with the requested number of threads instead of
  a hard-coded 1. Only the downloads overlap -- everything that prints is still
  done in revision order, so the output is byte-for-byte identical to the
  single-threaded one. This is the only change that helps when the filesystem
  itself is the bottleneck.

  Measured on a 120-revision repository on drvfs (the slow local mount from the
  table above), `/usr/bin/time` for `list`:

  ```
  threads=1   threads=2   threads=4   threads=8   threads=16
  0.25 s      0.19 s      0.18 s      0.20 s      0.26 s
  ```

  On a native ext4 filesystem the per-download syscall is a few microseconds, so
  there is nothing to overlap and more threads only add scheduling overhead
  (~0.12 s at one thread against ~0.18 s at four on a 300-revision repository).
  That is why the default stays at 1: the flag is for the round-trip case.
- **Avoid the double `ListRemoteFiles` pass** in the `showFiles` branch by
  computing sizes and printing in a single traversal. **Implemented**: the
  `showFiles` branch of `ListSnapshots` now walks the file sequence once and
  keeps the entries it walks, instead of computing the sizes in one pass and
  printing in another. The size column is only `String(maxSizeDigits)` wide, and
  its width is not known until the largest file has been seen, so the entries
  are collected during the walk and printed from the collected slice afterwards.
  The output is unchanged, but the second pass over the sequence is gone: it
  re-decoded every entry and re-fetched every metadata chunk of the file
  sequence (`chunks/` read on the first pass, snapshot cache hit on the second),
  which `list -files` paid for once per revision.
- **Stop expanding the chunk hash sequence in `list -files`.** The `showFiles`
  branch loaded all three sequences of every revision through
  `DownloadSnapshotSequences`, but the file walk reads only the length sequence;
  the chunk hash sequence is printed only by `-chunks`, which expands it itself
  through `GetSnapshotChunks`. Dropping it removes one metadata-chunk fetch per
  revision (`FindChunk` + `DownloadFile`, plus the cache write). **Implemented**:
  `list -files` now calls `DownloadSnapshotSequence(snapshot, "lengths")`. See
  the `list -files` section above for the measurements; on the 300-revision
  drvfs fixture this halves the cold run and the chunk-cache entries. Guarded by
  `TestListFilesWalksTheFileSequenceOnce` (now asserting the two sequences that
  are read are fetched exactly once) and
  `TestListChunksStillFetchesTheChunkSequence` (asserting `-files -chunks` still
  reads all three).
- **Let backends list only direct children of `snapshots/`** (B2 and other
  prefix-based backends) instead of scanning the whole subtree. **Implemented**:
  `B2Storage` and `AzureStorage` were the two remaining backends whose
  `snapshots` listing was a flat prefix scan, so they returned every snapshot
  file of every id and deduplicated the ids client-side. Both now pass the
  delimiter their object store offers (`/`), which makes the service fold each
  subdirectory into a single entry and stop descending into it: B2 returns a
  `"folder"` entry per id and Azure a `BlobPrefix`, and the id listing reads the
  direct children directly. `GCSStorage`, `S3Storage`, `S3CStorage`,
  `SwiftStorage`, `StorjStorage` and `HubicStorage` already used a delimiter or
  a non-recursive listing, so they are unchanged. On a bucket with many ids and
  revisions `list -all` no longer pays for a full `snapshots/` traversal before
  each id is visited; the B2 case additionally stops paging through
  `b2_list_file_names` 1000 names at a time. The listing of a single id
  (`snapshots/<id>`) is unaffected, because it is restricted to that directory
  either way. Add `TestB2ListSnapshotsListsOnlyDirectChildren` and
  `TestAzureListSnapshotsListsOnlyDirectChildren`; they serve the prefix and
  delimiter semantics from a test endpoint and assert the delimiter, the
  restricted prefix, that one entry per id came back and that the ids are
  correct.
- **Introduce a lightweight revision index** instead of one file per revision, so
  listing a repository becomes a single object read. **Not going to be
  implemented**: this is the only candidate that would change the storage format
  on disk, and that is out of scope. The revision list is the directory listing
  itself: one object per revision at `snapshots/<id>/<revision>`, written by
  `backup` (`src/duplicacy_backupmanager.go:1140-1141`) and `copy` (`:1782-1783`),
  enumerated by `ListSnapshotRevisions`
  (`src/duplicacy_snapshotmanager.go:498-521`), existence-checked by `copy`
  (`src/duplicacy_backupmanager.go:1632-1633`) and deleted one file at a time by
  `prune` (`src/duplicacy_snapshotmanager.go:2378-2379`). An index would have to
  replace all of those, would need a compatibility path for existing
  repositories and old clients, and would trade the independence of revisions
  (which is what makes the concurrent downloads of fix #4 safe without locks) for
  a shared mutable object. `list` therefore keeps paying one directory listing
  plus one download per revision.

### Deliberately not pursued

- **Overlap the metadata chunks inside a revision** by giving the chunk operator
  that `-files` and `-chunks` use the `-threads` count instead of a hard-coded 1
  (`src/duplicacy_snapshotmanager.go:850`). The sequences of a revision span
  several chunks once a repository holds many files, so they look like the
  `prune` sequence expansion, which is overlapped. Measured with the count wired
  through: on a 40-revision repository whose sequences are 75+ chunks each, on
  drvfs, `list -files` took 3.99 s / 3.87 s / 3.84 s at 1 / 4 / 8 threads before
  and 3.85 s / 3.92 s / 3.84 s after; on the 300-revision drvfs fixture it was
  4.26 s / 4.02 s / 4.08 s before and 4.37 s / 3.84 s / 4.15 s after; both
  changes are inside the run-to-run spread. **Not implemented**: it adds a
  thread-count dependency with no measured gain, so `list` keeps creating the
  operator with one thread.
