# Why `copy` is slow

Investigation into the performance of `duplicacy copy`. This document records
the findings and the candidate fixes, in the style of `snapshot_perf.md` and
`init_perf.md`; candidate fix #1 (don't re-encode chunks that are already
identical) has since been implemented in `src/duplicacy_config.go`,
`src/duplicacy_chunk.go`, `src/duplicacy_chunkoperator.go` and
`src/duplicacy_backupmanager.go`. The two items that follow it — the redundant
per-chunk destination check and the unconditional destination chunk listing —
were implemented together in the same files, since neither is safe on its own.
Candidate fix #4 (check the destination revisions with one listing) and
candidate fix #5 (create the snapshot directory once per id) are implemented as
well. Candidate fix #6 (raise the thread defaults) is not going to be
implemented: the defaults stay at 1, and the flags remain the opt-in.

## Summary

`copy` re-encodes every chunk it moves. It downloads each chunk from the source,
decrypts and decompresses it, then compresses and encrypts it again for the
destination and uploads it. When the two storages share their keys — always for
an unencrypted pair, and for an encrypted pair created with `-bit-identical` —
the destination chunks come out byte-for-byte identical to the source, verified
below, so the whole decompress/recompress cycle is overhead. What it buys in
general is only the erasure-coding and fresh-key re-application that the other
configurations need.

The rest of the cost is the usual I/O shape:

- For every chunk, `UploadChunk` re-checks the destination storage with
  `FindChunk` even though `copy` has already listed every chunk the destination
  holds, and only the chunks missing from that listing are handed to the
  uploader. On a local filesystem that is an extra `newfstatat` per new chunk;
  on a cloud storage it is an extra round trip per new chunk. (Fixed: the
  uploader is told the chunk is absent once `copy` has established that.)
- `FileStorage.UploadFile` calls `fsync` on every chunk. On ext4 one `fsync`
  costs roughly as much as the rest of the per-chunk work put together, and it
  dominates the whole command; on a slow mount it is the single largest cost.
- The destination chunk listing (`ListAllFiles(storage, "chunks/")`) is
  unconditional and walks every chunk directory, so its cost grows with the
  destination, not with the number of chunks being copied. `backup` already
  guards the same listing with `IsFastListing()`; `copy` does not. (Fixed: the
  listing is weighed against the per-chunk lookups.)
- Each snapshot file is uploaded one at a time with its own `CreateDirectory`
  call, which is a redundant round trip for every revision on a backend where
  `CreateDirectory` is a `Stat` + `Mkdir` pair. (Fixed: the directory is created
  once per id.)
- The destination is asked whether it already holds every source revision with a
  `GetFileInfo` per revision, when one listing of its snapshot directory would
  answer for all of them. (Fixed: one listing per snapshot id.)

The change with the best cost/benefit is the redundant per-chunk destination
check: it is correctness neutral, it removes a round trip per new chunk on cloud
storage, and it was verified to pass the existing copy integration test. The
`fsync` question is worth far more locally but is a deliberate durability
trade-off, so it should not be changed without a decision on that. The
per-revision destination check is the same kind of change applied to the
revisions instead of the chunks: one listing per snapshot id, and it was verified
not to re-upload a revision.

Candidate fixes #1 through #5 have since been implemented. #2 and #3 became one
change: each is only safe while the other finds the chunks the destination holds,
so `copy` now chooses between them per storage and keeps exactly one in effect.
#4 reuses the same listing helper on the destination side to enumerate the
revisions once per snapshot id. #6 is not going to be implemented: the thread
defaults stay at 1.

## Conclusion

For a local-to-local copy the command is dominated by `fsync` and by the
re-encoding of bytes that are already identical on both sides:

| Case | Per command | Notes |
| --- | --- | --- |
| `copy`, 12,585 chunks, ext4 | 38-50 s | ~12,600 `fsync` calls |
| same, with `fsync` skipped | ~2 s | 20x faster; durability trade-off |
| same, tmpfs (where `fsync` is free) | ~1.9 s | the re-encode CPU |
| `copy`, 12,253 chunks, drvfs | 300-420 s | 190-240 s even without `fsync` |

Against cloud storage the picture is different: `fsync` is a no-op, the
re-encode is CPU that overlaps with the network, and the remaining per-chunk
round trips are what matter. There, the redundant destination `FindChunk`
(two round trips per new chunk instead of one) and the full destination chunk
listing are the costs worth removing.

## The call path

`copySnapshots` (`duplicacy/duplicacy_main.go:1232`) parses `-threads` and
`-download-threads` (both default 1), creates the source and destination
storages, and calls `BackupManager.CopySnapshots`
(`src/duplicacy_backupmanager.go:1562`).

`CopySnapshots` runs in four phases:

```go
// 1. For each snapshot id and revision: list the destination's revisions (unless -r named them), download the source snapshot.
listDestination := len(revisionsToBeCopied) == 0
if listDestination {
    destinationRevisions, err := otherManager.SnapshotManager.ListSnapshotRevisions(id)  // :1633  once per id
    ...
}
for _, revision := range revisions {
    if listDestination { exist = otherRevisionMap[revision] } else { exist = GetFileInfo(...) }  // :1660  only with -r
    if exist { ... continue }
    snapshot := manager.SnapshotManager.downloadSnapshot(id, revision, true, ...)  // :1676
    snapshots = append(snapshots, snapshot)
}

// 2. Collect the hashes the snapshots reference, then list every destination chunk.
otherChunkFiles, otherChunkSizes := otherManager.SnapshotManager.ListAllFiles(otherManager.storage, "chunks/")  // :1703
...
for chunkHash := range chunks {
    if _, found := otherChunks[otherManager.config.GetChunkIDFromHash(chunkHash)]; !found {
        chunksToCopy = append(chunksToCopy, chunkHash)
    }
}

// 3. Download each missing chunk, re-encode it, upload it.
for i, chunkHash := range chunksToCopy {
    chunkID := manager.config.GetChunkIDFromHash(chunkHash)        // :1757  for a debug log only
    newChunkID := otherManager.config.GetChunkIDFromHash(chunkHash) // :1758  for a debug log only
    chunkDownloader.DownloadAsync(chunkHash, i, chunks[chunkHash], func(chunk *Chunk, chunkIndex int) {
        newChunk := otherManager.config.GetChunk()
        newChunk.Reset(true)
        newChunk.Write(chunk.GetBytes())                  // plaintext -> plaintext
        newChunk.isMetadata = chunks[chunk.GetHash()]
        chunkUploader.Upload(newChunk, chunkIndex, newChunk.isMetadata)  // re-encrypts
        manager.config.PutChunk(chunk)
    })
}

// 4. Upload the snapshot files.
for _, snapshot := range snapshots {
    description, _ := snapshot.MarshalJSON()
    otherManager.SnapshotManager.UploadFile(path, path, description)   // :1783
}
```

Phases 1 and 2 are serial. Phase 3 is the only phase that overlaps work, and
only at the granularity the two thread flags ask for. Phase 4 is serial and
uploads one snapshot file per revision.

Phase 2 has since changed shape: the destination is either listed, as above, or
each chunk is looked up before it is enqueued for download. Both paths build the
same `chunksToCopy`, so phases 3 and 4 are untouched.

## Chunks are re-encoded for no reason

`CopySnapshots` never moves a stored chunk file. It downloads the chunk,
`Decrypt`s it (which strips the banner, AES-GCM and the LZ4/zstd/zlib wrapper,
`src/duplicacy_chunk.go:456`), copies the plaintext into a fresh chunk, and
`Encrypt`s it again with the destination key
(`src/duplicacy_chunkoperator.go:557`).

Measured on a 12,585-chunk repository copied between two unencrypted local
storages, every destination chunk file is byte-identical to the source:

```
identical: 12585 different: 0 missing: 0
```

So for two compatible storages with the same encryption and erasure-coding
settings, the decrypt/compress/encrypt round trip is redundant: the exact bytes
that were read could have been written. A microbenchmark of the round trip on
5 KB chunks gives **~198 us per chunk**, which at 12,585 chunks is ~2.5 s of CPU
— consistent with the ~2 s a tmpfs copy takes, where `fsync` is free and the
command is pure CPU plus a memcpy.

`copy` cannot simply byte-copy in general: it has to re-apply erasure coding if
the destination uses it (`otherManager.config.DataShards`, handled inside
`Encrypt`) and re-encrypt under the destination key. But the identity case is
the common one. `add -copy` always carries over the chunk seed and the hash key
(the compatibility parameters `IsCompatibleWith` checks), and with
`-bit-identical` it also carries over the ID, chunk and file keys
(`duplicacy/duplicacy_main.go:2013`, `src/duplicacy_config.go:256-272`), so the
two storages then hold the same chunk bytes under the same names:

- unencrypted-to-unencrypted, which is what was measured, is always byte-identical;
- encrypted-to-encrypted with `-bit-identical` is byte-identical too;
- encrypted without `-bit-identical` gets fresh random keys, so the destination
  chunk names and ciphertext differ and a real re-encode is unavoidable;
- a destination with erasure coding enabled differs as well.

So the first candidate fix applies to the first two cases, which are the ones a
local-to-local or same-provider copy usually is.

## The per-chunk destination check is redundant

`UploadChunk` always asks the storage where the chunk goes and whether it is
already there:

```go
// src/duplicacy_chunkoperator.go:542
chunkPath, exist, _, err := operator.storage.FindChunk(threadIndex, chunkID, false)
```

and on `FileStorage` that is a chain of `GetFileInfo` calls, one `os.Stat` per
nesting level (`src/duplicacy_storage.go:140`, `src/duplicacy_filestorage.go:118`).

`copy` has already answered that question: `chunksToCopy` is built by removing
every hash found in `otherChunks`, which came from listing the destination
(`:1703-1726`). Every chunk handed to `chunkUploader` is known to be missing, so
the `FindChunk` in `UploadChunk` can only repeat the listing.

Counted with `strace` on a fresh destination (88 chunks), the destination side
of each upload emits:

```
newfstatat follow        (the chunk itself)          -> ENOENT   FindChunk
newfstatat NOFOLLOW      (the parent dir, Lstat)     -> ENOENT   UploadFile
newfstatat follow        (the parent dir, MkdirAll)  -> ENOENT
mkdirat                  (the parent dir)
openat                   (the temp file)
newfstatat NOFOLLOW      (the final path)                        -> the rename check
renameat
```

Three of the four `newfstatat` calls are spent before the write, and the first
one — the `FindChunk` that asks the destination whether the chunk is there —
repeats a question `copy` already answered. Counting the destination-chunk
`newfstatat` calls on an 88-chunk copy: 339 with the check, 251 without, i.e.
exactly one per chunk, with the other 251 being the directory bookkeeping that
`UploadFile` needs. On the many-revision fixture (341 chunks, 300 revisions) the
same change drops the total `newfstatat` count from 3,266 to 2,925.

This is worth far more against cloud storage: `FindChunk` on `SFTPStorage` is a
`Stat` (`src/duplicacy_sftpstorage.go:248`), on `S3Storage` a `HeadObject`
(`src/duplicacy_s3storage.go:175`), on `GCDStorage` a Drive `listByName` query,
and it is paid per new chunk, on top of the upload. This is the copy analogue of
the `list` fix that folded the snapshot existence check into the revision
listing (`641cedf`).

## The destination chunk listing is unconditional

```go
// src/duplicacy_backupmanager.go:1703
otherChunkFiles, otherChunkSizes := otherManager.SnapshotManager.ListAllFiles(otherManager.storage, "chunks/")
```

`ListAllFiles` (`src/duplicacy_snapshotmanager.go:550`) walks the whole `chunks/`
subtree, one `ListFiles` per chunk directory, and returns every chunk name and
size. Its cost therefore grows with the destination storage, not with what is
being copied. It is the mirror of what `backup` does, except that `backup` gates
it on `IsFastListing()` and only for an initial backup
(`src/duplicacy_backupmanager.go:190`), while `copy` pays it every time.

Measured with a 100-chunk source and a 12,685-chunk destination on drvfs:

| | time |
| --- | --- |
| full destination listing | 3.8 s |
| listing skipped | 1.9 s |

On a local filesystem that is syscalls; on object storage it is a prefix scan
per chunk directory. `IsFastListing()` is already the codebase's answer for
"this listing is cheap" (`S3Storage`, `B2Storage`, `AzureStorage`, `GCSStorage`,
`SwiftStorage`, `StorjStorage`, `HubicStorage`, `OneDriveStorage`, `ACDStorage`,
`GCDStorage`, `SFTPStorage` return true; `FileStorage`, `SambaStorage`,
`DropboxStorage`, `WebDAVStorage`, `FileFabricStorage` return false), so the
same guard applies unchanged.

Note that the listing is also a correctness crutch: it is what tells `copy`
which chunks already exist. Removing it is only safe together with the previous
item — otherwise nothing would know what the destination already has, and
either every chunk would be re-uploaded or the per-chunk `FindChunk` would
become load-bearing again.

**Implemented**, together with the previous item:

- The listing runs when `IsFastListing()` is true, or when `chunks/` holds fewer
  directories than there are chunks to copy. One `ListFiles` call counts them,
  since the entries of `chunks/` are the chunk directories under the single-level
  nesting a modern storage uses. The older multi-level layout undercounts, which
  can only leave the listing in place.
- When the listing does not run, each chunk is looked up with `FindChunk` in
  `CopySnapshots` itself, before it is enqueued for download, and only the chunks
  that are missing are offered to the uploader. Enqueuing first and letting the
  uploader reject the chunk would cost a full download of bytes the destination
  already holds. A zero-length file counts as absent, as in the listing.
- Either way the result contains only absent chunks, so
  `ChunkOperator.skipChunkCheck` is set and `UploadChunk` derives the path with
  the new `Storage.ChunkPath` instead of calling `FindChunk`. `backup` leaves the
  check enabled, since nothing else skips a chunk another client uploaded.

`ChunkPath` returns the path at `writeLevel`, the path `FindChunk` reports for a
missing chunk, and fails with the same "Invalid chunk nesting setup" error when
the write level is not among the read levels. Covered by `TestCopyChunkProbe` and
`TestChunkPath` (`src/duplicacy_copymanager_test.go`): the probe test copies an
unchanged revision a second time in each mode and checks the lookup counts, the
uploads and the source downloads.

## `fsync` on every chunk

`FileStorage.UploadFile` syncs every file it writes
(`src/duplicacy_filestorage.go:194`), and it is called once per chunk. That is
one `fsync` per chunk, plus one per snapshot file:

```
341 chunks + 300 snapshot files = 641 fsync and 641 renameat, exactly
```

measured on the many-revision fixture. The cost of a single `fsync` on ext4 here
is ~2.7-5.4 ms (a standalone benchmark of 5,000 write+sync+rename cycles), so
12,585 chunks is 35-70 s of `fsync` alone, which is the whole command. Removing
it makes a 12,585-chunk copy go from 38-50 s to ~2 s:

```
                       ext4, 12,585 chunks
  HEAD                38.2  39.1  44.5  49.9  45.1
  no fsync             2.01  1.89  2.01  1.96  2.04
```

On tmpfs `fsync` is free, so both variants take ~1.9 s — which is how the
re-encode cost and the `fsync` cost can be told apart.

This is not a clear-cut fix. `bb652d0` added the sync deliberately, for
durability of uploaded chunks, and the `init` investigation reached the opposite
conclusion for the storage config file (one file per init, durability worth
more than the syscall). The difference here is the 12,585-to-1 ratio. Options
worth considering, in decreasing order of aggressiveness: sync only metadata
chunks and snapshot files; batch the syncs; keep the sync but let the operator
turn it off. It deserves a decision rather than a silent change.

## Per-revision destination check and directory creation

Two smaller per-revision costs sit in phase 1 and phase 4:

- `GetFileInfo(0, snapshotPath)` at the old `:1633` checked the existence of
  every revision at the destination, even though `ListSnapshotRevisions` returns
  the destination's revisions in **one** directory listing. On drvfs, with a
  fully populated destination of 300 revisions, replacing the per-revision check
  with one listing per snapshot id halves the command: 0.42 s to 0.20 s.
  **Implemented**: `CopySnapshots` calls
  `otherManager.SnapshotManager.ListSnapshotRevisions(id)` once per snapshot id
  and filters the source revisions against that set. A missing destination
  directory reads as an empty one on every backend, so a destination that has
  never held the id still copies everything. A copy restricted with `-r` names
  its revisions, so there is nothing to enumerate and the listing would read
  every revision the destination holds instead of the few that were named; that
  case keeps the per-revision check, the same choice the snapshot commands make
  between a revision that was listed and one the user named. Covered by
  `TestCopyDestinationRevisionCheck` (`src/duplicacy_copymanager_test.go`): a
  three-revision copy to an empty destination lists once, makes no per-revision
  check and uploads all three revisions; a second copy uploads none; and a copy
  restricted with `-r` lists nothing and checks exactly the revision it named.
- `SnapshotManager.UploadFile` (`src/duplicacy_snapshotmanager.go:2772`) creates
  the containing directory before every snapshot file it writes, so a copy of
  300 revisions issues 300 `mkdirat` calls for one long-existing directory. On
  `SFTPStorage` that is a `Stat` + `Mkdir` pair per revision
  (`src/duplicacy_sftpstorage.go:288-305`). The directory only has to be created
  once per snapshot id. **Implemented**: `SnapshotManager` keeps the directories
  it has created and `UploadFile` skips the call for one it has already made, so
  a copy creates `snapshots/<id>` once and writes every revision into it.
  `backup`, which writes one snapshot file per run, is unaffected.

## Thread defaults

`copy` defaults both `-threads` and `-download-threads` to 1. The download side
is a real serial bottleneck: on ext4 with 12,585 chunks, `-threads 16` takes
13.6 s against 37.9 s at one thread. Unlike `list`, where the `-threads` flag was
added with a default of 1 to leave the request pattern alone, `copy` already has
the flags, so the default could have been changed. **It stays at 1**: a bare
`copy` would otherwise start several connections to whatever storage it is
pointed at, which is the user's decision to make rather than a side effect of
making the command faster. The flags remain the opt-in for a copy that wants the
speed.

## Candidate fixes

Ordered by expected benefit.

- **Don't re-encode chunks that are already identical.** Track whether the two
  configs agree on encryption, compression and erasure coding, and in that case
  stream the downloaded ciphertext straight to the destination upload instead of
  `Decrypt`/`Encrypt`. This is the whole CPU cost of the command. It needs care:
  the chunk id is currently re-derived during `Encrypt`, so the identity check
  that `Encrypt` performs would have to move or be reproduced. Saves ~2 s per
  12.5 k chunks locally, and more where compression is expensive.
  **Implemented**: `Config.IsBitIdenticalWith` (`src/duplicacy_config.go`)
  reports whether the two storages store a chunk in the same bytes — the same
  compression level, `HashKey`, `IDKey` and `ChunkKey`, the same erasure-coding
  parameters, and no RSA key on either side, because RSA encrypts each chunk
  with a fresh random key. `CopySnapshots` sets `rawData` on the download
  operator when that holds, so `DownloadChunk` skips `Decrypt` and marks the
  chunk with `SetRawData(task.chunkHash)`; the copy loop then hands the stored
  bytes to the uploader through `Chunk.WriteRawData`, and `UploadChunk` skips
  `Encrypt` for such a chunk. The identity that `Encrypt` used to establish is
  preserved two other ways: the hash comes from the source chunk instead of
  being recomputed, the id is derived from that hash with the destination's
  `IDKey` (the two are equal here, which is what the predicate checked), and
  `WriteRawData` arms the same in-memory checksum that `Encrypt` would have
  verified, so `VerifyChecksum` still guards the buffer that is about to be
  stored. Unencrypted pairs and `add -copy -bit-identical` pairs take the fast
  path; an encrypted pair without `-bit-identical`, a pair with different
  compression, and any pair with erasure coding or RSA keep the decode/encode
  path unchanged. Covered by `TestCopySnapshots` and `TestIsBitIdenticalWith`
  (`src/duplicacy_copymanager_test.go`): the test restores the copied snapshot
  and, for the bit-identical pairs, compares every destination chunk file with
  the source one byte for byte.
- **Skip the per-chunk destination check, but only where the listing has already
  answered the question.** Give the uploader a way to be told that the caller
  already knows the chunk is absent — either a field set on the
  `ChunkOperator`, or a path helper derived from the write nesting level instead
  of `FindChunk`. `copy` sets it because `chunksToCopy` was filtered against the
  destination. Removes one `newfstatat` per new chunk locally, and one round trip
  per new chunk on cloud storage. **Measured**: on drvfs, 341 chunks, the
  many-revision copy goes from 18-24 s to 17-19 s; the gain is larger relative to
  the total when `fsync` is not dominating, i.e. on cloud storage. Verified to
  pass `integration_tests/copy_test.sh`.
  **Implemented** together with the item below, since "the caller already knows"
  only holds where the listing or the lookups ran: `ChunkOperator.skipChunkCheck`
  is set by `CopySnapshots` after filtering `chunksToCopy`, and `UploadChunk`
  then derives the path through `Storage.ChunkPath`. `backup` keeps the check.
- **Gate the destination chunk listing on its cost, not on `IsFastListing()`
  alone.** Mirror `src/duplicacy_backupmanager.go:190`. When the listing is
  skipped, every chunk is offered to the uploader, which then rediscovers the
  duplicates through its own `FindChunk`, so this only pays off for storages
  where that per-chunk check exists anyway — which is why it has to be considered
  together with the item above rather than on its own. **Measured**: on drvfs
  with a 100-chunk source and a 12,685-chunk destination, 3.8 s to 1.9 s. Against
  a destination that holds nothing the copy needs this is a pure win; against a
  destination that already holds everything, the listing is what short-circuits
  the run, so skipping it must not be done blindly.
  **Implemented**, with one correction: `IsFastListing()` alone is the wrong
  test, because skipping the listing trades the tree walk for one round trip per
  already-present chunk *and* a full download of each, the download being
  enqueued before the uploader sees the chunk (`:1771-1784`). `CopySnapshots`
  counts the chunk directories with one `ListFiles(0, "chunks/")` call, switches
  to the lookups only when there are more directories than chunks to copy, and
  does them before enqueuing the download. An incremental copy of a
  slow-listing destination is still listed.
- **Check destination revisions with one listing instead of `GetFileInfo` per
  revision.** Same idea as the `list` fix `641cedf`, applied to the destination
  side. Measured on drvfs: 0.42 s to 0.20 s for a 300-revision no-op copy. The
  destination listing is also what makes the per-chunk check unnecessary, so the
  two changes reinforce each other.
  **Implemented**, with one exception for `-r`: the per-revision
  `otherManager.storage.GetFileInfo(0, snapshotPath)` is gone for a full copy of
  the id, which obtains the destination's revisions from one
  `ListSnapshotRevisions(id)` and looks each source revision up in that set. The
  listing enumerates the same directory the per-revision checks probed, so it
  replaces a round trip per revision with one listing per id, and the filter
  behaves the same when the destination has never held the id, since a missing
  directory reads as an empty one. A copy restricted with `-r` names its
  revisions and keeps the per-revision check, because listing an id with many
  revisions to answer a question about a few of them is the one case where the
  listing costs more than the checks. Verified that no revision is uploaded
  twice, that a restricted copy still copies exactly the requested revisions and
  that it lists nothing.
- **Create the snapshot directory once per id, not per revision.** Move the
  `CreateDirectory` out of `SnapshotManager.UploadFile` into the copy loop, or
  memoise it. Saves a round trip per revision on SFTP and similar backends; near
  free on object stores whose `CreateDirectory` is already a no-op.
  **Implemented** by memoising it: `SnapshotManager.createdDirectories` records
  the directories `UploadFile` has created, and `UploadFile` calls
  `CreateDirectory` only for a directory not in it. This keeps the creation on
  the write path (the `list` fix `d1c1448` moved it there so that read-only
  commands never touch the storage) while removing the repetition: a copy of N
  revisions creates `snapshots/<id>` once instead of N times. `UploadFile` is
  not called concurrently -- it shares `manager.fileChunk` -- so the map needs no
  lock. Covered by `TestCopyDestinationRevisionCheck`, which asserts a
  three-revision copy to an empty destination creates the directory once; the
  assertion fails on the old code, which creates it three times.
- **Raise the default thread counts.** `copy` already has `-threads` and
  `-download-threads`; the serial default leaves 2-3x on the table locally
  (37.9 s to 13.6 s at 16 threads). **Not going to be implemented**: raising a
  default changes the request pattern of every cloud copy that did not ask for
  it, and `-threads`/`-download-threads` are the opt-in for a copy that wants the
  speed.

### Deliberately not pursued

- **Replacing `fsync` outright.** It is a deliberate durability guarantee
  (`bb652d0`) and the right call is a policy decision, not an optimisation.
- **A chunk index or a shared chunk file**, as in `snapshot_perf.md`. `copy`
  reads the destination's chunk layout through `ListAllFiles`, and any index
  would be a storage-format change, which is out of scope for the same reason it
  was rejected there.
- **The two `GetChunkIDFromHash` calls per chunk** at `:1757-1758`. Both exist
  only to build a `LOG_DEBUG` message. At 0.51 us per call a 12.5 k-chunk copy
  spends ~13 ms on them, which is noise, and guarding them on `IsDebugging()`
  would only complicate the loop.
- **Phase 1's snapshot download.** `downloadSnapshot` is already called with
  `listed = true`, so it does not repeat the source existence check (the `list`
  fix `641cedf` covered this); only the destination-side check remains, above.
- **Marshalling the snapshot with `MarshalJSON` per revision.** It is a few
  kilobytes of JSON per revision and does not show up in any profile; the
  directory creation and the upload of the file are what cost.

## How to confirm on a given setup

- `duplicacy -d copy ...` sets DEBUG logging
  (`duplicacy/duplicacy_main.go:146-148`) and prints the `SNAPSHOT_COPY` trace
  lines, including the two `GetChunkIDFromHash` results per chunk; `-v` sets
  TRACE. There is no `COPY_BEGIN`/`COPY_END` pair to bracket the transfer, so
  time the gap between `Chunks to copy:` (`:1728`) and
  `Copied N new chunks` (`:1775`): that interval is the whole chunk-transfer
  phase and nothing else.
- Run the `strace` recipe in `docs/README.md` as `duplicacy copy ...` and sum
  the per-call times. If `fsync` dominates, the destination is a local filesystem
  and the re-encode is not the problem. On a local copy the `fsync` count should
  equal the number of chunks copied plus the number of snapshot files.
- Compare the same copy with the destination on tmpfs: `fsync` is free there, so
  what remains is compress/decompress CPU. The difference between the two is the
  `fsync` cost.
- To see whether the destination was listed, look at the `-d` decision line
  `The destination has N chunk directories for M chunks; listing: <bool>`; the
  `Chunks to copy:` line appends `the destination storage was not listed` when it
  was not. A listed copy makes no per-chunk destination `newfstatat`; an unlisted
  one makes exactly one per chunk, from `CopySnapshots` rather than the uploader.
- Copy between two storages with an empty destination and then `cmp` the chunk
  trees (`diff -rq s1/chunks s2/chunks`). Identical trees mean the re-encode
  produced exactly the input bytes, which is the premise of the first candidate
  fix.
- `-threads N` and `-download-threads N` overlap the transfer; compare against
  the default to see whether the run is latency- or CPU-bound. The output is
  independent of `N`.
- Run `integration_tests/copy_test.sh` for any change here: it copies in both
directions, prunes, and checks both storages afterwards.
- `TestCopyDestinationRevisionCheck` (`src/duplicacy_copymanager_test.go`) counts
  the destination's snapshot listings, per-revision existence checks and snapshot
  directory creations; it fails if the copy goes back to checking each revision
  individually or to creating the directory once per revision.
- `TestCopyChunkProbe` (`src/duplicacy_copymanager_test.go`) covers the same
  ground as a unit test: it copies an unchanged revision in both modes and
  asserts the destination lookups, the uploads and the source downloads, so it
  fails if either mechanism is dropped without the other taking over.
