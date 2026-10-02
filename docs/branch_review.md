# Duplication and refactoring review of the branch

A review of the code added since the branch point, looking for duplicated code and
for refactorings that would make it easier to follow. The baseline is
`upstream/master`, since the two commits on `origin/master` are also part of the
branch.

Line numbers are from the state of the tree when the review was written.

## Summary

The branch is 30 commits ahead of `upstream/master`: about 7,000 added lines over
23 non-test source files, 8 test files, and 7 new documents, plus `AGENTS.md` and
the module files. Almost all of it serves one goal — stop `prune`, `copy` and
`list` paying a round trip per revision, and drop a redundant `fsync`.

The changes work, and the per-backend listing work is factored well. The nine
items below have since been applied. The only duplication left is the two entries
under "Left alone deliberately", both judged not worth the churn.

Ordered by payoff:

1. Concurrency helper (applied) — the fan-out is now one helper.
2. Raw-chunk copy plumbing (applied) — the raw flag has one producer.
3. Path and sequence helpers (applied) — the storage layout and the chunk-id walk
   each have one definition.
4. Cloud-file detection (applied) — the denial predicate and the sentinel each have
   one definition.
5. Empty-listing rationale (applied) — the missing-directory explanation is written
   once.
6. Chunk-operator shutdown (applied) — one deferred shutdown replaces the five
   copied blocks.
7. Test scaffolding (applied) — the shared helpers and the storage doubles each
   live in one place, and the repeated recovery block is one deferred helper.
8. Documents (applied) — the document set has an index and a stated skeleton, and
   the shared `strace` recipe is written once.
9. Smaller items (applied) — the two config predicates share a chunk-hash helper,
   the cache-only write documents its placement, and the cloud-file skip reads the
   sentinel alone.

## Duplicated code

### Worker fan-out with panic capture — applied

`runConcurrently` now holds the pattern for every loop that only needs "first panic
wins and is re-raised in the caller": `expandSnapshots`, `downloadSnapshots` and
`benchmarkRun` (which used to be the channel-based variant and is now gone). What
remains is only the prune snapshot deletion, which also needs per-index completion
because it emits its log lines in revision order:

- `src/duplicacy_snapshotmanager.go` (prune snapshot deletion) — the same pattern
  plus the ordered-emission condition variable.

`DownloadSequence` still uses `WaitGroup` with `DownloadAsync`, which is a different
shape: the work is submitted to the chunk operator rather than driven by counting
indices, so it is out of scope for a fan-out helper.

### `failure`/`failureLock` and the recovery block — now in one place

`runConcurrently` holds the recovery block that `expandSnapshots`,
`downloadSnapshots` and `benchmarkRun` all used to carry. The prune deletion loop
still has its own, because a worker unwound by a panic must still finish its slot to
wake the emitter.

### Mapping the three sequences to chunk ids — applied

`snapshot.FileSequence`, `ChunkSequence` and `LengthSequence` were walked and mapped
through `config.GetChunkIDFromHash` in `expandSnapshots`, `GetSnapshotChunks`,
`GetSnapshotChunkHashes` and `CopySnapshots`, and the file sequence alone was
walked in `CheckSnapshots`.

Done: `Snapshot.MetadataSequences` returns the three sequences as one slice, so
"which chunks does this snapshot reference" has one definition. `expandSnapshots`
still expands per group, but no longer re-derives which fields hold the sequences.
`expandChunkHashes` remains the only variant that downloads. Unit test:
`TestMetadataSequences` in `src/duplicacy_snapshotmanager_test.go`.

### Snapshot path building — applied

`fmt.Sprintf("snapshots/%s/%d", ...)` appeared in six places: the upload in
`UploadSnapshot`, the destination existence check and the snapshot file upload in
`CopySnapshots`, the download in `downloadSnapshot`, and both halves of the prune
deletion.

Done: `snapshotDir(id)` and `snapshotPath(id, revision)` hold the layout and every
builder calls them. `UploadFile` still derives the containing directory from the
path it is handed, because it uploads an arbitrary non-chunk file rather than a
snapshot file, so that derivation is not the snapshot layout. Unit test:
`TestSnapshotPathHelpers` in `src/duplicacy_snapshotmanager_test.go`.

### Chunk id recovered from a listed path — applied

`strings.Replace(x, "/", "", -1)` appeared at seven sites: the backup chunk
listing, the destination chunk listing in `CopySnapshots`, the snapshot cache
clean, the snapshot check, and three places in prune.

Done: `chunkIDFromListedPath` removes the nesting directories in one place. It
deliberately keeps the `.fsl` suffix, because that suffix is what tells a caller an
entry is a fossil rather than a chunk -- the copy's listing relies on it to reject a
fossil with its `len(...) != 64` guard, and the two prune sites that want the id
strip the suffix themselves. Unit test: `TestChunkIDFromListedPath` in
`src/duplicacy_snapshotmanager_test.go`.

### OneDrive cloud-file detection — applied

`src/duplicacy_chunkmaker.go` carried the same read-error block twice, comment
included, and the sentinel string was matched again at
`src/duplicacy_backupmanager.go:485`.

Done: `isCloudFileError(err)` holds the message suffix and the Windows check in one
place, and `cloudFileFailure` names the sentinel both halves share. A read error that
is not the denial still goes through `LOG_ERROR` and aborts, which is what keeps the
skip-the-file path limited to the cloud case. Unit tests: `TestIsCloudFileError`,
`TestChunkMakerReadErrorStillAborts` and (Windows only)
`TestChunkMakerCloudFileDeniedIsSkipped` in `src/duplicacy_chunkmaker_test.go`.

### Missing directory treated as an empty listing — applied

`src/duplicacy_dropboxstorage.go`, `src/duplicacy_gcdstorage.go`,
`src/duplicacy_hubicstorage.go`, `src/duplicacy_onestorage.go`,
`src/duplicacy_sftpstorage.go` and `src/duplicacy_webdavstorage.go` each assert a
backend-specific error and returned `nil, nil, nil` under a byte-identical two-line
comment.

Done: `emptyListing()` in `src/duplicacy_storage.go` holds the rationale, and each
backend calls it once its own "not found" error has been matched. The assertion
still has to stay per backend, as the per-backend file layout requires, so the win
is only in the repeated explanation.

### `chunkOperator.Stop()` and reassignment — applied

The five commands that create an operator each carried the same two-line deferred stop
and reassignment.

Done: `stopChunkOperator()` performs the stop and the reassignment, and the five
command methods (`ListSnapshots`, `CheckSnapshots`, `Diff`, `ShowHistory`,
`PruneSnapshots`) register it with a single `defer manager.stopChunkOperator()`.
The helper has to do the work itself rather than defer it internally, since a
deferred call has to reach the command method's own return.

### Test scaffolding and storage doubles — applied

Roughly 3,000 lines of test code are added. The shared helpers were scattered by
whoever needed them first: `createTestSnapshotManager`, `uploadRandomChunk`,
`createTestSnapshot` and `uploadTestMetadataChunk` in
`duplicacy_snapshotmanager_test.go`; `readChunkTree` and `createChunkDirectories` in
`duplicacy_copymanager_test.go`; `createRandomFileSeeded` in
`duplicacy_backupmanager_test.go`. The tests call across those files.

Six instrumented storages existed — `copyTestStorage`, `countingStorage`,
`deletionTrackingStorage`, `blockedDownloadStorage`, `failingBenchmarkStorage` and
`failingDownloadStorage` — each overriding a different subset with its own mutex and
counters. The `defer recover()` block that reports into `t.Errorf` repeated about 13
times.

Done: the scaffolding shared by the test files now lives in
`src/duplicacy_testutils_test.go`, which holds `readChunkTree`,
`createChunkDirectories`, `createTestSnapshotManager`, `uploadTestChunk`,
`uploadRandomChunk(s)`, `createTestSnapshot`, `uploadTestMetadataChunk` and
`createRandomFileSeeded`. The six storages are one `instrumentedStorage`
whose optional fields turn on the counting, the blocked download and the injected
failures, and whose default behaviour is the promoted `FileStorage`. The repeated
recovery block is `recovering(t)`/`recoveringWithStack(t)`. The tests that assert on
a specific `Exception` (`verifyChunk`, `encryptChunk`, `downloadedSnapshotMissing`,
`recoverPanicFrom`) keep their own recovery, since they inspect the panic rather
than report it.

### Documents — applied

The six new documents share a fixed shape: title, `## Summary`, `## The call
path`, `## Candidate fixes`, `### Deliberately not pursued`, `## How to confirm on
a given setup`. One `strace` invocation was duplicated verbatim in `copy_perf.md`
and `snapshot_perf.md`. An index page and a stated template let the set be read
as a whole; `commands.md` also overlaps the wiki table.

Done: `docs/README.md` is the index. It names each document, states the skeleton
the per-command investigations follow, and holds the single
`strace -f -c -T -e trace=fsync,renameat,openat,newfstatat,mkdirat` recipe that
`copy_perf.md` and `snapshot_perf.md` now point at instead of repeating.
`commands.md` is listed there as the command reference and stays in the repo as the
offline copy of the wiki's command table, so that overlap is recorded rather than
removed.

## Refactorings, in order of payoff

### Concurrency helper (applied)

Done: `runConcurrently` in `src/duplicacy_concurrency.go` now covers the fan-out in
`expandSnapshots`, `downloadSnapshots` and the benchmark command (`benchmarkRun`
was folded in and removed). The recovery block all three carried lives in the helper
too. The prune deletion loop was left alone, because it needs per-index completion
rather than fire-and-forget. Unit tests for the helper are in
`src/duplicacy_concurrency_test.go`.

### Raw-chunk copy plumbing (applied)

The branch added `ChunkOperator.skipChunkCheck`, `ChunkOperator.rawData`,
`Chunk.isRawData`, `Chunk.WriteRawData` and `Chunk.SetRawData`, driven from
`src/duplicacy_backupmanager.go:1822`, `:1830`, `:1859` and
`src/duplicacy_chunkoperator.go:457`.

The redundancy was this: the raw download called `SetRawData(task.chunkHash)`, and
the copy completion then called `WriteRawData(chunk.GetBytes(), chunk.GetHash())`,
which called `SetRawData` a second time. One flag was set through two mechanisms,
so the reader and the writer could disagree with nothing to catch it.

Done: `Chunk.WriteRawData` is now the only constructor for a chunk in stored form
and the only place that sets `isRawData`; it takes the hash the chunk was stored
under, and the copy supplies the `chunkHash` it is iterating over rather than one
recomputed from the bytes. The downloader no longer marks the downloaded chunk as
raw -- that chunk is not the one uploaded, it belongs to the source config and pool
-- and instead records only the hash and id through `Chunk.SetStoredHash`, the
identity half of the constructor, which the reader cannot compute because it skips
the decryption that would have produced them. `ChunkOperator.rawData` therefore
means just "hand back the stored bytes", and the decision that they can be uploaded
as they are lives with the one caller that knows it. Unit tests for both halves are
`TestWriteRawData` in `src/duplicacy_chunk_test.go` and `TestDownloadRawChunk` in
`src/duplicacy_chunkoperator_test.go`; `TestCopySnapshots` still covers the
end-to-end byte-for-byte copy.

### Path and sequence helpers (applied)

Done: `snapshotDir`/`snapshotPath` in `src/duplicacy_snapshotmanager.go`,
`chunkIDFromListedPath` there too, and `Snapshot.MetadataSequences` in
`src/duplicacy_snapshot.go`. Each was small and local, and each replaced copies that
could have drifted apart; all three are covered by the unit tests named in the
sections above.

### The string sentinel in `AddData` (applied)

The sentinel is now the named `cloudFileFailure`, shared by the producer in
`duplicacy_chunkmaker.go` and the consumer in `duplicacy_backupmanager.go`.

## Smaller items — applied

- `IsCompatibleWith` and `IsBitIdenticalWith` compared overlapping field lists, both
  of which re-listed `HashKey`. They now share `sameChunkHash`, and
  `IsCompatibleWith`'s other four comparisons became `sameChunkBoundaries`, so the
  chunk-boundary rule is stated once and `HashKey` is compared through one helper.
  `IsCompatibleWith` reports `sameChunkBoundaries(otherConfig) && sameChunkHash(otherConfig)`.
  Covered by `TestIsCompatibleWith` in `src/duplicacy_copymanager_test.go`.
- `UploadFileNoSync` stays off the `Storage` interface, and its comment now says
  why: the cache is its only caller and is always a `FileStorage`, built by
  `BackupManager.SetupSnapshotCache` with `CreateFileStorage`. The comment states
  the placement rather than only when to call the method.
- The cloud-file skip no longer reads `entry.Size <= 0 && addDataErr ==
  cloudFileFailure` at `src/duplicacy_backupmanager.go:485`; the named sentinel is
  the whole condition, so the negative size is no longer half of a coupling that a
  reader has to reconstruct. The size contract is documented on `cloudFileFailure`.

## Left alone deliberately

- The per-backend `ListFiles` delimiter work in `duplicacy_b2client.go`,
  `duplicacy_b2storage.go` and `duplicacy_azurestorage.go` is already factored
  behind one parameter and is the pattern the rest should follow.
- `ChunkPath` (`src/duplicacy_storage.go:143`) duplicates the path-building loop
  in `FindChunk` (`:161`). The two could share a private helper, but the method and
  its documentation are correct and the duplication is four lines.

## How to apply

The nine items -- the concurrency helper, the raw-chunk copy plumbing, the path and
sequence helpers, the cloud-file detection, the empty-listing rationale, the
chunk-operator shutdown, the test scaffolding, the documents and the smaller items
-- have all been applied. The unit tests in `src/` are the safety net:
`go test ./src/ -vet=off`. Note the two tests that fail on a pristine checkout for
unrelated reasons (`TestEntryExcludeByAttribute`, `TestPersistRestore`), and the
pre-existing `go vet` warnings, both documented in `AGENTS.md`.
