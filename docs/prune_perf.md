# Where `prune` spends its time

Investigation into the performance of `duplicacy prune`, in the style of
`snapshot_perf.md`, `init_perf.md` and `copy_perf.md`. Five issues were worth
fixing and have now been implemented: the serial revision loop
(`src/duplicacy_snapshotmanager.go`), option 2 below (`src/duplicacy_filestorage.go`
and `src/duplicacy_chunkoperator.go`), the serial sequence expansion
(`src/duplicacy_snapshotmanager.go`), the 100 ms poll in the chunk operator's
completion wait (`src/duplicacy_chunkoperator.go`), and the serial snapshot-file
deletion loop (`src/duplicacy_snapshotmanager.go`). A sixth followed later, once
the filesystem-based cache decision in `docs/check_perf.md` removed what blocked
it: the chunk cache is now guarded by `IsCacheNeeded()` on the read side too, so a
local storage neither reads nor writes it (option 1 below). The remaining
candidates are recorded for a follow-up.

## Summary

`prune` is two commands in one. It reads every snapshot of every id, works out
which chunks are no longer referenced, and rewrites the storage to mark those
chunks as fossils (or delete them outright with `-exclusive`). The reading half
is a loop over revisions, now parallel under `-threads`; the chunk half is a
multi-threaded chunk operator that is already parallel, and it is fed by the
sequence expansion, which is now overlapped under the same flag.

The reading half is cheap against a local filesystem and expensive against
anything where a read is a round trip. It used to be strictly serial: `prune`
accepted `-threads` and used it for the chunk operator, but the snapshot files
were fetched one at a time regardless, so the flag did nothing for the largest
per-item cost in the command. This is the same shape as the fix `6584fb0` made
for `list`, which added `downloadSnapshots`; `prune` was not updated then and
called `downloadSnapshot` directly in its loop. It now calls `downloadSnapshots`
(`src/duplicacy_snapshotmanager.go:2238`), so `-threads 8` takes a
revision-read-dominated run from 0.63 s to 0.31 s (2.0x) with no change at
`-threads 1`.

The chunk half had one real defect. Every metadata chunk it fetched was copied
into the local snapshot cache and read back from there, and that cache write was
a full local-storage write cycle — temp file, `fsync`, `rename`. On a
180-revision repository on ext4 the `fsync` alone turned a real
`prune -r 1-90 -exclusive` from 0.62 s into 0.20 s.

The fix is to keep the cache but stop fsyncing it, for the chunk-cache entries
only. Simply dropping the write is faster still on a fast disk (0.17 s) but loses
more than it gains when a slow mount serves many revisions that share their
metadata, because the cache is what stops those chunks being re-read. Skipping
the `fsync` is only safe where the reader verifies what it finds: a cached
**chunk** is checked against its id on every read and re-fetched from the storage
on a mismatch, so a write torn by a crash is rejected rather than believed.
Cached **snapshot files**, fossil collections and the verified-chunk list are
read back unverified and JSON-parsed, so those keep fsyncing; when their entry is
torn, the first two are reported as errors that stop the command, while the
verified-chunk list is rebuilt by verifying again.

`-exhaustive` is the expensive mode, and for a reason that is inherent rather
than accidental: it must expand the chunk sequences of every revision to build
the referenced set before it can walk the chunk tree, so its cost grows with the
number of revisions and with the size of the chunk tree. Non-exhaustive returns
early when there is nothing to delete and does not pay for the expansion at all
unless a snapshot is actually being removed. The expansion itself is now
overlapped under `-threads`.

A fourth cost is not part of the work at all. `prune` hands its deletions to the
chunk operator and then waits for it, and that wait used to poll a counter every
100 ms. On a fast disk the whole batch finishes in a few milliseconds, so almost
the entire tick was paid for nothing: a real `prune -r 1-3 -exclusive` on a
41-revision ext4 fixture took 142 ms before and 40 ms after, and the 100 ms is
flat rather than proportional to the work. The same wait is shared, so `backup`
and `check` paid it too; the wait is now woken by the task that finishes last.

The last one is the deletion loop. After the chunk phase, `prune` deletes the
snapshot file of every revision it removed, one at a time on the calling
goroutine, so `-threads` did nothing for a delete that is a round trip on a
cloud backend. It now overlaps them, and while it does so it keeps the
`has been removed` lines in revision order.

## Conclusion

Five changes were worth making. The first is the parallel revision loop: the
snapshot files of a revision are independent, and the parallel helper `list`
already used was simply never wired into prune, so `-threads` bought nothing for
the largest per-item cost of the command.

The second is option 2: **keep the cache, but do not `fsync` the
chunk-cache writes**. The defect is real — the chunk operator writes every
metadata chunk to the snapshot cache without checking `IsCacheNeeded()` — but the
write is not pure waste, because the cache is also what stops a metadata chunk
being re-read when many revisions share it. Deleting the write (option 1) is the
fastest choice on a fast disk but the slowest on a slow mount with reuse.
Removing the `fsync` takes out the dominant cost of the write while keeping every
cache hit, and it is the only option that beats HEAD on every fixture measured,
with no case made worse. It was implemented as option 2 above.

The third is the sequence expansion. `-threads` overlapped the revision reads but
not the expansions that follow them, so the largest per-item cost of
`-exhaustive` — one round trip per metadata chunk of every sequence — was still
taken one chunk at a time. The expansions of distinct sequences are now done
concurrently, and a sequence shared by several revisions is expanded once rather
than once per revision. `-threads 4` takes a 40-revision fixture from 0.36 s to
0.30 s locally, and `-threads 1` is unchanged.

The fourth is the completion wait, and it is the cheapest to fix: the operator
knows when its last task finishes, so it signals a condition variable instead of
being polled on a 100 ms timer. `prune -r 1-3 -exclusive` on a 41-revision ext4
fixture goes from 0.15 s to 0.04 s, and because the same helper is used by
everything that queues chunk work, `backup` goes from 0.24 s to 0.03 s on the
same fixture. It costs nothing when the storage is slow, since the wait is what
the round trips take either way.

The fifth is the deletion loop, which was the last per-item serial cost in the
command. It ran after the chunk phase and removed each snapshot file one at a
time, with the same `0` thread index every time. The deletions are independent,
so they now overlap under `-threads`, and because the `has been removed` lines
are user-visible the finished deletions are flushed to the log in revision order
rather than in the order they complete. On a 200-revision repository on a slow
mount, deleting 150 revisions goes from 0.44 s to 0.37 s at `-threads 8`, and
`-threads 1` is unchanged.

The candidates left are worth less:

| Candidate | Scope | Local gain | Cloud gain |
| --- | --- | --- | --- |
| Create `fossils/` only when a collection is saved | every `prune` | one `mkdirat` | one round trip |
| Skip the `nesting` probe | every command | one `stat` | one round trip |
| Filter `-t` before the snapshot files are downloaded | `prune -t` | none | one round trip per filtered revision |

`prune` is not like `init`, which is a fixed handful of operations: it scales
with revisions and with the chunk tree, and it is the command a long-lived
repository runs most often. That is why it is worth optimising, and why the three
scaling costs — the serial revision loop, the per-metadata-chunk cache write and
the serial sequence expansion — were the ones to look at first. All three are now
fixed, along with the fixed completion-wait tick that sat on top of all of them,
and the deletion loop that followed them.

## The call path

`pruneSnapshots` (`duplicacy/duplicacy_main.go:1168`) parses `-threads`
(`:1178-1181`, default 1), creates the storage with that many threads (`:1188`),
sets up the snapshot cache (`:1225`) and calls `BackupManager.SnapshotManager.
PruneSnapshots` (`:1226`).

`PruneSnapshots` (`src/duplicacy_snapshotmanager.go:2108`) does, in order:

```go
manager.CreateChunkOperator(false, false, threads, false)                  // :2123
...
for _, id := range snapshotIDs {                                           // :2224
    revisions, err = manager.ListSnapshotRevisions(id)                     // :2226
    for _, snapshot := range manager.downloadSnapshots(id, revisions, true, threads) {  // :2238  parallel
    }
}
...
if toBeDeleted == 0 && !exhaustive { return false }                        // :2495  early out
...
success = manager.pruneSnapshotsExhaustive(...)                            // :2504
        || manager.pruneSnapshotsNonExhaustive(...)                        // :2506
manager.chunkOperator.WaitForCompletion()                                  // :2512
for ... { manager.storage.DeleteFile(...) }                                // :2550  parallel snapshot deletes
manager.CleanSnapshotCache(latestSnapshot, allSnapshots)                   // :2714
```

The chunk selection and fossilization differ by mode:

- `pruneSnapshotsNonExhaustive` (`:2725`) builds `targetChunks` from the
  snapshots being deleted, then walks the snapshots that are being kept and
  marks the ones that are still referenced. It only expands the sequences
  (`expandSnapshots`, which calls `GetSnapshotChunks` and downloads the chunk
  sequences) of the snapshots flagged for deletion (`:2759`) and of the ones
  being kept (`:2766`).
- `pruneSnapshotsExhaustive` (`:2799`) expands the sequences of every snapshot
  that is *not* flagged (`:2831`) and then lists the whole chunk tree
  with `ListAllFiles` (`:2838`).

So a non-exhaustive prune with nothing to delete stops at `:2495` after the
snapshot files have been read. An exhaustive prune always expands every
sequence and always walks the chunk tree, which is what makes it the slow mode.

## The revision loop used to be serial and ignore `-threads` — fixed

This was the first of the three scaling costs, and is now implemented. `prune`
creates its chunk operator with `threads` (`:2123`) but used to download the
snapshot files through `downloadSnapshot` one at a time (now removed).
The parallel helper that `list` uses, `downloadSnapshots`
(`src/duplicacy_snapshotmanager.go:766`), was right there but was not called from
the prune path. The commit that added it (`6584fb0`) rewrote `list`'s call sites
and left prune's:

```
-			snapshot := manager.downloadSnapshot(id, revision, true)
+			snapshot := manager.downloadSnapshot(id, revision, true, manager.fileChunk, 0)
```

— a signature fix, not a switch to the parallel helper.

Measured on a drvfs mount with 500 revisions and 4 chunks (so the run is almost
all revision reads), `/usr/bin/time`, best of three:

| Build | `-threads 1` | `-threads 8` |
| --- | --- | --- |
| HEAD (`downloadSnapshot` loop) | 0.83 s | 0.84 s |
| prototype using `downloadSnapshots` | 0.85 s | 0.47 s |

The flag had no effect on HEAD and a 1.8x effect once the loop is parallel. On
ext4 the same 500 revisions take 0.02-0.03 s either way, because a revision is
two syscalls and there is nothing to overlap — the same conclusion `list`
reached, which is why that flag also defaults to 1.

### How it was implemented

The loop now calls the helper, keeping the snapshots in revision order:

```go
var snapshots []*Snapshot
for _, snapshot := range manager.downloadSnapshots(id, revisions, true, threads) {
    if snapshot != nil {
        snapshots = append(snapshots, snapshot)
    }
}
```

The revision is known to exist because it came from `ListSnapshotRevisions`, so
`listed` is true and the existence check inside `downloadSnapshot` is skipped, as
it was before. Like `list`, `prune` keeps creating its storage with `threads`
(`duplicacy/duplicacy_main.go:1188`), which it already did, so the storage
accepts the thread indexes the workers pass.

Re-measured A/B on the same fixture as the table above (a local ext4 repository
with 301 revisions, none sharing a metadata chunk), 6 runs per cell, with the two
binaries alternating so the run order does not bias the result:

| Build | `-threads 1` | `-threads 4` | `-threads 8` |
| --- | --- | --- | --- |
| HEAD | 0.627 s | 0.647 s | 0.627 s |
| with the fix | 0.627 s | 0.323 s | 0.305 s |

The flag is flat on HEAD and `-threads 4`/`-threads 8` roughly halve the run once
the loop is parallel. `-threads 1` is unchanged, which is what the helper's
single-threaded branch gives: it falls back to the same `downloadSnapshot` call,
so a default `prune` takes the same path and the same order as before. This is a
local-filesystem fixture, so the win here is not the round trips that the drvfs
measurement isolates; it is the overlap of the per-revision work.

`prune` with more than one id downloads each id's revisions in turn, so the
concurrency is per id; ids are few and a single-revision id gains nothing, which
is why the helper returns early for `len(revisions) <= 1`.

### Correctness

- `prune -exhaustive -dry-run` prints byte-identical output on HEAD and with the
  fix (301-revision repository; `diff` reports no difference, no normalisation
  needed).
- A real `prune -r 1-75 -exclusive` against a pristine copy of the same storage
  produces a tree that `diff -rq` reports identical: the deletions, the fossils
  and the fossil collection are the same regardless of how many revisions were
  read at once.
- `TestPruneDownloadsRevisionsConcurrently` pins the overlap down: it fails with
  "at most 1 was in flight at a time" when the loop is put back, and it also
  checks that the workers never use a thread index beyond the number the storage
  was created with. It joins `TestDownloadSnapshotsConcurrently`, which covers
  the helper itself.

## The metadata cache is written even when the storage needs no cache

This is the largest finding. `ChunkOperator.DownloadChunk` treated the snapshot
cache as if it were always required:

```go
if task.isMetadata && operator.snapshotCache != nil {                    // :331  no IsCacheNeeded()
    chunk.Reset(true)
    cachedPath, exist, _, err = operator.snapshotCache.FindChunk(...)     // :339
    ...
}
...
if chunk.isMetadata && !chunk.isRawData && len(cachedPath) > 0 {          // :526  no IsCacheNeeded()
    err := operator.snapshotCache.UploadFileNoSync(threadIndex, cachedPath, chunk.GetBytes())
}
```

`FindChunk` returns a path whether or not the chunk was found, so
`len(cachedPath) > 0` is always true when a cache exists: **every** metadata
chunk is written to `.duplicacy/cache/<storage>/chunks/`, and read back before
the storage is consulted. On a cold cache that is one miss plus one write per
metadata chunk. The write at `:526` is the one the fix targets; it is now
`UploadFileNoSync`.

Every other cache access in this area is guarded. `SnapshotManager.downloadFile`
guards the read (`:3028`) and the write-back (`:3073`), and `UploadFile` guards
its write (`:3109`). The `list` investigation implemented exactly this rule for
the snapshot files (`snapshot_perf.md`, candidate #1: "Do not write the snapshot
cache when `IsCacheNeeded()` is false"). The chunk operator was not part of that
change.

`IsCacheNeeded()` is false for local file storage: `CreateStorage` leaves
`isCacheNeeded` off for a plain path and a `flat://` URL, and turns it on only
for the `\\` UNC form and `samba://` (`src/duplicacy_storage.go:258-283`). Every
cloud backend in `src/` returns true. So the affected configuration is a local
or network filesystem that has declared itself cache-free.

### What the unnecessary write costs

A local-storage write is not a single syscall. `FileStorage.UploadFile`
(`src/duplicacy_filestorage.go:151`) does `Lstat`, `MkdirAll` on the parent,
create a randomised `*.tmp`, write, **`fsync`** (`:210`), close and `rename`.
With `strace -f -c` on a 150-revision repository (452 chunks) whose revisions all
differ, so 137 metadata chunks are fetched, `prune -exhaustive -dry-run`:

| Syscall | HEAD | option 1 (guard) |
| --- | --- | --- |
| `fsync` | 137 | 0 |
| `renameat` | 137 | 0 |
| `unlinkat` | 136 | 0 |
| `mkdirat` | 110 | 5 |
| `newfstatat` | 1847 (494 failing) | 986 (11 failing) |
| `openat` | 780 | 539 |
| **total** | **14486** | **9316** |

The cache write counts match the metadata chunk count exactly: 137 `fsync` and
137 `renameat` here, and 284 of each in the 300-revision changing-content fixture
used below (284 metadata chunks fetched). That 1:1 match, rather than the totals,
is what identifies the mechanism.

Wall-clock, best of three:

| Fixture | Mode | HEAD | option 1 (guard) |
| --- | --- | --- | --- |
| ext4, 180 revs, 525 chunks | `prune -r 1-90 -exclusive` | 0.62 s | 0.17 s |
| ext4, 180 revs, 525 chunks | `prune -exhaustive -dry-run` | 0.31 s | 0.04 s |
| ext4, 300 revs, 892 chunks | `prune -exhaustive -dry-run` | 0.89 s | 0.07 s |
| drvfs, 150 revs, 458 chunks | `prune -exhaustive -dry-run` | 1.40 s | 1.01 s |

### `prune` is not the only command affected

`DownloadChunk` is shared, so any command that expands a metadata sequence pays
the same write. On the 180-revision ext4 fixture, `list -files` (which expands
the file, chunk and length sequences of every revision) took 0.55-0.67 s on HEAD
and left 181 files in the cache; with the guard it takes 0.06-0.07 s and leaves
none. `check`, `diff`, `history`, `cat` and `restore` go through the same path.
That makes the guarded version worth more than the `prune` numbers alone
suggest — but it also means the change belongs in `duplicacy_chunkoperator.go`,
where it affects all of them, and it should be validated against those commands
rather than only against prune.

Option 2 was validated command by command on a 150-revision repository with 463
chunks, against HEAD, with a pristine copy of the storage restored before each
pair of runs:

| Command | HEAD | option 2 | Result |
| --- | --- | --- | --- |
| `list -files` | 1.08 s | 0.16 s | 6.75x |
| `check` | 0.47 s | 0.09 s | 5.22x |
| `check -chunks` | 0.54 s | 0.16 s | 3.38x |
| `cat`, `diff` | 0.03 s | 0.02 s | ~1.5x |
| `check -files` | 5.29 s | 4.33 s | 1.22x |
| `list`, `history` | 0.01-0.03 s | 0.01-0.03 s | 1.00x (\*) |

(\*) `list` without `-files`, and `history`, only read the snapshot files and
their chunk sequences, which on this fixture is a handful of chunks; the win is
in the commands that expand a sequence per revision. `check -files` is I/O-bound
on the file contents rather than on the metadata, hence the smaller ratio.

For correctness, each command was run twice (once per build) from the same
pristine storage and the outputs and side effects compared:

- `list`, `list -files`, `list -chunks`, `list -all`, `check`, `check -files`,
  `check -stats`, `diff`, `cat` (of a snapshot and of a file) and `history`:
  identical stdout, stderr and exit code, once the throughput/time fields in the
  progress lines are normalised away.
- `check -chunks`: the *set* of chunk hashes verified is identical (463 of 463).
  The line order differs, but HEAD against itself also differs — the verification
  is parallel, so ordering was never deterministic and is not an option-2 effect.
- `restore -r 30` into the same target path: identical output and all 46 restored
  files byte-identical by `sha256sum`, confirming that skipping the cache fsync
  does not feed anything different back into restore.
- The cache is still populated and still reused: after `list -files`, both builds
  leave 329 files in the cache, and a second warm-cache `list -files` takes 0.11 s
  on both. Option 2 changes only when the cache bytes reach the disk.

### Three ways to fix it, and which one to pick

The defect admits three fixes. All three leave the storage byte-identical.

1. **Guard the cache with `IsCacheNeeded()`** (`src/duplicacy_chunkoperator.go:331`
   and `:527`), matching every other cache access in the code. Fastest on a fast
   disk. It used to lose badly when one slow mount serves many revisions that
   share their metadata: with the cache gone, the shared chunks are re-read for
   every revision. That case was a slow mount whose storage had reported
   `IsCacheNeeded()` false, which `docs/check_perf.md` has since fixed by asking
   the filesystem; a network storage now reports true and keeps its cache, so the
   guard only turns the cache off for a local storage, where it was never worth
   having. **Implemented** (see `### Smaller items`).
2. **Keep the cache, but don't `fsync` the chunk-cache writes.** `UploadFile`
   fsyncs every write (`src/duplicacy_filestorage.go:209`), which is the
   expensive part of the cycle; the cache write is a rename-into-place whose only
   requirement is to survive this process. This keeps every cache hit *and*
   removes the dominant cost. **Implemented** (see below).
3. **Split "needs a cache" from "is local"**, so that a mount which is local but
   slow can get the reuse without the fsync. The most correct but the largest
   change, since `IsCacheNeeded()` is a storage property consulted in several
   places. **Not implemented.**

Measured against each other, best of three, `/usr/bin/time`:

| Fixture | HEAD | 1. guard | 2. no `fsync` |
| --- | --- | --- | --- |
| ext4, 180 revs, churn, delete 90 | 0.62 s | 0.17 s | 0.20 s |
| ext4, 150 revs, churn, `-exhaustive` | 0.52 s | 0.06 s | 0.10 s |
| drvfs, 150 revs, churn, `-exhaustive` | 1.44 s | 0.95 s | 1.03 s |
| drvfs, 300 revs, 4 chunks, reuse, `-exhaustive` | 0.56 s | 1.16 s | 0.53 s |

Option 2 is never the slowest and is best or near-best everywhere; the reason
option 1 wins the two fast-disk rows is that it skips the write entirely, at the
cost of more than doubling the run on the reuse row. **Option 2 is the
recommendation**: the only one that improves every measured case, and the
smallest change — one flag on `FileStorage`, set for the snapshot cache where it
is created (`src/duplicacy_backupmanager.go:105`).

Option 2 is a durability question as much as a performance one. It is safe only
where the reader verifies the cache entry before trusting it, and that is true of
the chunk cache alone: `DownloadChunk` re-derives the id of the chunk it loads and
falls back to the storage when it does not match, so a torn write is rejected and
re-fetched. The cached **snapshot files**, **fossil collections** and
**verified-chunk list** are read back unverified and JSON-parsed, so a torn entry
there would be fatal — `list` and `prune` failed outright in a test that truncated
one. Those writes therefore keep fsyncing.

#### How it was implemented

- `FileStorage.UploadFile` keeps fsyncing; a new `FileStorage.UploadFileNoSync`
  goes through the same code with the `fsync` skipped
  (`src/duplicacy_filestorage.go`). The default is unchanged, so every other
  writer — the storage itself, the snapshot-file cache, the fossil collections —
  is untouched.
- The two chunk-cache writers call `UploadFileNoSync`: the one in `DownloadChunk`
  (one per metadata chunk, the write that dominates) and the one in `UploadChunk`.
- Measured end to end with the built binary on a 150-revision repository with 486
  chunks: `prune -exhaustive` 0.65 s → 0.12 s (5.4x), `list -files` 1.57 s →
  0.24 s (6.5x), `check` 0.65 s → 0.12 s (5.4x), `check -chunks` 0.78 s → 0.23 s
  (3.4x). `syscall fsync` per `prune -exhaustive` fell from 142 to 0 while
  `renameat` stayed at 142, which is the signature: the cache is still written and
  renamed into place, only the flush is gone. On a storage that needs a cache
  (`samba://`), where snapshot files are cached too, the count went 6 → 3: the
  chunk-cache fsyncs are gone and the snapshot-file ones remain.
- Correctness: `prune -exhaustive -dry-run` output is identical, a real
  `prune -r 1-75 -exclusive` produces a byte-identical storage tree, and a
  `restore` of all 151 files matches by `sha256sum`.

### Correctness

The implemented change was verified in three ways.

- **Against HEAD, command by command**, from the same pristine storage copy:
  identical stdout, stderr and exit code for `list`, `list -files`,
  `list -chunks`, `list -all`, `check`, `check -files`, `check -stats`, `diff`,
  `cat` (snapshot and file) and `history`; the same 463-element set of verified
  chunk hashes for `check -chunks`; and a `restore` whose 46 files are
  byte-identical by `sha256sum`.
- **At the storage level**: a real `prune -r 1-75 -exclusive` produces a storage
  tree that `diff -rq` reports identical, and `prune -exhaustive -dry-run` prints
  byte-identical output. Expected, since skipping the `fsync` changes when bytes
  reach the disk, not which bytes are written.
- **The safety property the change depends on**: a truncated chunk-cache entry is
  rejected and the chunk re-fetched from the storage. `TestCorruptCachedChunkIsRefetched`
  pins this down — it fails if the id check in `DownloadChunk` is removed — and
  `TestSnapshotCacheSkipsSync` checks that the cache is still written and readable.
  `TestPruneSingleRepository` and `TestPruneSingleHost`
  (`src/duplicacy_snapshotmanager_test.go:1190`, `:1234`) exercise the prune deletion
  path with and without `exclusive`, tagged snapshots and retention policies, and
  `TestCorruptNonChunkCacheEntries` records the read-side behaviour of the three
  cache entries that are *not* covered by the no-fsync change.

What the change gives up is that a chunk-cache entry is no longer guaranteed to
survive a crash. That is the whole point of the trade: the entry lost is
re-derived from the storage on the next read. The cached snapshot files and
fossil collections keep their `fsync`, because they are read back unverified.

## `-exhaustive` expands every sequence

The two modes cost very differently, and the difference is the sequence
expansion. On ext4, best of three, `-dry-run` (read everything, no deletion):

| Fixture | Revisions | Chunks | Non-exhaustive | `-exhaustive` |
| --- | --- | --- | --- | --- |
| unchanged content | 300 | 8 | 0.03 s | 0.07 s |
| changing content | 300 | 902 | 0.03 s | 0.87 s |

With `-d`, the two fixtures differ in exactly one respect that matters: the
changing-content run fetches and writes back 284 metadata chunks, while the
unchanged-content run fetches a single one and serves the other 299 revisions
from the cache. That is the whole difference. The tree walk does grow with the
chunk tree — with `-v`, `LIST_FILES` ("Listing chunks/...") appears 424 times for
the 902-chunk fixture against 11 times for the 8-chunk one, since
`pruneSnapshotsExhaustive` (`:2838`) lists the top directory and every
subdirectory — but it is a small part of the total next to the per-sequence
fetches, and it is the same walk `-delete-only` performs.

Consequences:

- `-exhaustive` cannot take the `:2495` early exit, by design: it has to expand
  the sequences to know what is referenced before it can find dangling chunks
  and temporary files. Scheduling it less often (the wiki recommends it
  periodically) is the practical lever.
- The `-threads` flag did not change the sequence expansion until the change
  described in the next section: each `DownloadSequence` waits for its chunks one
  at a time on the calling goroutine, and the tree walk also runs there. A
  `-threads 1/4/16` sweep on the 300-revision ext4 fixture was flat
  (0.94 s / 0.94 s / 0.95 s). The flag only overlapped the revision reads, which
  on this fixture are a small part of the run.
- The non-exhaustive mode with deletions does pay the expansion too, because it
  expands the sequences of the snapshots being deleted and of the ones being
  kept to compare against. The 0.62 s delete above is mostly that.

## The sequence expansion was serial too — fixed

This is the third scaling cost. `PruneSnapshots` creates its chunk operator with
`threads` (`:2123`), but the expansion never used them: `DownloadSequence`
submitted one chunk at a time and blocked on each completion channel before
submitting the next, so a sequence was fetched serially even with more threads.
And `pruneSnapshotsExhaustive` / `pruneSnapshotsNonExhaustive` walked their
snapshots on the calling goroutine, one sequence after another. On a storage
where a read is a round trip, and an unchanged chunk list is still a stored
sequence, that is one round trip per metadata chunk of every revision, taken
strictly in turn.

The measured effect is in the table above: on the 300-revision changing-content
fixture the expansion is almost the whole 0.87 s of `-exhaustive`, and the
`-threads` sweep was flat. The work is per revision and per chunk, exactly the
shape of the revision-loop fix, so `-threads` now reaches it.

### How it was implemented

`DownloadSequence` (`:319`) submits every chunk of the sequence to the operator
and waits once, keeping the chunks in sequence order:

```go
for i, chunkHash := range sequence {
    manager.chunkOperator.DownloadAsync(chunkHash, i, true, func(chunk *Chunk, chunkIndex int) {
        pieces[chunkIndex] = append(pieces[chunkIndex], chunk.GetBytes()...)
        manager.config.PutChunk(chunk)
        waitGroup.Done()
    })
}
```

The chunks arrive in whatever order the operator finishes them, so each one is
copied out and returned to the pool by the goroutine that completed it, and the
pieces are concatenated by sequence index afterwards. The content does not depend
on the order the downloads finish in, and the chunks do not have to be held until
the last one arrives.

The expansions themselves are driven by `expandSnapshots` (`:356`). It groups
the snapshots by their chunk sequence first: revisions that did not change their
chunk list share one sequence, and expanding each of them separately would fetch
that sequence once per revision. A shared sequence is expanded once and the ids
are handed to every revision that references it, which is what the serial code
got for free from the snapshot cache. Distinct sequences are then expanded by
concurrent workers; a worker that hits an error captures it and it is re-raised
in the calling goroutine, as `downloadSnapshots` (`:850`) already does for the
revisions. The merge of the per-sequence results into the caller's maps is done
by the calling goroutine, so the maps are still written by one goroutine.

The read-only commands (`list`, `cat`, `diff`, `history`) create the operator with
one thread, so `DownloadSequence` takes its single-threaded path there and their
behaviour is unchanged.

### Correctness

- A real `prune -exhaustive -r 1-10 -exclusive` and a plain `prune -r 1-10
  -exclusive` were each run from the same pristine storage by the old and the new
  binary at `-threads 4`: `diff -rq` reports the storage trees identical, and the
  `-v` logs are identical as sets once the timestamps and durations are removed.
- `prune -exhaustive -dry-run` at `-threads 1`, `4` and `8` produces the same
  output; `-threads 1` is unchanged because a single group takes the serial path.
- Measured on a drvfs mount, 40 revisions that each changed their file list (so
  every revision has its own sequence and the expansions are distinct), warm
  cache, 5 runs per cell with the two binaries alternating: 0.37 s / 0.36 s /
  0.39 s at `-threads 1` / `4` / `8` before, and 0.37 s / 0.30 s / 0.32 s after.
  The flag is flat before the change and worth about 1.2x after it. On 300
  revisions that all share their metadata the numbers are 0.58 s / 0.34 s /
  0.36 s before and 0.55 s / 0.33 s / 0.34 s after: there the reuse matters more
  than the overlap, and the grouping is what keeps the change from making it
  worse. On a native filesystem the expansion is a few syscalls per chunk and the
  difference is within noise, as the table above says.
- `TestPruneExpandsSequencesConcurrently` checks that the concurrent expansion
  returns the same chunk lists as the serial one and that the metadata downloads
  really overlap; it fails with "at most 1 was in flight at a time" if the
  workers are put back behind the serial path.
- `TestSharedSequenceIsExpandedOnce` checks that a sequence referenced by eight
  revisions is fetched once, not eight times; it fails with "was downloaded 8
  times" when the grouping is defeated.
- `TestDownloadSequencePreservesOrderConcurrently` checks that a sequence decodes
  to the same bytes however the chunks finish.
- `TestPruneSingleRepository`, `TestPruneSingleHost` and the rest of the prune
  suite (`go test ./src/ -run TestPrune -race`) pass, as does the full suite with
  `-race`.

## The completion wait polled every 100 ms — fixed

This is the fourth cost, and unlike the other three it is not proportional to
anything. `PruneSnapshots` submits its deletions to the chunk operator and then
waits for them (`src/duplicacy_snapshotmanager.go:2512`), and `backup` and
`check` do the same (`src/duplicacy_backupmanager.go:496`/`:1127`,
`src/duplicacy_snapshotmanager.go:1381`). The wait was a poll:

```go
for atomic.LoadInt64(&operator.numberOfActiveTasks) > 0 {
    time.Sleep(100 * time.Millisecond)
}
```

`Stop` (`src/duplicacy_chunkoperator.go:121`) re-used the same loop before it
stopped the workers, so it paid the tick as well.

The operator already knows when its last task finishes, so the 100 ms was only
there because the completion was not signalled. On a fast disk the whole batch
takes a few milliseconds, which means almost the entire tick was spent waiting
for nothing. Measured with `strace -f -e trace=futex,nanosleep`, the tick is a Go
runtime `futex(FUTEX_WAIT, ~99 ms)`: it disappears entirely when the wait is
woken instead of polling.

### What it costs

A real `prune` that deletes something, on a 41-revision ext4 fixture (233 chunks,
no cache), best of six, binaries alternating:

| Invocation | HEAD | signalled |
| --- | --- | --- |
| `prune -r 1-3 -exclusive` | 142 ms | 40 ms |
| `prune -r 1-3` (non-exclusive) | 151 ms | 43 ms |
| `prune -exclusive -r 1-3 -threads 4` | 135 ms | 31 ms |

The cost is a fixed tick, not a scaling one: deleting 1 revision and deleting 20
both take ~148 ms on HEAD. It is also only paid by invocations that end their run
with chunk work still outstanding. A `prune` that deletes nothing exits at the
`:2495` early return and takes ~23 ms, and `-dry-run` and `-delete-only`, which
submit no deletions, are ~40 ms and ~23 ms either way. That is why the
`-exhaustive -dry-run` rows in the tables above are unchanged by this fix.

Because the wait is shared, the same tick sat on top of other commands:

| Command | HEAD | signalled |
| --- | --- | --- |
| `backup` (one changed file) | 239 ms | 28 ms |
| `check -chunks` (warm cache) | 51 ms | 30 ms |

### How it was implemented

The counter moved under a mutex that also guards a condition variable, and the
task that decrements it to zero broadcasts:

```go
func (operator *ChunkOperator) WaitForCompletion() {
    operator.completionLock.Lock()
    for operator.numberOfActiveTasks > 0 {
        operator.idleCond.Wait()
    }
    operator.completionLock.Unlock()
}
```

`AddTask` increments the counter *before* pushing the task onto `taskQueue`, not
after. That ordering is what makes the wait sound: the workers only decrement a
task once it has been received, so if the increment happened after the push a
caller that waited in the gap could see a zero count, return, and miss a task
that was still in the queue. Counting it as outstanding from before the handover
closes that window, and it is why the counter is now described as "queued or
running".

`Stop` now waits through the same helper and is made idempotent with a `stopped`
flag rather than by the `numberOfActiveTasks = -1` sentinel it used before. It
still stops the workers only after the outstanding tasks have finished, which is
what the old loop achieved by spinning first.

### Correctness

- `prune -exhaustive -dry-run` prints byte-identical output on HEAD and with the
  fix, and a real `prune -r 1-5 -exclusive` produces a storage tree that
  `diff -rq` reports identical.
- `TestWaitForCompletionIsWokenNotPolled` pins it down. It holds a chunk download
  in the storage until the test releases it, so the wait cannot be satisfied by a
  timer: on HEAD the test fails with "took 100.475 ms after the last task
  finished; it is waiting on a timer, not on the task", and with the fix it
  passes in well under the 100 ms tick.
- The full suite passes under `-race`, including the operator, prune, copy and
  restore tests, which are the ones that exercise `AddTask`, `Run` and `Stop`
  concurrently. Fifteen repeats of the new test and five of the operator and
  prune tests under `-race` are clean. `go vet ./src/` reports the same
  pre-existing findings as on HEAD, and no new ones.

## The snapshot-file deletions were serial too — fixed

This is the fifth cost, and the last per-item serial loop in the command. After
the chunk phase has fossilized or deleted what it must, `prune` removes the
snapshot file of every revision it flagged:

```go
// Now delete the snapshot files.
for _, snapshots := range allSnapshots {
    for _, snapshot := range snapshots {
        if !snapshot.Flag || dryRun { continue }
        snapshotPath := fmt.Sprintf("snapshots/%s/%d", snapshot.ID, snapshot.Revision)
        err = manager.storage.DeleteFile(0, snapshotPath)   // threadIndex 0, one at a time
        ...
    }
}
```

The loop ignored `-threads` exactly as the revision loop and the expansion did,
and it hard-coded the thread index `0` as well. A delete is a round trip on every
cloud backend (`S3Storage.DeleteFile` is a `DeleteObject` call, and the others
are their equivalent), so deleting a large run of revisions was one round trip
after another.

Measured on a 200-revision repository whose revisions share their metadata, with
the storage on a slow mount and the cache on ext4, deleting 150 revisions:
`strace -f -tt -e trace=unlinkat` shows the 150 storage deletes issued by a
single thread over a 0.285 s span on HEAD.

### How it was implemented

The storage deletions are handed to worker goroutines; the log lines and the
cache removals stay on the calling goroutine, which keeps the `has been removed`
lines in revision order:

```go
deleteErrors := make([]error, len(snapshotsToDelete))
deleteDone := make([]bool, len(snapshotsToDelete))
var deleteLock sync.Mutex
deleteCond := sync.NewCond(&deleteLock)

// A worker deletes one snapshot file and finishes its own slot.
func deleteSnapshotFile(threadIndex int, index int) {
    ...
    deleteErr := manager.storage.DeleteFile(threadIndex, snapshotPath)
    deleteLock.Lock()
    deleteErrors[index] = deleteErr
    deleteDone[index] = true
    deleteCond.Broadcast()
    deleteLock.Unlock()
}

// The caller emits every contiguous run that is now complete, in order.
for nextToRemove < len(snapshotsToDelete) && failedIndex < 0 {
    for nextToRemove < len(snapshotsToDelete) && !deleteDone[nextToRemove] {
        deleteCond.Wait()
    }
    for nextToRemove < len(snapshotsToDelete) && deleteDone[nextToRemove] {
        ready = append(ready, nextToRemove)
        nextToRemove++
    }
    ... // log and drop the cached copy of each ready snapshot
}
```

Two points make it correct. The `deleteDone` slot is what decouples the two
orders: the deletes race, but each one only marks its own slot, and the caller
emits a slot only once every earlier slot is filled, so the output is the serial
loop's output. And the flush is a contiguous prefix rather than a wait for the
whole list, so a line is printed as soon as the gap in front of it closes
instead of being held until the last delete lands. A worker unwound by a
`LOG_ERROR` panic fills its own slot in its `recover`, otherwise the caller would
wait on it forever.

The thread index is the worker's own, so a backend that keeps a per-thread client
or nested directory (`azureStorage.containers[threadIndex]`, B2's upload URLs)
sees the same indexes it already sees from the chunk operator, within the count
the storage was created with.

### Correctness

- `prune -r 1-25 -exclusive` at `-threads 1`, `4` and `8` prints output that is
  byte-identical to HEAD's, both in full and as the ordered run of
  `SNAPSHOT_DELETE` "has been removed" lines, and the storage tree digest is
  identical. The `The chunk ... has been permanently removed` lines that
  `-exclusive` also prints come from ranging over `targetChunks` and already
  differ between two HEAD runs, so they are not compared by order.
- `strace -f -tt -e trace=unlinkat,write` on the 150-deletion run shows the
  storage deletes issued by 8 worker threads over a 0.13 s span, and 140 of the
  150 removal lines written while the deletions were still running rather than
  after the last one.
- Measured A/B, best of nine with the binaries alternating: 0.44 s → 0.37 s at
  `-threads 8` and 0.41 s → 0.35 s at `-threads 4`, with `-threads 1` unchanged,
  so the default path is untouched. On ext4 it is 89 ms vs 87 ms, within noise,
  since a delete is then one syscall.
- `TestPruneDeletesSnapshotsConcurrently` checks that the deletions overlap at
  `-threads 4` and do not at `-threads 1`, and that no worker uses a thread index
  beyond what the storage accepts. `TestPruneSnapshotDeletionOutputStaysInRevisionOrder`
  releases the held deletions newest first and checks the log is still oldest
  first, and `TestPruneStreamsSnapshotDeletionOutput` checks that the oldest
  revision's line is printed while the others are still held. Each fails if the
  serial loop is put back.
- A failed deletion still stops the command at the revision it failed on, and the
  error is raised from the calling goroutine rather than from a worker, so it
  cannot be lost with the worker's goroutine.
  `TestPruneStopsAtTheFirstFailedSnapshotDeletion` pins that down: with revision
  3 failing, revisions 1 and 2 are reported, 3 and 4 are not, and the command
  returns false.

## Smaller items

- **`fossils/` is created unconditionally.** `PruneSnapshots` calls
  `manager.snapshotCache.CreateDirectory(0, "fossils")` at `:2252` on every run,
  including one that ends at the `:2495` early return and creates no collection.
  `FileStorage.CreateDirectory` is an `os.Mkdir` whose `EEXIST` is swallowed
  (`src/duplicacy_filestorage.go:108-115`), so locally it is a wasted `mkdirat`
  (`mkdirat` count 5 with the guard); on a directory-based backend it is a
  `Stat` + `Mkdir` pair per run.
- **The `nesting` probe is paid by every command, prune included.** `prune`
  creates the storage at `duplicacy/duplicacy_main.go:1188` and
  `CreateBackupManager` downloads the config, which ends in
  `storage.SetNestingLevels(config)` (`src/duplicacy_config.go:493`) and probes
  for a file named `nesting` that nothing writes
  (`src/duplicacy_storage.go:109-126`). This is the same item recorded as a
  candidate in `init_perf.md`; it is one round trip per run.
- **`CleanSnapshotCache` probes every chunk of every cached snapshot.** At the
  end of a successful prune, each cached snapshot file is read and each of its
  chunks is looked up first in the cache and then in the storage
  (`src/duplicacy_snapshotmanager.go:614-615`). On a 60-revision fixture with the
  cache enabled, `prune -r 1 -exclusive` issues 290 `newfstatat` calls under the
  cache's `chunks/` and 46 under the storage's. It scales with the size of the
  cache rather than with the repository, so it costs most on a cache that has
  not been cleaned before.
- **The chunk cache is read even when the storage needs no cache — now guarded.**
  `DownloadChunk` used to guard the cache with `snapshotCache != nil` alone
  (`src/duplicacy_chunkoperator.go:331`), where every other cache access nearby
  asks `IsCacheNeeded()` first. `cachedPath` is assigned only inside that block,
  so gating the read gates the write-back with it, and the change is option 1
  above rather than a refinement of option 2. On a fast local disk it wins —
  `list -files` 69 ms → 47 ms, `check` 48 ms → 40 ms, `prune -exhaustive -dry-run`
  53 ms → 40 ms on a 40-revision ext4 fixture — because there the cache read costs
  what the storage read costs and only the write is saved. **Implemented**: the
  read now asks `IsCacheNeeded()` too, so a storage that declares itself
  cache-free neither reads nor writes the chunk cache. What used to block this is
  gone — see below.

  The guard was rejected when this document was written because it dropped the
  reuse a slow mount relies on: a 300-revision repository whose revisions all
  share one metadata sequence (4 chunks) went `list -files` 605 ms → 2649 ms,
  `check` 620 ms → 1312 ms, `list -chunks` 580 ms → 1258 ms, with `strace -f -e
  trace=openat` showing HEAD opening 3 storage chunks and 900 cache files against
  the guard's 900 storage chunks and 0 cache files. That measurement assumed a
  slow mount spelled as a plain path had `IsCacheNeeded()` false, so the cache it
  was losing was one the storage had said it did not want. `docs/check_perf.md`
  has since changed `IsCacheNeeded()` to ask the filesystem rather than the URL:
  a network filesystem whose repository is elsewhere now reports true, so the
  mount in that fixture keeps its cache whether or not the guard is in place.
  What the guard turns off is the configuration the flag was always meant to
  describe — a local storage, where a cache read is the same syscall as the read
  it replaces and the write is pure cost.

  `prune` itself is flat either way, because the sequence grouping above fetches
  a shared sequence once. Guarded by
  `TestChunkCacheIsGuardedByIsCacheNeeded` (`src/duplicacy_chunkoperator_test.go`),
  which fails with "The metadata chunk was written back to the cache of a storage
  that needs none" if the read guard is removed, and which also checks the
  cache-enabled half still reads and writes.
- **`prune` with a tag or a retention policy downloads every revision anyway.**
  The tag filter (`:2484`) and the retention policy (`:2433`) are applied after
  the snapshot files have been downloaded, which is unavoidable for the
  retention policy (it needs the timestamps) but not for `-t`, which only needs
  `snapshot.Tag`. This is a marginal item: the filter is applied per id, so the
  number of ids bounds the saving, and a tag filter is not a common invocation.
- **`CreateChunkOperator` keeps the first operator it is given.** The operator is
  created once and remembered (`:307`), and its thread count is fixed at creation,
  so a later caller that asks for more threads is ignored. That is what makes
  `DownloadSequence`'s own `1` harmless, but it also means the concurrency of the
  expansion is whatever the first caller on the manager asked for; prune happens
  to ask first, with `-threads` (`:2123`). Making the thread count a property of
  the request rather than of the operator would be more honest, and it is what
  would let `check` overlap its expansion too.

### Deliberately not pursued

- **A chunk index or a per-revision index.** As in `snapshot_perf.md`
  (candidate #7) and `copy_perf.md`, this would change the storage format on
  disk. Prune's chunk walk is what an index would replace, but it would have to
  be maintained by every writer and understood by old clients.
- **Parallelising the tree walk.** `ListAllFiles` is a serial BFS
  (`src/duplicacy_snapshotmanager.go:726-761`). It is proportional to the number
  of chunk directories, and the measurements above put it well behind the
  sequence expansion; parallelising it would change the order chunks are
  fossilized in, which is user-visible in the log.
- **Skipping the sequence expansion in non-exhaustive mode when `-exclusive` is
  given.** The snapshots being deleted still have to be expanded to know which
  chunks to fossilize, so there is nothing to skip.
- **The `fsync` in `FileStorage.UploadFile`.** It is a deliberate durability
  guarantee and applies to the real chunk uploads as well as the cache; the
  cache-specific question is covered above.
- **`-exclusive` versus fossil collection.** `-exclusive` deletes chunks instead
  of renaming them to `.fsl`, which skips the fossil-collection machinery, but
  it also requires `IsMoveFileImplemented()` at
  `duplicacy/duplicacy_main.go:1216` and is not a speed switch: the measurements
  above use it only to make the deletions deterministic between runs.

## How to confirm on a given setup

- `duplicacy -d prune ...` sets DEBUG logging
  (`duplicacy/duplicacy_main.go:146-148`); `-v` sets TRACE. The `DOWNLOAD_FILE`
  lines ("Downloaded file snapshots/<id>/<n>") show the revision loop — their
  count is the revision count, and with `-threads 1` they are printed one after
  another with no overlap, while under more threads they interleave. Compare
  with `duplicacy -d list` on the same repository.
- The `CHUNK_DOWNLOAD` ("Chunk ... has been downloaded") and `CHUNK_CACHE`
  ("loaded from the snapshot cache") counts show the metadata fetches. The
  `CHUNK_CACHE` line is what the unguarded write-back used to leave behind even
  on a local path: `DownloadChunk` read the chunk cache whenever `snapshotCache`
  was set, without checking `IsCacheNeeded()`, so a local storage reported cache
  hits although the cache exists only for the storages that ask for one. A local
  run now reports none; a storage that needs a cache still reports both the save
  and the load.
- `duplicacy -d prune ...` prints `SNAPSHOT_DELETE` per revision being removed
  and, for the fossils, `FOSSIL_COLLECT`/`FOSSIL_DELETABLE`. The gap between the
  last `DOWNLOAD_FILE` and the first of those is the chunk-selection phase.
- `strace -f -c -e trace=fsync,renameat,unlinkat,mkdirat,newfstatat,openat
  duplicacy prune ...` and look at `fsync` + `renameat`: one pair per metadata
  chunk is the cache write-back. Option 1 removes both (zero of each); option 2
  keeps the `renameat` and removes the `fsync`; that pair is the signature to look
  for when confirming option 2.
- Compare the same repository with the storage on a native filesystem and on the
  mount in question. The syscall counts match and the timings differ, which
  separates filesystem cost from the command's own behaviour.
- `prune -exhaustive` against `prune` on the same repository sizes the two modes;
  `prune -r N` (a single revision) gives the marginal per-revision cost. Note
  that `-delete-only` returns at `:2398`, before the chunk-selection phase, so it
  measures the fossil-collection cleanup only rather than the work described
  above.
- Run `go test ./src/ -run 'TestPrune' -vet=off -v` for the deletion semantics,
  `go test ./src/ -run 'TestPruneDownloadsRevisionsConcurrently|TestDownloadSnapshotsConcurrently'
  -vet=off -v` for the parallel revision reads,
  `go test ./src/ -run 'TestPruneExpandsSequencesConcurrently|TestSharedSequenceIsExpandedOnce|TestDownloadSequencePreservesOrderConcurrently'
  -vet=off -v` for the sequence expansion, and
  `go test ./src/ -run 'TestCorruptCachedChunkIsRefetched|TestSnapshotCacheSkipsSync|TestCorruptNonChunkCacheEntries|TestChunkCacheIsGuardedByIsCacheNeeded'
  -vet=off -v` for the cache behaviour, and
  `go test ./src/ -run 'TestWaitForCompletionIsWokenNotPolled' -vet=off -v` for
  the completion wait.
- The completion wait is timed by holding a chunk download until the wait is
  running and releasing it: on a polled implementation the elapsed time is the
  whole 100 ms tick regardless of the release, on a signalled one it is the
  release. `strace -f -e trace=futex,nanosleep` shows the same thing without the
  test, as a `futex(FUTEX_WAIT, ~99 ms)` that vanishes with the fix.
- `-threads` is now visible in three places, so a thread sweep on one repository
  separates them: the `DOWNLOAD_FILE` lines show the revision loop, the
  `CHUNK_DOWNLOAD`/`CHUNK_CACHE` counts show the expansion, and the wall clock
  shows both. To check that the expansion itself overlaps, use a repository whose
  revisions do not share their chunk list (so there is one group per revision)
  and watch the chunk downloads interleave under `-d`.
- The snapshot-file deletions are timed with `strace -f -tt -e trace=unlinkat`,
  which shows both the number of threads issuing them and the span they cover:
  one thread over ~0.29 s for 150 revisions on HEAD, eight over ~0.13 s after the
  fix. `strace -f -tt -e trace=unlinkat,write` also shows the `has been removed`
  lines landing inside that window. `go test ./src/ -run
  'TestPruneDeletesSnapshotsConcurrently|TestPruneSnapshotDeletionOutputStaysInRevisionOrder|TestPruneStreamsSnapshotDeletionOutput'
  -vet=off -v` covers the same three properties without the mount.
- To check the change does not alter results, prune the same pristine copy of a
  storage with and without it and compare the trees with `diff -rq`; that is how
  the claims above were verified.
