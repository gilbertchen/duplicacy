# Where `restore` spends its time

Investigation into the performance of `duplicacy restore`, in the style of
`snapshot_perf.md`, `copy_perf.md`, `prune_perf.md`, `check_perf.md` and
`init_perf.md`. Five performance defects were found, and all five are fixed. The
three per-file syscall ones come first: the deferred cleanup of the temporary file
probed the filesystem twice for every file even when there was no temporary file;
the target file's existence was established twice, once by `Restore` before the
per-file loop and again by the `Open` in `RestoreFile`; and the parent directory of
every restored file was re-created although the directory pass had just created
it. The fourth is the metadata sequences being expanded on a one-thread operator,
so that `-threads` did nothing for them. The fifth is the in-place `ftruncate`,
which the write loop has already made unnecessary for a file this run created. A
sixth defect, a latent nil dereference rather than a cost, is fixed on the same
pass and is recorded at the end of the in-place section.

## Summary

`restore` is five phases:

1. the local file list, walked on its own goroutine (`ListLocalFiles`);
2. the remote file list and the snapshot description, read on the calling
   goroutine (`DownloadSnapshot`, `DownloadSnapshotSequences`, `ListRemoteFiles`);
3. a merge of the two sorted lists into `fileEntries` (the files to restore) and
   `directoryEntries`, with `extraFiles` holding local files absent from the
   snapshot;
4. the download plan — a first-occurrence map over the snapshot's chunk list,
   a sort of `fileEntries` by starting chunk, `AddFiles` to build the download
   task list, and `CreateFileChunkMaker`;
5. the per-file loop, calling `RestoreFile` for each file.

Phases 1 and 2 already overlap with each other and with phase 3. Phase 5 is
serial per file, and each file costs it a fixed handful of syscalls. Three of
those are avoidable and are the per-file findings below, and a fourth — the
in-place `ftruncate` — is avoidable for every file a fresh restore creates. The
rest of phase 5 is either the chunk download itself (which already overlaps across
files, see "Deliberately not pursued") or the metadata writes (`chmod`, `utimes`,
`chown`) that a restore is supposed to perform.

Candidate #4 is much smaller and is fixed on the same grounds rather than for its
timing: `-threads` reaches the file chunks but did nothing for the metadata
sequences, and it now does.

Measured on a 20,005-file tree, `/usr/bin/time`, best of several runs, the three
per-file syscalls together take a fresh restore from 1.53 s to 1.30 s on ext4 and
from 163 s to 108 s when the target is on a virtiofs mount, and take a re-restore
of an unchanged tree from 0.28 s to 0.21 s. Those figures were taken with all
three applied, before any was committed. Measured separately afterwards against
the commit before each, the three are worth, in order, `unlinkat` 40,010 → 0;
`openat` 40,028 calls with 20,009 failures → 20,027 with 9, 1.12x on ext4 and
1.20x on virtiofs; and `newfstatat` 60,828 → 41,028, 1.09x on ext4 and 1.13x on
virtiofs.

A second pass over the in-place branch added candidate #5, the `ftruncate` that
the write loop has already made unnecessary for a file the restore created: 1.13x
on ext4 and 1.04x on the virtiofs target, far less than the three syscalls of the
first pass. The pair of `Seek` calls in the same block turned out not to be worth
changing at all, and is recorded as ruled out.

That completes the five: the three per-file syscalls, the metadata sequences and
the in-place truncate are all implemented. The latent nil dereference found in the
same pass is fixed as well. What is left below is recorded as looked at and not
worth changing rather than as an open candidate. The one item that came close is
the per-file `Lstat` in `RestoreMetadata`: it is genuinely dead on a fresh restore
and measures 1.12x on a slow target, but it is the `-o` comparison and the
already-matching check of a re-restore, so removing it is a design change rather
than a deletion; it is recorded as examined and retained.

## The call path

`restoreRepository` (`duplicacy/duplicacy_main.go:816`) parses the flags, creates
the storage with `-threads` (`:843`), and calls
`BackupManager.Restore` (`:890`), passing `true` for `inPlace` and setting
`quickMode` by the absence of `-hash` (`:853`).

`Restore` (`src/duplicacy_backupmanager.go:636`) then does, in order:

```go
chunkOperator := CreateChunkOperator(config, storage, cache, showStatistics, false, threads, allowFailures)  // :688
go localSnapshot.ListLocalFiles(top, nobackupFile, filtersFile, excludeByAttribute, localListingChannel, ...) // :694
remoteSnapshot := manager.SnapshotManager.DownloadSnapshot(snapshotID, revision)                             // :697
manager.SnapshotManager.DownloadSnapshotSequences(remoteSnapshot)                                            // :698
go remoteSnapshot.ListRemoteFiles(config, chunkOperator, func(entry) { remoteListingChannel <- entry; ... })  // :702
for remoteEntry := range remoteListingChannel { ... merge local and remote ... }                             // :712
chunkMap := map[string]int{}; for i, chunk := range remoteSnapshot.ChunkHashes { ... }                       // :812
for _, file := range fileEntries { ...collapse single-chunk files onto their first chunk... }               // :820
sort.Sort(ByChunk(fileEntries))                                                                             // :829
chunkDownloader := CreateChunkDownloader(chunkOperator); chunkDownloader.AddFiles(remoteSnapshot, fileEntries) // :831
chunkMaker := CreateFileChunkMaker(config, true)                                                            // :835
for _, file := range fileEntries {                                                                          // :840
    stat, _ := os.Stat(fullPath)                                                                            // :843
    ... manager.RestoreFile(chunkDownloader, chunkMaker, file, top, ...)                                    // :887
}                                                                                                           // :908
```

`RestoreFile` (`:1155`) is where phase 5 spends its syscalls:

```go
defer func() { ...; if temporaryPath != fullPath { os.Remove(temporaryPath) } }()                            // :1167
existingFile, err = os.Open(fullPath)                                                                       // :1192
if inPlace { ...hash the existing file in place... } else { chunkMaker.AddData(existingFile, chunkFunc) }    // :1239/:1348
for i := entry.StartChunk; i <= entry.EndChunk; i++ { if _, found := offsetMap[...]; !found { needed = true } } // :1359
chunkDownloader.Prefetch(entry)                                                                             // :1365
if inPlace { ...write in place, hashing as it goes, then Truncate(offset)... } else { ...write a temporary file, remove, rename... } // :1367
```

The two modes are worth separating:

- **in-place** (the only mode the CLI reaches: the call at
  `duplicacy/duplicacy_main.go:890` passes `true` for `inPlace`, and `:646`
  forces it when the preference path is not the default): the file is rewritten
  at its own path, the chunks already at the right offsets are reused, and no
  temporary file is created.
- **non-in-place**: the old file is split with the chunk maker so that a chunk
  found anywhere in it can be reused, a temporary file
  (`.duplicacy/temporary`) is written, then `os.Remove(fullPath)` (`:1544`) and
  `os.Rename(temporaryPath, fullPath)` (`:1550`) install it.

The chunk downloads themselves go through `ChunkDownloader`:
`AddFiles` (`src/duplicacy_chunkdownloader.go:67`) turns the sorted
`fileEntries` into one task per distinct chunk and rewrites each file's
`StartChunk`/`EndChunk` to indexes into that task list; `Prefetch` (`:102`)
submits up to `threads` tasks ahead of the current file; `WaitForChunk` (`:168`)
blocks until the current chunk arrives and keeps the prefetch window full. The
tasks are submitted through the operator's `DownloadAsync`
(`src/duplicacy_chunkoperator.go:184`) and each completes on a worker goroutine;
`WaitForCompletion` (`:218`) and `GetLastDownloadedChunk` (`:158`) have no caller.

## The temporary-file cleanup probed the filesystem for every file — candidate #1 — **Implemented**

`RestoreFile` used to register an unconditional cleanup:

```go
defer func() {
    if existingFile != nil { existingFile.Close() }
    if newFile != nil { newFile.Close() }

    if temporaryPath != fullPath {
        os.Remove(temporaryPath)
    }
}()
```

`os.Remove` cannot know whether the path is a file or a directory, so it issues
`unlink` and, when that fails, `rmdir`. When no temporary file exists — every
file of an in-place restore, the only mode the CLI reaches — both calls fail with
`ENOENT`: two syscalls per file, each one a `newfstatat` on the parent directory
plus the failing call on a network mount.

The non-in-place path was barely better. The temporary file is renamed onto the
target (`:1555`), so by the time the deferred function runs the temporary path no
longer exists either, and the cleanup was again two failing calls. It is needed
only when an error abandons the run between `:1467` (where the temporary file is
created) and `:1555`.

On the 20,005-file fixture a fresh restore issued 40,010 `unlinkat` calls, all
`ENOENT`, exactly two per file; the count was the same for a re-restore, for
`-hash`, and for a restore limited by a pattern.

`temporaryFileCreated` (`:1165`) is now set where the temporary file is created
(`:1472`) and cleared once it has been renamed onto the target (`:1560`), and the
deferred cleanup (`:1179`) only asks about a file it made. Nothing else moves:
the same `os.Remove` is still reached for every file the non-in-place path
actually creates one for, which is what an abandoned run relies on.

Measured with `strace -f -c -e trace=unlinkat,openat,newfstatat`, the `unlinkat`
count goes to 0 while `openat` and `newfstatat` are unchanged: 40,010 → 0 on the
20,005-file fixture, and 1,000 → 0 on a 500-file one, where `openat` stays at
1,027 and `newfstatat` at 1,568. On a virtiofs target, where each failed call is
a path resolution, the fix is part of the 158 s → 100 s the first three
candidates give together. The restored tree (`diff -r`) and the log are identical
across `restore -r 1`, `-hash`,
`-hash -overwrite`, `-delete`, a pattern-restricted restore, `-stats` and
`-threads 4`, with the same exit codes, and `TestRestoreCleansUpAfterAFailure`
(`src/duplicacy_backupmanager_test.go`) guards the abandoned-run cleanup by
stopping a non-in-place restore with a missing chunk and checking that
`.duplicacy/temporary` is gone. `TestBackupManager` and `TestPersistRestore`
already restore both in place and not.

## The target file's existence was established twice — candidate #2 — **Implemented**

`Restore` already stats each target before the per-file loop (`:843`) and uses the
answer to choose between the quick skip, the size-0 skip, and creating the parent
directory. `RestoreFile` then opened it again to hash it in place or to split it,
and on a fresh restore that open was guaranteed to fail:

```
newfstatat(AT_FDCWD, ".../dir000/file000", 0x..., 0) = -1 ENOENT
openat(AT_FDCWD, ".../dir000/file000", O_RDONLY|O_CLOEXEC) = -1 ENOENT
openat(AT_FDCWD, ".../dir000/file000", O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC, 0600) = 7
```

20014 failing `openat` calls for 20005 files, all for a file the caller had
already been told is absent.

`Restore` now keeps the error from its `os.Stat` (`:843`) and passes
`os.IsNotExist(statErr)` to `RestoreFile` as `knownAbsent`, which guards the
`Open` (`:1219`). The flag is derived from `os.IsNotExist`, not from a bare
`stat == nil`, so that an `Open` that fails for a reason other than absence is
still reported the way it was. Only the probe is dropped, so the `DOWNLOAD_OPEN`
line for a file that exists but cannot be read is unchanged — verified by
restoring over a `chmod 000` target, which still logs exactly one `Can't open the
existing file: ... permission denied` and exits with the same code as before.

The absent-file branch, which creates the sparse file for a large in-place
target, was the one thing not unchanged. It used to be reached from the failed
`Open`'s `os.IsNotExist`, and that test cannot fire when the probe is skipped, so
a file the caller had already reported absent never got its sparse file: a fresh
in-place restore of a 145 MB file allocated 294,928 blocks instead of 2,064, and
the same hole was written for every absent file over 100 MB — `sparse_test.sh`'s
955 MB fixture went from 22 MB to 955 MB allocated. The branch now tests
`targetAbsent`, which is the caller's `knownAbsent` when the probe is skipped and
the probe's `os.IsNotExist` when it is not, so both paths reach it. The
rest of the absence handling is otherwise untouched, and a large target still gets
its sparse file on either path.

Measured with `strace -f -c -e trace=openat,newfstatat`, on the 20,005-file tree
`openat` goes from 40,028 calls with 20,009 failures to 20,027 with 9, while
`newfstatat` stays at 60,828 with 20,409 failures; on a 500-file tree it is 1,028
and 509 down to 528 and 9. On its own that is 1.26 s → 1.13 s on ext4 (1.12x) and
138.6 s → 115.8 s on a virtiofs target (1.20x) — a third of the first three
candidates' combined virtiofs win, from removing one round trip per file. The
restored tree, the file metadata and the log are identical to the previous commit
for `restore -r 1`, `-hash`, `-hash -overwrite`, `-delete`, a pattern-restricted
restore, `-stats`, `-threads 4` and `-ignore-owner`, with the same exit codes.

`TestInPlaceRestoreOfAnAbsentLargeFileStaysSparse`
(`src/duplicacy_backupmanager_sparse_test.go`) pins the sparse-file path: it backs
up a 105 MB file whose data is at the end of a hole, restores it in place into an
empty repository, and fails with "The restored file has 110100480 bytes allocated,
so the hole was written rather than left sparse" if the branch is reached only
from the skipped probe.

## The parent directory was re-created for every file — candidate #3 — **Implemented**

The same `stat != nil` test drives the parent lookup (`:865`):

```go
} else {
    parent, _ := SplitDir(fullPath)
    err = os.MkdirAll(parent, 0744)
    ...
}
```

`os.MkdirAll` stats the directory first and does nothing when it exists, so for the
100 files of one directory that the directory pass has already created, 99 of the
calls were a wasted `newfstatat`: 19,804 on the 20,005-file fixture, one per file
after the first in each directory.

`createdParents` (`:842`) now records the parents this pass has created and the
`MkdirAll` is only called for a parent that is not in it (`:867`), the pattern
`SnapshotManager.UploadFile` already uses for the per-id snapshot directory
(`src/duplicacy_snapshotmanager.go:3121`). The map is bounded by the number of
directories in the snapshot, and the first file of each directory still creates
it. The entry is only recorded on success, so a failed `MkdirAll` is retried for
the next file rather than silently skipped.

Measured with `strace -f -c -e trace=newfstatat,mkdirat`, `newfstatat` goes from
60,828 calls to 41,028 on the 20,005-file fixture, with `mkdirat` unchanged at
203. On its own that is 1.11 s → 1.02 s on ext4 (1.09x) and 116.3 s → 103.1 s on a
virtiofs target (1.13x). The restored tree and the log are identical to the
previous commit for `restore -r 1`, `-hash`, `-hash -overwrite`, `-delete`, a
pattern-restricted restore, `-stats`, `-threads 4` and `-ignore-owner`, including
on a tree with nested directories (`a/b/c`), with the same exit codes.

### Measured

The three candidates were prototyped in sequence (each on top of the previous)
and measured against HEAD. `/usr/bin/time`, best of four, cache warm. These are
the prototype figures from before any of the three was committed; the
per-candidate figures in each section were measured separately afterwards, one
commit at a time:

| Fixture | HEAD | #1 | #1+#2 | #1+#2+#3 |
| --- | --- | --- | --- | --- |
| fresh, 20,005 files, ext4, `-threads 1` | 1.53 s | 1.39 s | 1.34 s | 1.30 s |
| re-restore unchanged, ext4, quick | 0.28 s | — | 0.21 s | — |
| re-restore unchanged, ext4, `-hash` | 0.89 s | — | 0.75 s | — |

On a virtiofs target, where every syscall pays the mount's path resolution
whether or not it succeeds, the same fixture measured with a single run per
build: 163 s at HEAD, 131 s with #1, 119 s with #1+#2, and 108 s with all three.

The syscall counts separate the three cleanly. On the 20,005-file fixture:

| Probe | HEAD | #1+#2 | #1+#2+#3 |
| --- | --- | --- | --- |
| `unlinkat` (all `ENOENT`) | 40,010 | 0 | 0 |
| `openat` | 40,055 (20,014 errors) | 20,051 (9 errors) | 20,050 (9 errors) |
| `newfstatat` | 60,862 (20,415 errors) | 60,862 (20,415 errors) | 41,058 (20,415 errors) |
| total | 141,131 | 81,117 | 61,312 |

Each fix is output-identical: the restored tree (`diff -r`) and the log (modulo
the target path and the total running time) are unchanged for `restore -r 1`,
`-hash`, `-hash -overwrite`, `-delete`, and a pattern-restricted restore, with the
same exit codes. The three only remove syscalls that failed or that established
something already known — no file is opened, created, removed or renamed
differently — and the one cleanup they touch, the temporary-file removal, is
still reached for every file the non-in-place path creates one for.

## The metadata sequences were expanded on a one-thread operator — candidate #4 — **Implemented**

`Restore` calls `DownloadSnapshotSequences` to expand the sequences, and each goes
through `SnapshotManager.DownloadSequence`
(`src/duplicacy_snapshotmanager.go:345`), which creates the snapshot manager's
operator with a hard-coded one thread:

```go
func (manager *SnapshotManager) DownloadSequence(sequence []string) (content []byte) {
    manager.CreateChunkOperator(false, false, 1, false)   // :346
    ...
    manager.chunkOperator.DownloadAsync(chunkHash, i, true, func(chunk *Chunk, chunkIndex int) { ... })
    waitGroup.Wait()
}
```

`DownloadSequence` submits every chunk of the sequence at once and waits, so the
chunks do overlap — but only as wide as the operator, which was one. The user's
`-threads` reached `ListRemoteFiles` and the file chunks through the other operator
(`:688`), not the sequences. This is the same shape `check` and `prune` have: they
create the manager's operator with `-threads` before expanding (`check_perf.md`,
"The revision loop used to be serial"), and `DownloadSequence` then reuses it
instead of creating its own.

`Restore` now does the same (`:701`), with a deferred `stopChunkOperator` so that
the operator is shut down when the restore returns. The first argument is named
`resurrect` in the manager method but becomes the operator's `showStatistics`; it
stays false so that the per-chunk `DOWNLOAD_PROGRESS` lines `-stats` prints are
not emitted for the metadata chunks.

The value is small, and the prototype that first measured it is still the right
picture: the sequences are a handful of metadata chunks even for a large file
list, a 250,000-file repository expanding all three in 23, and the index-only
phase on a virtiofs storage went from 2.16 s to 2.09 s with no clear gain on ext4.
Two things confirm the mechanism rather than the timing. `-stats` output is
unchanged, which is the `resurrect` argument staying false. And
`TestDownloadSequencesOverlapUnderThreads`
(`src/duplicacy_snapshotmanager_test.go`) shows what the operator's thread count
decides: the same ten-chunk sequence is fetched strictly serially through a
one-thread operator and by several workers at once through an eight-thread one.
An end-to-end assertion that `restore` passed the user's `-threads` is not
practical, because the chunk sequence of any fixture small enough to build in a
test is a single chunk, so there is nothing to overlap; the flag is checked by
reading the log of a real restore instead.

## The in-place `ftruncate` is a no-op for a file this run created — candidate #5 — **Implemented**

The in-place branch issued one `ftruncate` per file, after the write loop had
already left the file the right length (`:1447`):

```go
// Must truncate the file if the new size is smaller
if err = existingFile.Truncate(offset); err != nil {
    LOG_ERROR("DOWNLOAD_TRUNCATE", "Failed to truncate the file at %d: %v", offset, err)
    return false, nil
}
```

It is needed for a target that already existed and is being shrunk, and it is
needed for the in-place read path, which asks for one byte past `entry.Size` so
that a grown file is not reported unchanged (`chunkSize = 1` at `:1258`). It is
not needed for a file this call created, which is every file of a fresh restore:
the first branch of the in-place block opens the target with
`os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)`, the write loop
then writes exactly the bytes the snapshot holds — `Write` and the `io.CopyN` of
a reused chunk each advance the descriptor by the chunk's slice — so the file is
already `offset` bytes long and the truncate only asserts the size it has. A
boolean set where the create happens and consulted at the truncate removes it for
those files and leaves it in place for the two cases that need it. Verified by
appending 150 MB of zeroes to a 150 MB target and restoring: 150,000,000 bytes
with the truncate and 300,000,000 without it. The one byte past the end that the
read path asks for is only a read, so it does not make the truncate necessary on
the created path.

Measured on the 20,005-file fixture, `/usr/bin/time`, best of five: ext4 1.13 s
→ 1.02 s and virtiofs 158 s → 152 s (1.04x), and it takes the `ftruncate` count
from 20,000 to zero. On this fixture the chunk sizes are the same as the file
sizes, so the loop and the truncate disagree about nothing; on a tree whose files
each span several chunks the saving is the same one call per file, which is small
in the context of the command.

**Implemented**: `fileCreated` (`:1184`) is set at both places the in-place branch
creates the target — the sparse-file path at `:1229` and the empty-file path at
`:1405` — and the truncate runs only when it is false (`:1478`). On a 20-file,
300 KB-per-file fresh restore, `strace -c -e trace=ftruncate` counts 20 calls
before and none after, and the restored tree is byte-identical (`cmp` on every
file). The shrink case is the one the guard must not break: a target grown to
150 MB and restored over goes back to its 300,000 bytes, which
`TestInPlaceRestoreStillTruncatesALongerTarget`
(`src/duplicacy_backupmanager_test.go`) pins down — it fails with "The restored
file is 483511 bytes and does not match the 279410 bytes of the snapshot's copy"
if the truncate is skipped for a file that already existed.

Two related no-ops were looked at and are not worth changing. `existingFile.Seek(0,
0)` at `:1387` and the per-chunk `Seek(offset, 0)` at `:1412` do issue 20,000 and
40,000 `lseek` calls on the fixture, but `lseek` only sets a field on the open
file description rather than making a request to the storage, so removing the
pair is flat against HEAD on both filesystems (ext4 1.13 s → 1.12 s, virtiofs
158 s → 158 s) while dropping the single `ftruncate` is the 1.04x above. The
`Seek(0, 0)` is in any case not removable on its own: the read path has left the
descriptor one byte past `entry.Size`, so without it the first `Write` would land
there. Only the seek at `:1412` is redundant — the loop's own `Write` and
`io.CopyN` each leave the descriptor where the next seek would put it — and the
pair is recorded here as ruled out rather than as a candidate.

A third, unrelated item in the same area is a latent nil dereference. `downloadFile`
(`src/duplicacy_snapshotmanager.go:3083`) reads `manager.chunkOperator.rewriteChunks`
after decrypting a file, but `restore` never calls `CreateChunkOperator` before
`DownloadSnapshot`, so the field is nil when a *file* decrypt reports a stale hash
version or a repaired erasure-coding shard — the operator is only created later,
for the file chunks (`:688`). `list` and `prune` create it first, so the same code
is guarded there. `Decrypt` only ever set `rewriteNeeded` for such a file under
`-erasure-coding`, which is why this has not shown up; a `manager.chunkOperator !=
nil` guard would make it unreachable.

**Implemented**: the read now requires the operator to exist (`:3086`). Both
commands that reach it with a nil operator — `backup` reads the snapshot file at
`src/duplicacy_backupmanager.go:160` and `restore` at `:697`, ahead of the
`CreateChunkOperator` at `:232` and `:701` — now skip the rewrite, which is what
`list` already does through an operator created with `rewriteChunks` false. The
reconstruction itself is unaffected: `Decrypt` repairs the chunk in memory
regardless, so only the re-upload is skipped. `TestDownloadFileWithoutChunkOperatorDoesNotPanic`
(`src/duplicacy_snapshotmanager_test.go`) stores an erasure-coded snapshot file,
corrupts one data shard so that `Decrypt` has to reconstruct it, and downloads it
with no operator (must not panic, must return the original bytes) and then with
one that asks for rewrites (must re-upload once); it fails with "invalid memory
address or nil pointer dereference" when the guard is removed.

## Candidate fixes

### Smaller items

- **The first-occurrence map is built per restore.** `chunkMap` (`:812`) scans
  the whole chunk list to find each chunk's first occurrence, which is what lets
  a small file use the first copy of a chunk it shares with another file. It is a
  local map over one revision's chunk list and costs no syscalls, so it is not
  worth changing.
- **`AddFiles` deduplicates by adjacency.** `AddFiles`
  (`src/duplicacy_chunkdownloader.go:77`) emits a task only when the chunk index
  differs from the previous file's last index; the `needed` flag on the previous
  task is set instead. `fileEntries` is sorted by chunk, so the adjacency test is
  what makes a chunk shared by consecutive files download once.
- **`-hash` re-reads every existing file.** `quickMode` is false with `-hash`, so
  every file is opened and hashed (`:1192`, `:1239`), which is what the flag
  promises. Candidate #2 removes the *failed* open of a file that does not exist;
  it does not, and should not, remove the read of one that does.
- **`ChunkDownloader.WaitForCompletion` and `GetLastDownloadedChunk` were
  unreachable.** `WaitForCompletion` (`src/duplicacy_chunkdownloader.go:218`) and
  `GetLastDownloadedChunk` (`:158`) had no caller anywhere in the tree; only the
  operator's own `WaitForCompletion` is used. Nothing in `restore` referred to
  them, so they were not a cost, but they were the kind of dead code that misleads
  a reader looking for where the download tail is awaited. Along with them
  `AddFiles` computed `maximumChunks` (`:71`, updated at `:94-96`) and never read
  it. **Removed**: the two methods and the variable are gone, and the download
  path is unchanged, since neither was reachable.
- **The local listing walks with one goroutine.** `ListLocalFiles`
  (`src/duplicacy_snapshot.go:66`) lists directories in a loop. It runs
  concurrently with the remote listing and the merge, so it is on the critical
  path only when it is the slowest of the three, which on a local repository it
  is not.
- **The two `Seek` calls in the in-place branch are not worth removing.** The one
  at `:1412` before every chunk is redundant, since the loop's own `Write` and
  `io.CopyN` leave the descriptor where the next seek would put it, and the one at
  `:1387` is not removable on its own because the read path ends one byte past
  `entry.Size`. Together they are 40,000 `lseek` calls on the fixture, but a
  `lseek` is a local operation, so removing them is flat on both filesystems
  (ext4 1.13 s → 1.12 s, virtiofs 158 s → 158 s). See candidate #5.

### Deliberately not pursued

- **Parallelising `RestoreFile` itself.** The obvious-looking target is the
  per-file loop, but the chunk downloads already overlap across files:
  `Prefetch` (`:1365`) submits the current file's chunks ahead of time and
  `WaitForChunk` keeps the window full, so on a 40-file, 400 MB repository of
  10 MB files a fresh restore goes from 1.74 s at one thread to 0.87 s at four.
  What is serial per file is the local syscalls and the writes,
  and for a tree of small files those are exactly the syscalls the three fixes
  above removed.
  Overlapping them would mean a worker per file with its own descriptors and its
  own slice of the shared chunk task list, and the task list's `Reclaim`
  (`:132`) assumes the files are visited in chunk order.
- **Reading the file sequence serially.** `ListRemoteFiles`
  (`src/duplicacy_snapshot.go:107`) fetches the sequence chunks through
  `operator.Download` (`:119`), which blocks on the calling goroutine, so the
  sequence is read one chunk at a time even at `-threads 8`. It is the same read
  candidate #4 addresses for the other two sequences, and it would need the
  sequence-to-list walk to become a prefetching one rather than a blocking one.
  On a 250,000-file repository (23 sequence chunks) it was below the noise.
- **A chunk index.** As in `snapshot_perf.md` (candidate #7), `copy_perf.md` and
  `prune_perf.md`, this would change the storage format on disk.
- **Removing the per-file `Lstat` in `RestoreMetadata`.** `RestoreMetadata`
  (`src/duplicacy_entry.go:526`) begins with an `os.Lstat` whenever its `fileInfo`
  argument is nil, and all five call sites pass nil (`:761`, `:774`, `:891`,
  `:921`, `:936`), so every restored file and directory costs one `newfstatat`:
  41,044 → 20,839 on the 20,005-file fixture, 20,205 calls. On a fresh restore the
  answer cannot change a decision — the file was created `0600` moments ago with
  `Now` as its mtime, so `Chmod` and `Chtimes` run whatever the stat reports — and
  a prototype that dropped it measured a fresh restore at 117.5 s → 105.3 s on the
  virtiofs target (1.12x) and 0.953 s → 0.924 s on ext4 (1.03x), with `fchmodat`
  and `utimensat` unchanged. It is nevertheless **Not going to be implemented**, on
  two counts. `setOwner` is true by default (`duplicacy/duplicacy_main.go:856`) and
  `SetOwner` (`src/duplicacy_utils_others.go:37`) dereferences the `FileInfo`
  immediately, so the naive removal crashes a default restore with "invalid memory
  address or nil pointer dereference"; and on a re-restore of an unchanged file
  `RestoreFile` writes nothing, which leaves this stat as the only thing that
  reports the metadata already matches. On an unchanged re-restore of 20 files it
  is 84 `newfstatat` with none of `fchmodat`/`utimensat`, against 64 `newfstatat`
  but 20 of each without it, so removing it literally adds two metadata writes per
  file. A fix has to keep the "is it already right?" answer while losing the probe,
  which is a design change rather than a deletion, for a saving that only shows on
  a slow target. Recorded here as examined and retained, like the `Seek` pair.

## How to confirm on a given setup

- `duplicacy -d restore ...` sets DEBUG logging
  (`duplicacy/duplicacy_main.go:142-148`); `-v` sets TRACE. `RESTORE_PARAMETERS`
  prints the effective `in-place`, `quick` and `delete` flags, `RESTORE_INDEXING`
  "Indexing `<top>`" starts the local listing, and `RESTORE_START` "Restoring
  `<top>` to revision N" is where phase 5 begins — the gap between the two lines
  is phases 2-4.
- Sum the syscalls with the recipe in `docs/README.md`. On a fresh restore of a
  tree of N files, candidate #1's signature was `unlinkat` at exactly 2N calls and
  all failures and is now none; candidate #2 showed as `openat` at 2N calls with N
  failures and now shows as N+1 calls with a handful; candidate #3 showed as
  `newfstatat` at roughly one per file beyond the first in each directory and is
  now about one per directory. A restore of an already-populated tree cuts the
  `openat` count to the files that are actually opened. Candidate #5 is the
  `ftruncate` count: one per file on a fresh restore, zero once the create path
  skips it, and exactly one for a re-restore that shrinks a single file.
  Candidate #4 has no syscall signature: the operator's thread count decides
  whether the metadata downloads overlap, so compare `restore -r 1 -threads 1`
  with `-threads 8` on a storage where they are round trips.
  The retained `Lstat` in `RestoreMetadata` is what is left of the `newfstatat`
  count: one successful call per restored file and directory, with no failures, so
  on the 20,005-file fixture the count is 41,044 and 20,205 of those are this
  probe. A re-restore of an unchanged tree is the case it pays for: it shows no
  `fchmodat` or `utimensat` at all, because the stat says the metadata already
  matches.
- Time a fresh restore against a re-restore of the same tree: on the 20,005-file
  fixture a fresh restore is 1.5 s and an unchanged re-restore 0.3 s, and the
  gap is the file writes and the `openat` per file that the fresh restore no
  longer takes.
- `restore -r 1 -threads 1` against `-threads 8` isolates what is left: the
  per-file local work does not overlap at all, so the flag moves only the chunk
  downloads. On the small-file fixture, where there is nothing to download, the
  two thread counts are within 10% of each other; on the 10 MB-file fixture the
  flag is worth about 2x.
- `go test ./src/ -vet=off` is the safety net for any change to this path.
  `TestBackupManager` (`src/duplicacy_backupmanager_test.go:178`) exercises
  restores at 1 thread, quick and not, delete, and pattern-restricted, and
  compares the restored files by hash; `TestPersistRestore` (`:407`) covers the
  corrupt-chunk paths and the failure modes `allowFailures` controls. `AGENTS.md`
  records both as failing on an unmodified checkout; on this tree they pass
  (`go test ./src/ -vet=off -run 'TestBackupManager|TestPersistRestore'`), so if
  one starts failing after a change, the change is the likely cause.
  `TestInPlaceRestoreStillTruncatesALongerTarget` (`src/duplicacy_backupmanager_test.go`)
  pins the case candidate #5 must not break: a target longer than the snapshot's
  copy is shrunk back to the snapshot's size by the truncate, so the test fails
  with "The restored file is ... bytes and does not match ..." if the guard skips
  it for a file that already existed.
- `TestDownloadFileWithoutChunkOperatorDoesNotPanic`
  (`src/duplicacy_snapshotmanager_test.go`) guards the nil dereference: it stores
  an erasure-coded snapshot file, corrupts one data shard, and downloads it with no
  operator and then with one that asks for rewrites. It fails with "invalid memory
  address or nil pointer dereference" if the `manager.chunkOperator != nil` guard is
  removed.
- `TestInPlaceRestoreOfAnAbsentLargeFileStaysSparse`
  (`src/duplicacy_backupmanager_sparse_test.go`) guards candidate #2's sparse-file
  path: a 105 MB file whose data is at the end of a hole is restored in place into
  an empty repository, and the test reads `syscall.Stat_t.Blocks` to fail with "The
  restored file has ... bytes allocated, so the hole was written rather than left
  sparse" if the absence no longer reaches the branch that creates the sparse file.
  It is in its own `!windows` file because `Stat_t` is not portable.
`integration_tests/sparse_test.sh` covers the same path end to end on a 955 MB
fixture, where the difference is 22 MB against 955 MB allocated; `ls -lsh` prints
it directly.
