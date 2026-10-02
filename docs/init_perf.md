# Why `init` is not worth optimising

Investigation into the performance of `duplicacy init`. Unlike the `list`
investigation in `snapshot_perf.md`, this one found no meaningful local work to
remove: a fresh `init` does about a dozen file operations and is dominated by
fixed process startup. Against cloud storage there are a few redundant round
trips, but they are one-time costs, so no source changes are included.

## Summary

`init` is a create-if-absent operation: it creates `.duplicacy`, checks whether
the storage already has a `config`, writes one if it does not, creates the
`chunks` and `snapshots` directories, and writes `preferences`. Everything is
strictly sequential and happens once, so there is nothing to overlap and no
per-item cost that grows with the size of the repository.

The measured cost of a fresh `init` on ext4 is about 4 ms more than the cost of
`--version`, i.e. of the Go runtime and CLI startup. Init against a storage that
is already configured costs the same as `--version` within noise, because it
only reads `config` and writes `preferences`. On a slow local mount the gap
widens to about 20 ms, but it is still a fixed handful of syscalls, not a loop.

Three operations are nonetheless redundant and are recorded below as
candidates: the `config` existence check runs twice on a fresh init, a `nesting`
file that nothing ever writes is probed whenever the config already exists, and
the `chunks`/`snapshots` directories are created eagerly even though the write
paths create their own parents since the snapshot-directory fix.

## Conclusion

**There is little to gain here.** `init` is a one-off, fixed-size operation: it
does about a dozen file operations and never iterates over revisions, chunks or
files, so its cost does not grow with the repository and cannot be amortised
across a run the way the `list` costs could. Of the roughly 20 ms a fresh init
takes on a native filesystem, only about 4 ms is init's own work; the rest is
the constant cost of starting the process. Removing every item in the candidate
list would save a few milliseconds locally and a handful of round trips on
cloud storage, once, at setup time — which is not a worthwhile trade against
touching the storage-creation path that every other command depends on.

The `list` fixes in `snapshot_perf.md` were worth making because they multiplied
by the number of revisions and by the number of commands a user runs. Nothing in
`init` has that shape: it costs the same against a storage with thousands of
revisions as against an empty one, and a user runs it once per repository. The
candidates below are recorded so the findings are not lost, and because two of
them (the duplicate `config` probe and the `nesting` probe) are also paid by
other commands and would be reasonable to clean up as correctness-neutral
simplifications rather than as performance work.

## The call path

`initRepository` (`duplicacy/duplicacy_main.go:235-237`) is
`configRepository(context, true)` (`:243-513`):

```go
repository, err = os.Getwd()                              // :292
err = os.Mkdir(preferencePath, 0744)                      // :308   .duplicacy
storage := duplicacy.CreateStorage(preference, true, 1)   // :348
existingConfig, _, err := duplicacy.DownloadConfig(...)   // :360   probe + read config
if existingConfig == nil {
    duplicacy.ConfigStorage(...)                          // :499   probe again + write config
}
duplicacy.Preferences = append(duplicacy.Preferences, preference)  // :503
duplicacy.SavePreferences()                               // :505   preferences
```

`DownloadConfig` (`src/duplicacy_config.go:403-480`) does an existence check
(`:408`) followed by a download (`:417`) when the file exists, and finishes with
`storage.SetNestingLevels(config)` (`:476`). `ConfigStorage` (`:563-591`)
repeats the same existence check (`:566`) and calls `UploadConfig` (`:482-558`),
which uploads `config` (`:540`) and then loops over `"chunks"` and `"snapshots"`
calling `CreateDirectory` (`:550-555`).

## Operations per invocation

Traced with `strace -f -e trace=newfstatat,openat,mkdirat,renameat`, a fresh
`init` on ext4 emits exactly this, in this order:

```
newfstatat  .duplicacy/preferences            -> ENOENT
mkdirat     .duplicacy
newfstatat  <storage>                         -> ENOENT   CreateFileStorage:33
newfstatat  <storage>                         -> ENOENT   MkdirAll:36, stats it again
mkdirat     <storage>
newfstatat  <storage>/config                  -> ENOENT   DownloadConfig:408
newfstatat  <storage>/config                  -> ENOENT   ConfigStorage:566  (duplicate)
openat      <storage>/config.<rand>.tmp       O_WRONLY|O_CREAT
newfstatat  <storage>/config                  AT_SYMLINK_NOFOLLOW   UploadFile:158
renameat    config.<rand>.tmp -> config
mkdirat     <storage>/chunks
mkdirat     <storage>/snapshots
openat      .duplicacy/preferences            O_WRONLY|O_CREAT
```

That is ten operations against the storage and three local ones. Nothing here
iterates.

`init` against a repository whose storage is already configured is shorter
still — the storage directory exists, so `CreateFileStorage` only stats it, and
the config is read rather than written:

```
newfstatat  <storage>                         -> exists
newfstatat  <storage>/config                  -> exists
openat      <storage>/config                  O_RDONLY
newfstatat  <storage>/nesting                 -> ENOENT   SetNestingLevels:109
mkdirat     .duplicacy
openat      .duplicacy/preferences            O_WRONLY|O_CREAT
```

## Local measurements

Binary built from HEAD. To remove the shell's own overhead from the comparison,
one repository directory was created per invocation and reused, and the same
loop shape (`cd <repo> && duplicacy ...`) was used for every row. 150
invocations per row, on ext4:

| Case | Per invocation | Delta vs baseline |
| --- | --- | --- |
| `--version` (startup baseline) | 16.7 ms | — |
| fresh `init`, storage on ext4 | 20.4 ms | +3.7 ms |
| re-`init`, config already exists | 16.9 ms | +0.2 ms |

With the storage on the slow drvfs mount (repository still on ext4), the extra
syscall latency becomes visible even for the handful of operations above:

| Case | Per invocation | Delta vs baseline |
| --- | --- | --- |
| `--version` (startup baseline) | 17.1 ms | — |
| fresh `init`, storage on drvfs | 35.5 ms | +18.4 ms |

With both the repository and the storage on drvfs, a fresh init is about 40 ms
per invocation against an 18 ms baseline. So the storage-side work is roughly
3 ms on ext4 and roughly 18 ms on drvfs. The process is not computing: the same
operations are a few microseconds each on a native filesystem, and `init` costs
only a few milliseconds more than starting the binary at all.

Single-invocation `/usr/bin/time` agrees: `init` is `0.01-0.02 s` on ext4
against `0.01 s` for `--version`.

Unlike `list`, there is no scaling dimension here. `init` does a fixed amount of
work regardless of how many revisions or chunks the storage holds; re-running
it against a storage with hundreds of revisions costs the same as against an
empty one, because it never reads them. This is the main reason there is little
to gain: the `list` fixes were worth having because the cost was per revision
and grew with the repository, and none of that applies to `init`.

## Candidate fixes

Recorded for completeness; none is implemented.

- **The `config` existence check runs twice on a fresh init.**
  `DownloadConfig` checks `config` (`src/duplicacy_config.go:408`) and
  `ConfigStorage` checks the same path again (`:566`) immediately before
  uploading it. The caller already knows the file was absent, so the second
  check is one wasted `Stat`/`HeadObject` round trip, and the trace above shows
  the two adjacent `newfstatat` calls. Removing it means passing that knowledge
  into `ConfigStorage` instead of re-deriving it.
- **The `nesting` probe is a guaranteed miss.**
  `DownloadConfig` ends with `storage.SetNestingLevels(config)`
  (`src/duplicacy_config.go:476`), which for the current `fixed-nesting` format
  asks for a file named `nesting` (`src/duplicacy_storage.go:109`). Nothing in
  the repository ever writes one: `git grep nesting` finds only that reader and
  its comments, and `git log -S'"nesting"'` shows only the commit that
  introduced it (`86767b3`) and the one that corrected the download to use the
  `nesting` path (`dfbc5ec`). Every storage creation therefore pays a round trip
  on a file that cannot be there. This is not `init`-specific — every command
  pays it — but `init` pays it whenever the config already exists. It should at
  least be confirmed whether the override is still needed for storages written
  by old releases before the probe is removed.
- **`chunks` and `snapshots` are created eagerly at init time.**
  `UploadConfig` loops over both names calling `CreateDirectory`
  (`src/duplicacy_config.go:550-555`). The snapshot-directory fix (`8420c5d`)
  established the opposite policy — create the containing directory on the write
  path, which is where it is needed — and the write paths do exactly that:
  `FileStorage.UploadFile` calls `MkdirAll` (`src/duplicacy_filestorage.go:163`),
  `SFTPStorage.UploadFile` mkdirs intermediate directories
  (`src/duplicacy_sftpstorage.go:296-304`), and `SnapshotManager.UploadFile`
  creates the containing directory (`src/duplicacy_snapshotmanager.go:2777-2784`),
  which is also what the existing `8420c5d` rationale relies on for snapshot
  uploads. On directory-based backends the saving is several round trips rather
  than one, because `SFTPStorage.CreateDirectory` does a `Stat` *and* a `Mkdir`
  (`src/duplicacy_sftpstorage.go:234-245`) and each GCD `CreateDirectory` goes
  through `GetFileInfo`/`listByName` (`src/duplicacy_gcdstorage.go:684-741`). On
  SFTP these two calls alone cost four round trips (a `Stat` and a `Mkdir`
  each), which is the largest single item in a fresh init once the connection is
  up. This is the one candidate that needs validation across backends, because it
  relies on uploads creating missing parents; on the object stores it is a no-op
  either way (`S3Storage`, `B2Storage`, `AzureStorage`, `GCSStorage`,
  `SwiftStorage` and `StorjStorage` all have a no-op `CreateDirectory`), while
  Dropbox-like backends, which cannot upload into a missing directory, are
  exactly the ones the `8420c5d` change was written for and should be
  re-checked.

### Smaller items

- `CreateFileStorage` calls `os.Stat` (`src/duplicacy_filestorage.go:33`) and
  then `os.MkdirAll` (`:36`), which stats the same path again before creating
  it; the trace shows the duplicate `newfstatat` on the storage directory. Using
  `os.Mkdir` after the failed stat removes one syscall, but this is a startup
  path shared by every command and the saving is a single stat.
- `init -e` prompts for the password and then asks for it again to confirm
  (`duplicacy/duplicacy_main.go:350-358` and `:422-429`). Both calls pass
  `resetPassword = true`, so each one clears the keyring entry
  (`src/duplicacy_utils.go:218-220`), which on Linux is a D-Bus round trip
  (`src/duplicacy_keyring.go:25-30`). The confirmation prompt does not need to
  clear it.
- `init` never calls `SavePassword`, unlike every other command
  (`duplicacy/duplicacy_main.go:793`, `:885`, `:939`, ...). After `init -e` the
  next command asks for the same password again (verified), so the keyring
  lookup that `GetPassword` performs is wasted on every subsequent run. Adding
  the call would fix the repeated prompt; that is a behaviour change, not a
  speedup of `init` itself.

### Deliberately not pursued

- **Parallelising anything in `init`.** It is a sequence of create-if-absent
  operations with no independent work, and the whole thing is a handful of
  round trips.
- **The `fsync` in `FileStorage.UploadFile`.** One file per `init`
  (`src/duplicacy_filestorage.go:194`). The snapshot-cache investigation removed
  a per-revision `fsync`, but here it is the single write of the storage config,
  where the durability guarantee is worth more than the syscall.
- **Key derivation cost.** `CONFIG_DEFAULT_ITERATIONS` is 16384
  (`src/duplicacy_config.go:58`). Measured over 100 fresh encrypted inits, the
  default is ~23 ms per invocation against ~22 ms at `-iterations 1000` and
  ~32 ms at `-iterations 100000`, so the default is already cheap and the
  parameter is a security setting, not a tuning knob.
- **The `config` download on re-init.** It is needed for the `config.Print()`
  output (`duplicacy/duplicacy_main.go:375-378`), so it is not redundant work.
- **Replacing the eager directory creation with a lazy scheme only for chunks.**
  Chunk subdirectories are already created by the upload paths on the backends
  that need it, so there is no additional win beyond removing the two top-level
  directories.

## How to confirm on a given setup

- `strace -f -e trace=newfstatat,openat,mkdirat,renameat duplicacy init <id>
  <url>` and read the operations on the storage path. A fresh init against local
  storage should match the trace above; anything more is a redundant probe, and
  a different shape means a backend that resolves paths differently.
- `duplicacy -d init ...` sets DEBUG logging (`duplicacy/duplicacy_main.go:146-148`)
  and is the only level that prints the `STORAGE_NESTING` line ("Chunk read
  levels: [1], write level: 1") from `SetNestingLevels`, which is the visible
  sign that the config was parsed and the nesting override was consulted. It
  appears only when the config already exists, because `SetNestingLevels` is
  called from `DownloadConfig`. `-v` sets TRACE
  (`duplicacy/duplicacy_main.go:142-144`), one level quieter, so it shows the
  `CONFIG_INFO` trace lines (the hash and ID keys) but not `STORAGE_NESTING`;
  those `CONFIG_INFO` trace lines appear for a fresh init as well, since
  `UploadConfig` also calls `config.Print()` when tracing is on
  (`src/duplicacy_config.go:546-548`).
- `/usr/bin/time -v duplicacy init ...`: a low CPU percentage with a high
  elapsed time means round trips or syscalls, not processing.
- Time `duplicacy --version` from the same directory as a baseline, and subtract
  it. Most of an `init` on a local filesystem is the constant startup cost, and
  without the baseline it is easy to attribute that to `init` itself.
- Run the same init with the storage on a native filesystem and on the mount in
  question. The syscall counts will match while the timings differ, which
  separates backend cost from local filesystem cost.
- Run the same init against a fresh storage and against one that is already
  configured. The second form shows the `nesting` probe and no writes, and is
  the cheapest case `init` has.
