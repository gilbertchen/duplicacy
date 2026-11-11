# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project Overview

Duplicacy is a cloud backup tool based on lock-free deduplication. It is a Go
program that splits files into variable-size chunks (buzhash), stores each chunk
independently under its hash, and supports many cloud storage backends.

The Go module path is `github.com/gilbertchen/duplicacy` and must stay that way
even when working in a fork, because `duplicacy/duplicacy_main.go` imports
`github.com/gilbertchen/duplicacy/src`. Do not rename the module path or change
the `src` import (see the wiki "Build from a fork" section).

Documentation lives in the wiki, not in the repo: `README.md`, `GUIDE.md` and
`DESIGN.md` are thin link pages pointing at
https://github.com/gilbertchen/duplicacy/wiki. Bugs/features are handled on the
[Duplicacy Forum](https://forum.duplicacy.com/), not GitHub issues.

## Layout

- `duplicacy/duplicacy_main.go` — the only `main` package: CLI definition
  (commands, flags) and wiring into `src`. Also holds `GitCommit` and the
  hard-coded `app.Version` string.
- `src/` — package `duplicacy`, all library logic: chunking, snapshots,
  encryption/erasure coding, backup/restore/prune, entry listing, and every
  storage backend.
- `src/duplicacy_Xstorage.go` — one file per storage backend, typically also
  providing a `CreateXStorage(...)` constructor.
- `src/duplicacy_storage.go` — the `Storage` interface plus `CreateStorage()`,
  which dispatches on the storage URL scheme (`s3://`, `b2://`, `sftp://`, ...).
  Add new backends here.
- `integration_tests/` — bash end-to-end tests.
- `.github/ISSUE_TEMPLATE.md` — redirects users to the forum.

Platform-specific code uses build tags and `_GOOS.go` suffixes
(`duplicacy_utils_linux.go`, `duplicacy_shadowcopy_windows.go`,
`duplicacy_keyring.go` with `// +build !windows`, etc.).

## Build

```
go build -o duplicacy_main duplicacy/duplicacy_main.go
```

- Build only the `duplicacy` main package; the produced binary is
  `duplicacy_main` (that is the name the integration tests expect at the repo
  root).
- `duplicacy_main` is not gitignored, so delete it after building.
- `go build ./...` (or `go install ./duplicacy`) compiles everything for a quick
  check; use the `-o` form above when you need a runnable binary.
- Cross-compile example: `env GOARCH=arm GOOS=linux go build -o out
  duplicacy/duplicacy_main.go`.
- On macOS, cgo is required (see the wiki Installation page).

## Tests

Unit tests (package `duplicacy`, in `src/`):

```
go test ./src/
go test ./src/ -run TestPruneSingleRepository -v
```

They cover chunk maker/operator, backup manager, snapshot pruning, entry
listing/ordering, pattern matching, rate limiting and storage. `TestStorage`
and friends accept flags such as `-storage`, `-quick`, `-threads`,
`-limit-rate`, `-fixed-chunk-size`, `-rsa`, `-erasure-coding`; cloud backends
are configured through a `src/test_storage.conf` file that is not committed
(absent → local `file` storage is used).

Integration tests (bash, require the built binary):

```
go build -o duplicacy_main duplicacy/duplicacy_main.go
cd integration_tests && ./test.sh
```

Individual scenarios: `copy_test.sh`, `fixed_test.sh`, `resume_test.sh`,
`sparse_test.sh`, `threaded_test.sh`. Shared helpers are in
`test_functions.sh`; they hard-code `DUPLICACY=../duplicacy_main` and create
everything under `$HOME/DUPLICACY_TEST_ZONE` (deleted/recreated by the
`fixture` helper). Clean that directory up afterwards.

There is no CI (`.github/` only contains the issue template), so tests have to
be run manually before submitting changes.

`ChunkMaker.AddData` returns three values; the third came in with the OneDrive
cloud-file workaround. `src/duplicacy_chunkmaker_test.go:65` discards the extra
one with `fileSize, _, _ := maker.AddData(...)` — keep that rather than weakening
the test.

`TestEntryExcludeByAttribute` fails on a Linux checkout because it asserts on
macOS `com.apple.metadata` extended attributes; that failure predates the branch
and is not a regression. It is the only test that fails here — `TestPersistRestore`
passes, both on a pristine `upstream/master` checkout and on the branch.

`go vet ./src/` exits 1 on four pre-existing findings (the IPv6 address format in
`sambastorage.go`, the discarded `context.WithTimeout` cancel in `swiftstorage.go`,
and the two malformed `json:` struct tags in `config.go`). `go test ./src/` needs
no `-vet=off`: its default vet subset covers those files without tripping on them,
and the full suite passes with vet enabled.

## Code Conventions

- Every source file starts with the Acrosync LLC copyright header:
  ```
  // Copyright (c) Acrosync LLC. All rights reserved.
  // Free for personal use and commercial trial
  // Commercial use requires per-user licenses available from https://duplicacy.com
  ```
  Keep it at the top of new files. Third-party contributed backends use their
  own copyright line instead.
- Standard Go formatting is required for new/changed code. Note that the repo
  is not fully `gofmt`-clean historically (`gofmt -l src duplicacy` reports many
  pre-existing files), so do not reformat untouched code — only keep your own
  edits formatted.
- Logging: use `LOG_DEBUG` / `LOG_TRACE` / `LOG_INFO` / `LOG_WARN` /
  `LOG_ERROR` / `LOG_FATAL` / `LOG_ASSERT` from `src/duplicacy_log.go` with a
  short uppercase `logID` (e.g. `"STORAGE_CREATE"`, `"CHUNK_MAKER"`) and a
  `printf`-style format string. Use `LOG_WERROR(isWarning, ...)` when a
  condition is a warning only on some platforms.
- Error handling is **not** idiomatic Go: `LOG_ERROR` and above panic with an
  `Exception` value, which is recovered at the top level (and converted into
  `t.Errorf` in tests). Return `err` values are used for recoverable cases;
  follow the surrounding style rather than introducing a new error strategy.
- Tests call `setTestingT(t)` so log output is attached to the test, seed with
  `rand.Seed(time.Now().UnixNano())`, and register `defer recovering(t)` (or
  `defer recoveringWithStack(t)`) from `src/duplicacy_testutils_test.go`, which
  reports an `Exception` via `t.Errorf`. A test that asserts on the panic value
  keeps its own `defer recover()`.
- Version bumps are a one-line change to `app.Version` in
  `duplicacy/duplicacy_main.go`, committed as `Bump version to X.Y.Z`.
- Do not commit build outputs (`duplicacy_main`) or the `DUPLICACY_TEST_ZONE`.

## Working Agreement

- Check `git status` before making changes; do not start work on a dirty tree.
- Keep changes minimal and consistent with the existing per-backend file
  layout; when adding a storage backend, mirror an existing one
  (`CreateStorage` branch + `duplicacy_<name>storage.go` + client file if
  needed).
- Do not commit or push unless explicitly asked.
