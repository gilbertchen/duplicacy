# Performance investigations

Where each `duplicacy` command spends its time: what was measured, what was
changed, and how to confirm the result on a given setup. The
[wiki](https://github.com/gilbertchen/duplicacy/wiki) is the user reference.

| Document | Subject |
| --- | --- |
| `commands.md` | The CLI commands with a one-line description each; flags stay in the wiki. |
| `snapshot_perf.md` | Why listing snapshot revisions is slow; fixes #1-#6 and #8 applied, #7 rejected; the cache decision is now filesystem-based (see `check_perf.md`). |
| `copy_perf.md` | Why `copy` re-encodes chunks and re-probes the destination; fixes #1-#5 applied, #6 rejected. |
| `prune_perf.md` | Where `prune` spends its time; six fixes applied, the last being the chunk cache guard. |
| `check_perf.md` | Where `check` spends its time; the parallel revision loop, the chunk-tree walk, the double file-sequence walk and the snapshot-cache decision all fixed; the chunk cache guard followed from the decision. |
| `restore_perf.md` | Where `restore` spends its time; five performance fixes applied -- the three per-file syscalls (temporary-file probe, duplicated existence check, per-file parent-directory probe), the one-thread metadata expansion and the in-place `ftruncate` -- plus a latent nil dereference in `downloadFile`, the dead `ChunkDownloader` methods and the sparse-file path the existence check had broken; the two `Seek` calls and the per-file `Lstat` in `RestoreMetadata` are examined and retained. |
| `backup_perf.md` | Where `backup` spends its time; the one-thread expansion of the previous revision's metadata sequences fixed, so `-threads` reaches it as it already did in `restore`, `check` and `prune`; the serial local file walk, the serial packing loop and the snapshot-cache clean at the end of every run are recorded as not going to be implemented, with the attribute read and several smaller items examined and retained. |
| `history_perf.md` | Where `history` spends its time; a code review rather than a measurement. The unread chunk-hash sequence expanded per revision (candidate #1), the serial one-thread revision loop (candidate #2, which added `-threads`) and the existence check paid on an explicit `-r` (candidate #3) are all fixed; `FindFile`'s per-revision file-sequence walk is rejected with the chunk index. |
| `diff_perf.md` | Where `diff` spends its time; a measurement. `diff <file>` runs `difflib`'s O(n·m) line diff, so a guard (`-max-diff-size`, default `256m`) compares the revisions by hash and prints why above the limit, leaving the exact diff for the head/tail-trimmed case -- implemented. The whole-snapshot form read the chunk-hash sequence it never used, fixed like `list -files` and `history`; the existence check on an explicit `-r` and the serial two-revision read are recorded. |
| `init_perf.md` | Why `init` is not worth optimising; nothing changed. |
| `highwayhash_arm64.md` | The `zipperMerge` symbol collision in `github.com/gilbertchen/highwayhash`. |
| `branch_review.md` | Duplication and refactoring review of the branch, and the state of each item. |

## Layout of an investigation

The skeleton of a per-command investigation, in order:

- a title naming the command and the cost;
- `## Summary`, with the fixes the investigation produced;
- `## The call path`, naming the functions with their file and line references;
- one `##` section per finding, each ending in `**Implemented**` or `**Not going
  to be implemented**`;
- `## Candidate fixes`, with `### Smaller items` and `### Deliberately not
  pursued` where they apply;
- `## How to confirm on a given setup`, the commands, log lines and tests that
  show the cost or guard the fix.

`snapshot_perf.md` keeps `## Candidate fixes` after the confirmation steps,
`prune_perf.md` records each fix inside its finding section rather than in a
candidate list, `history_perf.md` is a code review that cites the measurements
of the documents it mirrors rather than its own, `diff_perf.md` measures its own
command with `/usr/bin/time` and `strace`, guards the quadratic line diff of
`diff <file>` and records the other three findings unimplemented, and
`highwayhash_arm64.md` is a build diagnosis that ends at the decision on the
dependency rather than a per-command investigation.

## Tracing syscalls

`strace -f -c -T -e trace=fsync,renameat,openat,newfstatat,mkdirat duplicacy
<command>` sums the wall time of the file operations a command issues, per
syscall; swap in the syscalls the run is suspected of issuing (`unlinkat` for
the snapshot deletions, for instance). A dominant `fsync` is the chunk-cache
write, and the call counts identify the loop issuing them. `strace -f -T`
without `-c` times each call individually, which is how the drvfs attribution
table in `snapshot_perf.md` was produced.
