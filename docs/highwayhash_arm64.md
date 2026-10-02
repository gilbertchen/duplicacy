# The `zipperMerge` symbol collision in `github.com/gilbertchen/highwayhash`

Review of the `github.com/gilbertchen/highwayhash` dependency, prompted by an
`arm64` link failure under Go 1.27 and by the removal of the now-unused
`github.com/gilbertchen/xattr` requirement in `7c98865`. The outcome is
deliberately "no change": the current pin is kept as it is and the hope is that
upstream `cmd/link` stops rejecting the symbol. This document records what was
found so that decision can be revisited with the evidence in hand.

## Summary

There are two `highwayhash` modules in `go.mod`:

| Module | Pin | Role |
| --- | --- | --- |
| `github.com/minio/highwayhash` | `v1.0.4` | the real hashing, used for chunk hashes and shard hashes |
| `github.com/gilbertchen/highwayhash` | `v0.0.0-20221109044721-eeab1f4799d8` | a copy of `v1.0.1`, used **only** to recognise chunks whose shard hashes were written by the buggy 1.0.1 on arm64 |

The fork was added in `6a7a2c8` ("Upgrade github.com/minio/highwayhash to
1.0.2", 2022-11-09) for exactly one reason, in the author's words:

> highwayhash 1.0.1 contains a bug leading to incorrect hashes on arm64
> machines. The 1.0.1 version is retained in github.com/gilbertchen/highwayhash
> so the hash can be checked again if a mismatch is detected by 1.0.2.

The fork is **byte-identical to upstream `minio/highwayhash v1.0.1` except for
its `go.mod`** — every `.go` and `.s` file matches. It carries no patches of its
own, so its `arm64` defect is inherited, not introduced.

That defect turns out to be a **symbol name collision**, and the collision is
also what breaks the build under Go 1.27. The two symptoms have a single cause,
which is why the "keep the wrong hashes but fix the assembly" option does not
exist.

## The collision

Two different things are named `zipperMerge` in the same package:

```
highwayhash_generic.go:127   func zipperMerge(v0, v1 uint64, d0, d1 *uint64) {   // a Go function
highwayhash_arm64.s:326      GLOBL ·zipperMerge(SB), 8, $48                       // a 48-byte data table
highwayhash_arm64.s:143,176  MOVD $·zipperMerge(SB), R3                           // takes "the table"'s address
```

`highwayhash_generic.go` has no build constraint, so the function is compiled on
every architecture, including `arm64`. The assembly declares a 48-byte constant
table under the same name and then loads its address with `MOVD
$·zipperMerge(SB), R3`, followed by `VLD1 (R3), [V28.B16, V29.B16, V30.B16]`,
which reads 48 bytes from whatever that symbol resolves to.

On `amd64` there is no collision: `highwayhash_amd64.s` uses the `<>` static
suffix, which keeps the symbol package-local:

```
GLOBL ·cons<>(SB), (NOPTR+RODATA), $64
GLOBL ·zipperMerge<>(SB), (NOPTR+RODATA), $16
```

On `arm64` the suffix is missing, so `·zipperMerge` resolves to the **Go
function**, and the vector load reads 48 bytes of its compiled machine code.

## What the assembly actually reads

To confirm this rather than infer it, the address that the buggy build resolves
for `·zipperMerge` was printed and 48 bytes were dumped from it:

```
consts: 04 1c 70 92 25 1c 48 92 06 1c 58 92 27 1c 50 92 c6 00 07 8b ...
q0 = $0x92481c2592701c04
q1 = $0x92501c2792581c06
q2 = $0x92681c078b0700c6
q3 = $0x8b07010792601c28
q4 = $0xf940004992781c08
q5 = $0x8b4640848b452084
```

Those are recognisable AArch64 instructions, not constant data — `92` is a
`MOV`/`ADD` immediate, `8b` an `ADD` register, `f9` an `LDR`, and the trailing
`...d1` is the `RET` of the function. The zipper-merge tables that the code was
written to consume never participate in the hash at all.

## Why the hash is wrong on arm64 only

The buggy `zipperMerge` transformation therefore operates on instruction bytes
as if they were the intended constant table. On `amd64` the table is reached
correctly, so upstream 1.0.1 and 1.0.4 agree there, which is why the problem
never showed up outside arm64.

Measured with the two modules built for `linux/arm64` and run under
`qemu-aarch64-static`, hashing the same message with a 32-byte key:

| Build | Hash |
| --- | --- |
| fork (1.0.1, collision present) | `8031ad1ae6f12533f7810d3d6e8063defe597042c12ff5e21b0976b8e40a1d2e` |
| `minio/highwayhash v1.0.4` | `b65b406ea1acda42d654726ecf7264c8f4025332d74ae4af3772c14e8b3462e7` |
| on `amd64`, both builds | `b65b406ea1acda42d654726ecf7264c8f4025332d74ae4af3772c14e8b3462e7` |

across 32 keys × 8 message lengths, the fork differed from 1.0.4 in **all 256**
comparisons on arm64 and in **none** on amd64.

## The Go 1.27 link failure

The same symbol is now rejected by the linker. On arm64 targets only:

```
link: error: non-function sym 858772/github.com/gilbertchen/highwayhash.zipperMerge t=SRODATA passed to GetFuncDwarfAuxSyms
```

The message comes from the Go linker itself, in
`cmd/link/internal/loader/loader.go`, which fatals when a symbol reached while
walking function DWARF is not a text symbol — and the colliding `zipperMerge` is
`SRODATA`, a data symbol. Observed per toolchain and target:

| Target | go1.26.7 | go1.27.1 |
| --- | --- | --- |
| `linux/amd64` | OK | OK |
| `darwin/amd64` | OK | OK |
| `windows/amd64` | OK | OK |
| `linux/arm64` | OK | **FAIL** |
| `darwin/arm64` | OK | **FAIL** |

So the failure is driven by the target architecture, not by the target OS, and
by the toolchain, not by cross-compilation. `go.mod` declares `go 1.26.0`, and
Go 1.26.7 cross-compiles both arm64 targets successfully today. `minio/highwayhash`
`v1.0.1` fails the same way, and `v1.0.2` onwards is fine, which is consistent
with upstream having fixed the collision in 1.0.2 by renaming the table.

For reference, this was checked against the project rather than only in
isolation: the same minimal-module reproduction needs no `duplicacy` code at
all, and the failure reproduces on a pristine checkout, so it predates `1baf540`.

## The fix and the bug are the same thing

Applying the 1.0.2-style rename (giving the table a non-colliding name) and
re-measuring under `qemu-aarch64-static`:

| Variant | Links on 1.27 | arm64 hash vs 1.0.4 |
| --- | --- | --- |
| pristine fork (collision present) | no | different |
| table renamed to a non-colliding name | yes | **equal** |
| only the `constants` table renamed | no | still different |
| the Go function renamed instead | yes | **equal** |

Renaming the *table* fixes the link failure and simultaneously makes the hash
correct. Renaming only the non-colliding `constants` symbol fixes neither.
Renaming only the *Go function* (leaving the assembly symbol names alone) fixes
both as well, because either rename removes the collision.

Every change that stops the assembly from reading function bytes also stops it
from producing the 1.0.1 hash. There is no patch that repairs the build while
preserving the compatibility behaviour. A further check confirmed the mechanism
directly: editing the body of the `zipperMerge` function without renaming
anything left the wrong arm64 hash unchanged, i.e. the "constant table" really
is the function's code.

## Why the wrong hash is not a fixed value

Because the loaded table is compiled code, the buggy hash depends on compiler
codegen rather than on any defined input. The fork was rebuilt with several
toolchains and the same message hashed on arm64:

| Toolchain | fork arm64 hash |
| --- | --- |
| go1.21.13 | `8031ad1a…` |
| go1.23.12 | `8031ad1a…` |
| go1.24.11 | `8031ad1a…` |
| go1.25.5 | `8031ad1a…` |
| go1.26.7 | `8031ad1a…` |

They agree with each other, which is encouraging for the shim: it means the
value has been stable across the toolchains that can be tested, so it very
likely still matches the hashes that 2.7.0–3.0.1-era arm64 clients wrote. But it
is still a codegen artefact. Baking those 48 bytes into a renamed table produced
a third, unrelated value, confirming that the bytes alone do not define the
"wrong hash" and that it cannot be reproduced deliberately without the collision.
The exact compiler used for the affected releases could not be checked here,
because Go 1.19 and 1.20 are not installed.

## Exposure

`github.com/minio/highwayhash` has been pinned to the buggy `v1.0.1` commit
since the erasure-coding feature shipped, and the shard hashes are what the
fallback is about:

- Erasure coding, and with it the per-shard `highwayhash` of each shard, was
  added in `923a6fb` ("Implement Erasure Coding", 2020-09-03), and the hashes
  are written into the chunk body in `Chunk.Encrypt` (`src/duplicacy_chunk.go:448`).
  `Decrypt` already treats the stored hash as the arbiter of shard validity in
  that first version, so the layout and the dependency have been unchanged since.
- The erasure-coding feature was first released in 2.7.0 (`e0a72ef`, 2020-09-26),
  whose `Gopkg.lock` pins highwayhash at revision
  `86a2a969d04373bf05ca722517d30fb1c9a3e4f9` — the `v1.0.1` commit of
  2020-09-16, i.e. the buggy one. Before that there is no `highwayhash`
  requirement in `Gopkg.lock` at all.
- `5c35ef7` ("Switch to go modules", 2022-10-04) pinned `v1.0.1` explicitly;
  the fix arrived in `6a7a2c8` and was released in 3.1.0 (`27ff3e2`,
  2022-12-06).

So the window in which an arm64 client could write wrong shard hashes is
**2.7.0 through 3.0.1** — wider than the source comment in
`src/duplicacy_chunk.go:29-31` implies, since that names only 3.0.1. Every
release in the window had the buggy pin *and* erasure coding. It still only
affects repositories that had the `-erasure-coding` option enabled, because it
is opt-in; default repositories are unaffected.

The shard hashes are written at upload time and never recomputed, so an affected
chunk keeps its wrong hashes in the storage permanently. The obligation is
therefore **data-shaped, not client-shaped** — reading such a repository from any
machine needs the 1.0.1 result, because the stored hashes are wrong, not the
reader. The `runtime.GOARCH == "arm64"` guard in `src/duplicacy_chunk.go:561`
reflects the assumption that only arm64 clients could have written them, which
is how it worked out in practice.

With the shim in place, a mismatched shard is recognised, logged as
`CHUNK_ERASURECODE`, and flagged for rewrite (`rewriteNeeded`), which is what
keeps those chunks readable. Without it, every shard of such a chunk fails its
check, `availableShards` can fall below `dataShards`, and recovery aborts with
`Not enough chunk data for recover; only N out of M shards are complete`, with
no rewrite attempted — a failed restore or check, and no automatic repair.

## Decision

**Keep the current pin and wait for the linker to be fixed upstream.** That
preserves the `arm64` compatibility behaviour in full, and the practical cost is
only that arm64 builds need a Go 1.26 toolchain — which is what `go.mod` already
declares — until the `cmd/link` rejection goes away.

The alternatives, for the record:

- **Patch the fork** (rename the table) so it links on any toolchain. Loses the
  shim: affected arm64 repositories would stop being readable, and the
  diagnostic and the automatic rewrite go with it.
- **Reimplement the 1.0.1 arm64 result in pure Go** and drop the fork. The only
  route that fixes the build *and* keeps compatibility, since it does not depend
  on a collision. It needs the buggy hash captured from a known-good arm64
  build and validated against a real 2.7.0–3.0.1-era corpus, so it is real work
  rather than an assembly tweak.
- **Drop the fork and accept the data loss** for 2.7.0–3.0.1-era arm64
  erasure-coded repositories.

Two things that do *not* work, recorded so they are not tried again:

- The `-tags noasm` build tag appears to help, because it switches the fork to
  the generic implementation and it does link. But the generic implementation is
  *correct*, so the tag silently defeats the shim: the fork then agrees with
  1.0.4 and can no longer recognise the affected chunks.
- Renaming only the `constants` table changes neither the link failure nor the
  hash; only removing the `zipperMerge` collision does.

## How to confirm on a given setup

- Cross-compile for the two arm64 targets and the amd64 ones; only arm64 fails:

  ```
  for t in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
      env GOOS=${t%/*} GOARCH=${t#*/} go build -o /dev/null ./duplicacy/
  done
  ```

- Check which toolchain the failure starts at. On this machine Go 1.26.7 is at
  `/snap/go/11262/bin/go` and Go 1.27.1 is the default `go`.
- To see the hash divergence without an arm64 host, build the two modules for
  `linux/arm64` and run them under `qemu-aarch64-static`, comparing the sums
  against the same program built for `amd64`.
- `go tool nm` on an arm64 build shows the collision plainly: with the pristine
  fork, `zipperMerge` is a `T` (text) symbol and there is no data symbol of that
  name; after renaming the table it becomes an `R` symbol and the function keeps
  the plain name.
- Reproduce the whole thing in isolation with a throwaway module that imports
  both `highwayhash` modules directly, so no `duplicacy` code is involved.
