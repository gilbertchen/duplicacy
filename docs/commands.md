# Commands

The commands provided by the `duplicacy` CLI, with a short description of what
each one does. Arguments and flags are not documented here; run
`duplicacy <command> -h` for the full list for a command, or consult the
[wiki](https://github.com/gilbertchen/duplicacy/wiki) for the detailed
reference.

| Command | Description |
| --- | --- |
| `init` | Initialize the storage if necessary and the current directory as the repository. |
| `backup` | Save a snapshot of the repository to the storage. |
| `restore` | Restore the repository to a previously saved snapshot, optionally limited to the files matching the given patterns. |
| `list` | List snapshots, optionally printing the files or the chunks in each of them. |
| `check` | Check the integrity of snapshots by verifying their chunks and files. |
| `cat` | Print to stdout the specified file, or the content of the snapshot if no file is specified. |
| `diff` | Compare two snapshots, or two revisions of a file. |
| `history` | Show the history of a file across the revisions of a snapshot. |
| `prune` | Prune snapshots by revision, tag, or retention policy, and collect the chunks they leave unreferenced. |
| `password` | Change the storage password. |
| `add` | Add an additional storage to be used for the existing repository. |
| `set` | Change the options for the default or the specified storage. |
| `copy` | Copy snapshots between compatible storages. |
| `info` | Show the information about the specified storage. |
| `benchmark` | Run a set of benchmarks to test download and upload speeds. |
