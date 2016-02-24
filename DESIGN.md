## Lock-Free Deduplication

The three elements of lock-free deduplication are:

* Use variable-size chunking algorithm to split files into chunks
* Store each chunk in the storage using a file name derived from its hash, and rely on the file system API to manage chunks without using a centralized indexing database
* Apply a *two-step fossil collection* algorithm to remove chunks that become unreferenced after a backup is deleted

The variable-size chunking algorithm, also called Content-Defined Chunking, is well-known and has been adopted by many
backup tools. The main advantage of the variable-size chunking algorithm over the fixed-size chunking algorithm (as used
by rsync) is that in the former the rolling hash is only used to search for boundaries between chunks, after which a far
more collision-resistant hash function like MD5 or SHA256 is applied on each chunk. In contrast, in the fixed-size
chunking algorithm, for the purpose of detecting inserts or deletions, a lookup in the known hash table is required every
time the rolling hash window is shifted by one byte, thus significantly reducing the chunk splitting performance.

What is novel about lock-free deduplication is the absence of a centralized indexing database for tracking all existing
chunks and for determining which chunks are not needed any more.  Instead, to check if a chunk has already been uploaded
before, one can just perform a file lookup via the file storage API using the file name derived from the hash of the chunk.
This effectively turn a cloud storage offering only a very limited
set of basic file operations into a powerful modern backup backend capable of both block-level and file-level deduplication.  More importantly, the absence of a centralized indexing database means that there is no need to implement a distributed locking mechanism on top of the file storage.

There is one problem, though.
Deletion of snapshots without an indexing database, when concurrent access is permitted, turns out to be a hard problem.
If exclusive access to a file storage by a single client can be guaranteed, the deletion procedure can simply search for
chunks not referenced by any backup and delete them. However, if concurrent access is required, an unreferenced chunk
can't be trivially removed, because of the possibility that a backup procedure in progress may reference the same chunk.
The ongoing backup procedure, still unknown to the deletion procedure, may have already encountered that chunk during its
file scanning phase, but decided not to upload the chunk again since it already exists on the file storage. 

Fortunately, there is a solution to address the deletion problem and make lock-free deduplication practical.  The solution is an algorithm that we call *two-step fossil collection* that deletes unreferenced chunks in two steps: identify and collect them in the first step, and then permanently remove them when certain conditions are met.

## Two-Step Fossil Collection

Interestingly, the two-step fossil collection algorithm hinges on a basic file operation supported almost universally, *file renaming*.
When the deletion procedure identifies a chunk not referenced by any known snapshots, instead of deleting the chunk file
immediately, it changes the name of the chunk file (and possibly moves it to a different directory).
A chunk that has been renamed is called a *fossil*.

The fossil still exists on the file storage.  Two rules are enforced regarding the access of fossils:

* A restore, list, or check procedure that reads existing backups can read the fossil if the original chunk cannot be found.
* A backup procedure does not check the existence of a fossil. That is, it must upload a chunk if it cannot find the chunk, even if an equivalent fossil exists.
 
In the first step of the deletion procedure, called the *fossil collection* step, the names of all identified fossils will
be saved in a fossil collection file. The deletion procedure then exits without performing further actions. This step has not effectively changed any chunk references due to the first fossil access rule.

The second step, called the *fossil deletion* step, will permanently delete fossils, but only when two conditions are met:

* For each snapshot id, there is a new snapshot that was not seen by the fossil collection step
* The new snapshot must finish after the fossil collection step

The first condition guarantees that if a backup procedure references a chunk before the deletion procedure turns it into a fossil, the reference will be detected in the fossil deletion step which will then turn the fossil back into a normal chunk.

The second condition guarantees that any backup procedure unknown to the fossil deletion step can start only after the fossil collection step finishes.  Therefore, if it references a chunk that was identified as fossil in the fossil collection step, it should observe the fossil, not the chunk, so it will upload a new chunk, according to the second fossil access rule.

## Snapshot Format

A snapshot file is a file that the backup procedure uploads to the file storage after it finishes splitting files into
chunks and uploading all new chunks. It mainly contains metadata for the backup overall, metadata for all the files,
and chunk references for each file. Here is an example snapshot file for a repository containing 3 files (file1, file2,
and dir1/file3):

```json
{
  "id": "host1",
  "revision": 1,
  "tag": "first",
  "start_time": 1455590487,
  "end_time": 1455590487,
  "files": [
    {
      "path": "file1",
      "content": "0:0:2:6108",
      "hash": "a533c0398194f93b90bd945381ea4f2adb0ad50bd99fd3585b9ec809da395b51",
      "size": 151901,
      "time": 1455590487,
      "mode": 420
    },
    {
      "path": "file2",
      "content": "2:6108:3:7586",
      "hash": "f6111c1562fde4df9c0bafe2cf665778c6e25b49bcab5fec63675571293ed644",
      "size": 172071,
      "time": 1455590487,
      "mode": 420
    },
    {
      "path": "dir1/",
      "size": 102,
      "time": 1455590487,
      "mode": 2147484096
    },
    {
      "path": "dir1/file3",
      "content": "3:7586:4:1734",
      "hash": "6bf9150424169006388146908d83d07de413de05d1809884c38011b2a74d9d3f",
      "size": 118457,
      "time": 1455590487,
      "mode": 420
    }
  ],
  "chunks": [
    "9f25db00881a10a8e7bcaa5a12b2659c2358a579118ea45a73c2582681f12919",
    "6e903aace6cd05e26212fcec1939bb951611c4179c926351f3b20365ef2c212f",
    "4b0d017bce5491dbb0558c518734429ec19b8a0d7c616f68ddf1b477916621f7",
    "41841c98800d3b9faa01b1007d1afaf702000da182df89793c327f88a9aba698",
    "7c11ee13ea32e9bb21a694c5418658b39e8894bbfecd9344927020a9e3129718"
  ],
  "lengths": [
    64638,
    81155,
    170593,
    124309,
    1734
  ] 
}
```

When Duplicacy splits a file in chunks using the variable-size chunking algorithm, if the end of a file is reached and yet the boundary marker for terminating a chunk
hasn't been found, the next file, if there is one, will be read in and the chunking algorithm continues. It is as if all 
files were packed into a big tar file which is then split into chunks.

The *content* field of a file indicates the indexes of starting and ending chunks and the corresponding offsets. For
instance, *fiel1* starts at chunk 0 offset 0 while ends at chunk 2 offset 6108, immediately followed by *file2*.

The backup procedure can run in one of two modes. In the default quick mode, only modified or new files are scanned. Chunks only
referenced by old files that have been modified are removed from the chunk sequence, and then chunks referenced by new 
files are appended. Indices for unchanged files need to be updated too.

In the safe mode (enabled by the -hash option), all files are scanned and the chunk sequence is regenerated.

The length sequence stores the lengths for all chunks, which are needed when calculating some statistics such as the total
length of chunks. For a repository containing a large number of files, the size of the snapshot file can be tremendous. 
To make the situation worse, every time a big snapshot file would have been uploaded even if only a few files have been changed since
last backup. To save space, the variable-size chunking algorithm is also applied to the three dynamic fields of a snapshot
file, *files*, *chunks*, and *lengths*.

Chunks produced during this step are deduplicated and uploaded in the same way as regular file chunks. The final snapshot file
contains sequences of chunk hashes and other fixed size fields:

```json
{
  "id": "host1",
  "revision": 1,
  "start_time": 1455590487,
  "tag": "first",
  "end_time": 1455590487,
  "file_sequence": [
    "21e4c69f3832e32349f653f31f13cefc7c52d52f5f3417ae21f2ef5a479c3437",
  ],
  "chunk_sequence": [
    "8a36ffb8f4959394fd39bba4f4a464545ff3dd6eed642ad4ccaa522253f2d5d6"
  ],
  "length_sequence": [
    "fc2758ae60a441c244dae05f035136e6dd33d3f3a0c5eb4b9025a9bed1d0c328"
  ]
}
```

In the extreme case where the repository has not been modified since last backup, a new backup procedure will not create any new chunks,
as shown by the following output from a real use case:

```
$ duplicacy backup -stats
Storage set to sftp://gchen@192.168.1.100/Duplicacy
Last backup at revision 260 found
Backup for /Users/gchen/duplicacy at revision 261 completed
Files: 42367 total, 2,204M bytes; 0 new, 0 bytes
File chunks: 447 total, 2,238M bytes; 0 new, 0 bytes, 0 bytes uploaded
Metadata chunks: 6 total, 11,753K bytes; 0 new, 0 bytes, 0 bytes uploaded
All chunks: 453 total, 2,249M bytes; 0 new, 0 bytes, 0 bytes uploaded
Total running time: 00:00:05
```
