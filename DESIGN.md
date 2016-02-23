## Lock-free deduplication

## Snapshot Format

A snapshot file is a file that the backup procedure uploads to the file storage after it finishes breaking files into
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

When Duplicacy splits a file in chunks, if the end of a file is reached and yet the boundary marker for terminating a chunk
hasn't been found, the next file, if there is one, will be read in and the chunking algorithm continues. It is as if all 
files were packed into a big zip file which is then split into chunks.

The *content* field of a file indicates the indexes of starting and ending chunks and the corresponding offsets. For
instance, *fiel1* starts at chunk 0 offset 0 while ends at chunk 2 offset 6108, immediately followed by *file2*.

The backup procedure can run in one of two modes. In the quick mode, only modified or new files are scanned. Chunks only
referenced by old files that have been modified are removed from the chunk sequence, and then chunks referenced by new 
files are appended. Indices for unchanged files need to be updated too.

In the safe mode, all files are scanned and the chunk sequence is regenerated.

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

Under the extreme case of the respository remainging unchanged since last backup, no new chunks will be uploaded, as shown by the output of a real run:

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
