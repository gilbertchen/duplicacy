# Duplicacy: A new generation cloud backup tool based on Lock-Free Deduplication

Duplicacy supports major cloud storage providers (Amazon S3, Googld Cloud Storage, Microsoft Azure, Dropbox, and Blaze) and at the same time offers all essential features of a modern backup tool:

* Incremental backup: only back up what has been changed
* Full snapshot : although each backup is incremental, it must appear to be a full snapshot independent of others
* Deduplication: identical files must be stored as one copy (file-level deduplication), and identical parts from different files must be stored as one copy (block-level deduplication)
* Encryption: encrypt not only file contents but also file paths, sizes, times, etc.
* Deletion: every backup can be deleted independently without affecting others
* Concurrent access: multiple clients can back up to the same storage at the same time

The key idea behind Duplicacy is a technique called **Lock-Free Deduplication**.  There are three elements of lock-free deduplication:

* Use variable-size chunking algorithm to split files into chunks
* Store each chunk in the storage using a file name derived from its hash, and rely on the file system API to manage chunks without using a centralized indexing database
* Apply a *two-step fossil collection* algorithm to remove chunks that become unreferenced after a backup is deleted

## Getting Started

```sh
$ cd path/to/your/dir
$ duplicacy init mywork sftp://192.168.1.100/Duplicacy
```

```sh
$ duplicacy backup
```

```sh
$ duplicacy list
```

```sh
$ duplicacy add s3 mywork s3://amazon.com/duplicacy/mywork
```

```sh
$ duplicacy copy -r 1-2 -to s3
```




