# Duplicacy: A new generation cloud backup tool

This repository contains only binary releases and documentation for Duplicacy.  It also serves as an issue tracker for user-developer communication during the beta testing phase.

## Overview

Duplicacy supports major cloud storage providers (Amazon S3, Google Cloud Storage, Microsoft Azure, Dropbox, and BackBlaze) and offers all essential features of a modern backup tool:

* Incremental backup: only back up what has been changed
* Full snapshot : although each backup is incremental, it behaves like a full snapshot
* Deduplication: identical files must be stored as one copy (file-level deduplication), and identical parts from different files must be stored as one copy (block-level deduplication)
* Encryption: encrypt not only file contents but also file paths, sizes, times, etc.
* Deletion: every backup can be deleted independently without affecting others
* Concurrent access: multiple clients can back up to the same storage at the same time

The key idea behind Duplicacy is a concept called **Lock-Free Deduplication**, which can be summarized as follows:

* Use variable-size chunking algorithm to split files into chunks
* Store each chunk in the storage using a file name derived from its hash, and rely on the file system API to manage chunks without using a centralized indexing database
* Apply a *two-step fossil collection* algorithm to remove chunks that become unreferenced after a backup is deleted

The [design document](https://github.com/gilbertchen/duplicacy-beta/blob/master/DESIGN.md) explains lock-free deduplication in detail.

## Getting Started

During beta testing only binaries are available.  Please visit the [releases page](https://github.com/gilbertchen/duplicacy-beta/releases/latest) to download and run the executable for your platform.  Installation is not needed.

Once you have the Duplicacy executable under your path, you can change to the directory that you want to back up (called *repository*) and run the *init* command:

```
$ cd path/to/your/repository
$ duplicacy init mywork sftp://192.168.1.100/path/to/storage
```
This *init* command connects the repository with the remote storage at 192.168.1.00 via SFTP.  It will initialize the remote storage if this has not been done before.  It also assigns the snapshot id *mywork* to the repository.  This snapshot id is used to uniquely identify this repository if there are other repositories that also back up to the same storage.

You can now create snapshots of the repository by invoking the *backup* command.  The first snapshot may take a while depending on the size of the repository and the upload bandwidth.  Subsequent snapshots will be much faster, as only new or modified files will be uploaded.  Each snapshot is identified by the snapshot id and an increasing revision number starting from 1.

```sh
$ duplicacy backup -stats
```

Duplicacy provides a set of commands, such as list, check, diff, cat history, to manage snapshots:

```makefile
$ duplicacy list            # List all snapshots
$ duplicacy check           # Check integrity of snapshots
$ duplicacy diff            # Compare two snapshots, or the same file in two snapshots
$ duplicacy cat             # Print a file in a snapshot
$ duplicacy history         # Show how a file changes over time
```

The *restore* command rolls back the repository to a previous revision:
```sh
$ duplicacy restore -r 1
```

The *prune* command removes snapshots by revisions, or tags, or retention policies:

```sh
$ duplicacy prune -r 1            # Remove the snapshot with revision number 1
$ duplicacy prune -t quick        # Remove all snapshots with the tag 'quick'
$ duplicacy prune -keep 1:7       # Keep 1 snapshot per day for snapshots older than 7 days
$ duplicacy prune -keep 7:30      # Keep 1 snapshot every 7 days for snapshots older than 30 days
$ duplicacy prune -keep 0:180     # Remove all snapshots older than 180 days
```

The first time the *prune* command is called, it removes the specified snapshots but keeps all unreferenced chunks as fossils.
Since it uses the two-step fossil collection algorithm to clean chunks, you will need to run it again to remove those fossils from the storage:

```sh
$ duplicacy prune           # Chunks from deleted snapshots will be removed if deletion criteria are met
```

To back up to multiple storages, use the *add* command to add a new storage.  The *add* command is similar to the *init* command, except that the first argument is a storage name used to distinguish different storages:

```sh
$ duplicacy add s3 mywork s3://amazon.com/mybucket/path/to/storage
```

You can back up to any storage by specifying the storage name:

```sh
$ duplicacy backup -storage s3
```

However, snapshots created this way will be different on different storages, if the repository has been changed during two backup operations.  A better approach, is to use the *copy* command to copy specified snapshots from one storage to another:

```sh
$ duplicacy copy -r 1 -to s3   # Copy snapshot at revision 1 to the s3 storage
$ duplicacy copy -to s3        # Copy every snapshot to the s3 storage
```

The [User Guide](https://github.com/gilbertchen/duplicacy-beta/blob/master/DESIGN.md) contains a complete reference to
all commands and other features of Duplicacy.

## Storages

Duplicacy currently supports local file storage, SFTP, and 5 cloud storage providers.

#### Local disk

```
Storage URL:  /path/to/storage (on Linux or Mac OS X)
              C:\path\to\storage (on Windows)
```

#### SFTP

```
Storage URL:  sftp://username@server/path/to/storage
```

Login methods include password authentication and public key authentication.  Due to a limitation of the underlying Go SSH library, the key pair for public key authentication be generated without a passphrase.  To work with a key that has a passphrase, you can set up SSH agent forwarding which is also supported by Duplicacy.

#### Dropbox

```
Storage URL:  dropbox://path/to/storage
```

For Duplicacy to access your Dropbox storage, you must provide an access token that can be obtained in one of two ways:

* Create your own app on the [Dropbox Developer](https://www.dropbox.com/developers) page, and then generate the [access token](https://blogs.dropbox.com/developers/2014/05/generate-an-access-token-for-your-own-account/)

* Or authorize Duplicacy to access its app folder inside your Dropbox (following [this link](https://dl.dropboxusercontent.com/u/95866350/start_dropbox_token.html)), and Dropbox will generate the access token (which is not visible to us, as the redirect page showing the token is merely a static html hosted by Dropbox)

Dropbox has two advantages over other cloud providers.  First, if you are already a paid user then to use the unused space as the backup storage is basically free.  Second, unlike other providers Dropbox does not charge API usage fees.

#### Amazon S3

```
Storage URL:  s3://amazon.com/bucket/path/to/storage (default region is us-east-1)
              s3://region@amazon.com/bucket/path/to/storage (other regions must be specified)
```

You'll need to input an access key and a scret key to access your Amazon S3 storage.


#### Google Cloud Storage

```
Storage URL:  s3://storage.googleapis.com/bucket/path/to/storage
```

Duplicacy uses the s3 protocol to access Google Cloud Storage, so you must enable the [s3 interoperability](https://cloud.google.com/storage/docs/migrating#migration-simple) in your Google Cloud Storage settings.

#### Microsoft Azure

```
Storage URL:  azure://account/container
```

You'll need to input the access key once prompted.

#### BackBlaze

```
Storage URL: b2://bucket
```

You'll need to input the account id and application key.

BackBlaze offers perhaps the least expensive cloud storage at 0.5 cent per GB per month.  Unfortunately their API does not support file renaming, so the -exclusive option is required when pruning old backups.  This means concurrent access and deletion can't be permitted at the same time.

## Comparison with Other Backup Tools

[duplicity](http://duplicity.nongnu.org) works by applying the rsync algorithm (or more specific, the [librsync](https://github.com/librsync/librsync) library)
to find the differences from previous backups and only then uploading the differences.  It is the only existing backup tool with extensive cloud support -- the [long list](http://duplicity.nongnu.org/duplicity.1.html#sect7) of storage backends covers almost every cloud provider one can think of.  However, duplicity's biggest flaw lies in its incremental model -- a chain of dependent backups starts with a full backup followed by a number of incremental ones, and ends when another full backup is uploaded.  Deleting one backup will render useless all the subsequent backups on the same chain.  Periodic full backups are required, in order to make previous backups discardable.

[bup](https://github.com/bup/bup) also uses librsync to split files into chunks but save chunks in the git packfile format.  It doesn't support any cloud storage, or deletion of old backups.

[Obnam](http://obnam.org) got the incremental backup model right in the sense that every incremental backup is actually a full snapshot.  Although Obnam also splits files into chunks, it does not adopt either the rsync algorithm or the variable-size chunking algorithm.  As a result, deletions or insertions of a few bytes will foil the
[deduplication](http://obnam.org/faq/dedup).
Deletion of old backups is possible, but no cloud storages are supoprted.
Multiple clients can back up to the same storage, but only sequential access is granted by the [locking on-disk data structures](http://obnam.org/locking/).
It is unclear if the lack of cloud backends is due to difficulties in porting the locking data structures to cloud storage APIs.

[Attic](https://attic-backup.org) has been acclaimed by some as the [Holy Grail of bacups](https://www.stavros.io/posts/holy-grail-backups).  It follows the same incremental backup model as Obnam, but embraces the variable-size chunk algorithm for better performance and better deduplication.  Deletions of old backup is also supported.  However, no cloud backends are implemented, as in Obnam.  Although concurrent backups from multiple clients to the same storage is in theory possible by the use of locking, it is 
[not recommended](http://librelist.com/browser//attic/2014/11/11/backing-up-multiple-servers-into-a-single-repository/#e96345aa5a3469a87786675d65da492b) by the developer due to chunk indices being kept in a local cache. 
Concurrent access is not only a convenience; it is a necessity for better deduplication.  For instance, if multiple machines with the same OS installed can back up their entire drives to the same storage, only one copy of the system files needs to be stored, greatly reducing the storage space regardless of the number of machines.  Attic still adopts the traditional approach of using a centralized indexing database to manage chunks, and relies heavily on caching to improve performance.  The presence of exclusive locking makes it hard to be adapted for cloud storage APIs and reduces the level of deduplication.

[restic](https://restic.github.io) is a more recent addition.  It is worth mentioning here because, like Duplicacy, it is written in Go.  It uses a format similar to the git packfile format, but not exactly the same.  Multiple clients backing up to the same storage are still guarded by 
[locks](https://github.com/restic/restic/blob/master/doc/Design.md#locks).
A command to delete old backups is in the developer's [plan](https://github.com/restic/restic/issues/18). S3 storage is supported, although it is unclear how hard it is to support other clould storage APIs because of the need for locking.  Overall, it still falls in the same category as Attic.  Whether it will eventually reach the same level as Attic remains to be seen.

The following table compares the feature lists of all these backup tools:

| Tool | Incremental Backup | Full Snapshot | Deduplication | Encryption | Deletion | Concurrent Access |Cloud Support |
|:----:|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
| duplicity | Yes | No  | Weak | Yes | No  | No | Extensive |
| bup       | Yes | Yes | Yes  | Yes | No  | No | No   |
| Obnam     | Yes | Yes | Weak | Yes | Yes | Exclusive locking | No   |
| Attic     | Yes | Yes | Yes  | Yes | Yes | Not recommended | No   |
| restic    | Yes | Yes | Yes  | Yes | No  | Exclusive locking | S3 only |
| **Duplicacy** | **Yes** | **Yes** | **Yes**  | **Yes** | **Yes** | **Lock-free** | **S3, GCS, Azure, Dropbox, BackBlaze** |

