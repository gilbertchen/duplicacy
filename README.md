# Duplicacy: A lock-free deduplication cloud backup tool

Duplicacy is a new generation cross-platform cloud backup tool based on the idea of [Lock-Free Deduplication](https://github.com/gilbertchen/duplicacy-cli/blob/master/DESIGN.md).  It is the only cloud backup tool that allows multiple computers to back up to the same storage simultaneously without using any locks (thus readily amenable to various cloud storage services).

The repository hosts source code, design documents, and binary releases of the command line version.  There is also a Duplicacy GUI frontend built for Windows and Mac OS X available from https://duplicacy.com.

There is a special edition of Duplicacy developed for VMware vSphere (ESXi) named [Vertical Backup](https://www.verticalbackup.com) that can back up virtual machine files on ESXi to local drives, network or cloud storages.

## Features

Duplicacy currently supports major cloud storage providers (Amazon S3, Google Cloud Storage, Microsoft Azure, Dropbox, Backblaze, Google Drive, Microsoft OneDrive, and Hubic) and offers all essential features of a modern backup tool:

* Incremental backup: only back up what has been changed
* Full snapshot : although each backup is incremental, it must behave like a full snapshot for easy restore and deletion
* Deduplication: identical files must be stored as one copy (file-level deduplication), and identical parts from different files must be stored as one copy (block-level deduplication)
* Encryption: encrypt not only file contents but also file paths, sizes, times, etc.
* Deletion: every backup can be deleted independently without affecting others
* Concurrent access: multiple clients can back up to the same storage at the same time
* Snapshot migration: all or selected snapshots can be migrated from one storage to another

The key idea of **Lock-Free Deduplication** can be summarized as follows:

* Use variable-size chunking algorithm to split files into chunks
* Store each chunk in the storage using a file name derived from its hash, and rely on the file system API to manage chunks without using a centralized indexing database
* Apply a *two-step fossil collection* algorithm to remove chunks that become unreferenced after a backup is deleted

The [design document](https://github.com/gilbertchen/duplicacy-cli/blob/master/DESIGN.md) explains lock-free deduplication in detail.

## Getting Started

<details>
<summary>Installation</summary>

Duplicacy is written in Go.  You can run the following command to build the executable (which will be created under `$GOPATH/bin`):

```
go get -u github.com/gilbertchen/duplicacy/...
```

You can also visit the [releases page](https://github.com/gilbertchen/duplicacy-cli/releases/latest) to download the pre-built binary suitable for your platform..

</details>

<details>
<summary>Commands</summary>

Once you have the Duplicacy executable on your path, you can change to the directory that you want to back up (called *repository*) and run the *init* command:

```
$ cd path/to/your/repository
$ duplicacy init mywork sftp://user@192.168.1.100/path/to/storage
```

This *init* command connects the repository with the remote storage at 192.168.1.00 via SFTP.  It will initialize the remote storage if this has not been done before.  It also assigns the snapshot id *mywork* to the repository.  This snapshot id is used to uniquely identify this repository if there are other repositories that also back up to the same storage.

You can now create snapshots of the repository by invoking the *backup* command.  The first snapshot may take a while depending on the size of the repository and the upload bandwidth.  Subsequent snapshots will be much faster, as only new or modified files will be uploaded.  Each snapshot is identified by the snapshot id and an increasing revision number starting from 1.

```sh
$ duplicacy backup -stats
```

The *restore* command rolls back the repository to a previous revision:
```sh
$ duplicacy restore -r 1
```



Duplicacy provides a set of commands, such as list, check, diff, cat history, to manage snapshots:


```makefile
$ duplicacy list            # List all snapshots
$ duplicacy check           # Check integrity of snapshots
$ duplicacy diff            # Compare two snapshots, or the same file in two snapshots
$ duplicacy cat             # Print a file in a snapshot
$ duplicacy history         # Show how a file changes over time
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

</details>


The [User Guide](https://github.com/gilbertchen/duplicacy-cli/blob/master/GUIDE.md) contains a complete reference to
all commands and other features of Duplicacy.


## Storages

Duplicacy currently supports local file storage, SFTP, and many cloud storage providers.

<details> <summary>Local disk</summary>

```
Storage URL:  /path/to/storage (on Linux or Mac OS X)
              C:\path\to\storage (on Windows)
```
</details>

<details> <summary>SFTP</summary> 

```
Storage URL:  sftp://username@server/path/to/storage
```

Login methods include password authentication and public key authentication.  Due to a limitation of the underlying Go SSH library, the key pair for public key authentication must be generated without a passphrase.  To work with a key that has a passphrase, you can set up SSH agent forwarding which is also supported by Duplicacy.

</details>

<details> <summary>Dropbox</summary>

```
Storage URL:  dropbox://path/to/storage
```

For Duplicacy to access your Dropbox storage, you must provide an access token that can be obtained in one of two ways:

* Create your own app on the [Dropbox Developer](https://www.dropbox.com/developers) page, and then generate the [access token](https://blogs.dropbox.com/developers/2014/05/generate-an-access-token-for-your-own-account/)

* Or authorize Duplicacy to access its app folder inside your Dropbox (following [this link](https://dl.dropboxusercontent.com/u/95866350/start_dropbox_token.html)), and Dropbox will generate the access token (which is not visible to us, as the redirect page showing the token is merely a static html hosted by Dropbox)

Dropbox has two advantages over other cloud providers.  First, if you are already a paid user then to use the unused space as the backup storage is basically free.  Second, unlike other providers Dropbox does not charge bandwidth or API usage fees.

</details>

<details> <summary>Amazon S3</summary>

```
Storage URL:  s3://amazon.com/bucket/path/to/storage (default region is us-east-1)
              s3://region@amazon.com/bucket/path/to/storage (other regions must be specified)
```

You'll need to input an access key and a secret key to access your Amazon S3 storage.

Minio-based S3 compatiable storages are also supported by using the `minio` or `minios` backends:
```
Storage URL:  minio://region@host/bucket/path/to/storage (without TLS)
Storage URL:  minios://region@host/bucket/path/to/storage (with TLS)
```

There is another backend that works with S3 compatible storage providers that require V2 signing:
```
Storage URL:  s3c://region@host/bucket/path/to/storage
```

</details>

<details>  <summary>Google Cloud Storage</summary>
```
Storage URL: s3://us-east-1@s3.wasabisys.com/bucket/path/to/storage
```

[Wasabi](https://wasabi.com) is a relatively new cloud storage serivce providing a S3-compatible API.
It is well suited for storing backups, because it is much cheaper than Amazon S3 with a storage cost of $.0039/GB/Month and a download fee of $0.04/GB, and no additional charges on API calls.

</details>

<details>  <summary>Google Cloud Storage</summary>

```
Storage URL:  gcs://bucket/path/to/storage
```

Starting from version 2.0.0, a new Google Cloud Storage backend is added which is implemented using the [official Google client library](https://godoc.org/cloud.google.com/go/storage).  You must first obtain a credential file by [authorizing](https://duplicacy.com/gcp_start) Duplicacy to access your Google Cloud Storage account or by [downloading](https://console.cloud.google.com/projectselector/iam-admin/serviceaccounts) a service account credential file.
 
You can also use the s3 protocol to access Google Cloud Storage.  To do this, you must enable the [s3 interoperability](https://cloud.google.com/storage/docs/migrating#migration-simple) in your Google Cloud Storage settings and set the storage url as `s3://storage.googleapis.com/bucket/path/to/storage`.

</details>

<details> <summary>Microsoft Azure</summary>

```
Storage URL:  azure://account/container
```

You'll need to input the access key once prompted.

</details>

<details> <summary>Backblaze B2</summary>

```
Storage URL: b2://bucket
```

You'll need to input the account id and application key.

Backblaze's B2 storage is not only the least expensive (at 0.5 cent per GB per month), but also the fastest.  We have been working closely with their developers to leverage the full potentials provided by the B2 API in order to maximize the transfer speed.

</details>

<details> <summary>Google Drive</summary>

```
Storage URL: gcd://path/to/storage
```

To use Google Drive as the storage,  you first need to download a token file from https://duplicacy.com/gcd_start by
authorizing Duplicacy to access your Google Drive, and then enter the path to this token file to Duplicacy when prompted.

</details>

<details> <summary>Microsoft OneDrive</summary>

```
Storage URL: one://path/to/storage
```

To use Microsoft OneDrive as the storage,  you first need to download a token file from https://duplicacy.com/one_start by
authorizing Duplicacy to access your OneDrive, and then enter the path to this token file to Duplicacy when prompted.

</details>

<details> <summary>Hubic</summary>

```
Storage URL: hubic://path/to/storage
```

To use Hubic as the storage,  you first need to download a token file from https://duplicacy.com/hubic_start by
authorizing Duplicacy to access your Hubic drive, and then enter the path to this token file to Duplicacy when prompted.

Hubic offers the most free space (25GB) of all major cloud providers and there is no bandwidth charge (same as Google Drive and OneDrive), so it may be worth a try.

</details>

## Feature Comparison with Other Backup Tools

[duplicity](http://duplicity.nongnu.org) works by applying the rsync algorithm (or more specific, the [librsync](https://github.com/librsync/librsync) library)
to find the differences from previous backups and only then uploading the differences.  It is the only existing backup tool with extensive cloud support -- the [long list](http://duplicity.nongnu.org/duplicity.1.html#sect7) of storage backends covers almost every cloud provider one can think of.  However, duplicity's biggest flaw lies in its incremental model -- a chain of dependent backups starts with a full backup followed by a number of incremental ones, and ends when another full backup is uploaded.  Deleting one backup will render useless all the subsequent backups on the same chain.  Periodic full backups are required, in order to make previous backups disposable.

[bup](https://github.com/bup/bup) also uses librsync to split files into chunks but save chunks in the git packfile format.  It doesn't support any cloud storage, or deletion of old backups.

[Obnam](http://obnam.org) got the incremental backup model right in the sense that every incremental backup is actually a full snapshot.  Although Obnam also splits files into chunks, it does not adopt either the rsync algorithm or the variable-size chunking algorithm.  As a result, deletions or insertions of a few bytes will foil the
[deduplication](http://obnam.org/faq/dedup).
Deletion of old backups is possible, but no cloud storages are supported.
Multiple clients can back up to the same storage, but only sequential access is granted by the [locking on-disk data structures](http://obnam.org/locking/).
It is unclear if the lack of cloud backends is due to difficulties in porting the locking data structures to cloud storage APIs.

[Attic](https://attic-backup.org) has been acclaimed by some as the [Holy Grail of backups](https://www.stavros.io/posts/holy-grail-backups).  It follows the same incremental backup model as Obnam, but embraces the variable-size chunk algorithm for better performance and better deduplication.  Deletions of old backup is also supported.  However, no cloud backends are implemented, as in Obnam.  Although concurrent backups from multiple clients to the same storage is in theory possible by the use of locking, it is 
[not recommended](http://librelist.com/browser//attic/2014/11/11/backing-up-multiple-servers-into-a-single-repository/#e96345aa5a3469a87786675d65da492b) by the developer due to chunk indices being kept in a local cache. 
Concurrent access is not only a convenience; it is a necessity for better deduplication.  For instance, if multiple machines with the same OS installed can back up their entire drives to the same storage, only one copy of the system files needs to be stored, greatly reducing the storage space regardless of the number of machines.  Attic still adopts the traditional approach of using a centralized indexing database to manage chunks, and relies heavily on caching to improve performance.  The presence of exclusive locking makes it hard to be adapted for cloud storage APIs and reduces the level of deduplication.

[restic](https://restic.github.io) is a more recent addition.  It is worth mentioning here because, like Duplicacy, it is written in Go.  It uses a format similar to the git packfile format.  Multiple clients backing up to the same storage are still guarded by 
[locks](https://github.com/restic/restic/blob/master/doc/Design.md#locks).  A prune operation will therefore completely block all other clients connected to the storage from doing their regular backups.  Moreover, since most cloud storage services do not provide a locking service, the best effort is to use some basic file operations to simulate a lock, but distributed locking is known to be a hard problem and it is unclear how reliable restic's lock implementation is.  A faulty implementation may cause a prune operation to accidentally delete data still in use, resulting in unrecoverable data loss.  This is the exact problem that we avoided by taking the lock-free approach.


The following table compares the feature lists of all these backup tools:


| Feature/Tool       | duplicity | bup | Obnam             | Attic           | restic            | **Duplicacy** | 
|:------------------:|:---------:|:---:|:-----------------:|:---------------:|:-----------------:|:-------------:|
| Incremental Backup | Yes       | Yes | Yes               | Yes             | Yes               | **Yes**       |
| Full Snapshot      | No        | Yes | Yes               | Yes             | Yes               | **Yes**       |
| Deduplication      | Weak      | Yes | Weak              | Yes             | Yes               | **Yes**       |
| Encryption         | Yes       | Yes | Yes               | Yes             | Yes               | **Yes**       |
| Deletion           | No        | No  | Yes               | Yes             | No                | **Yes**       |
| Concurrent Access  | No        | No  | Exclusive locking | Not recommended | Exclusive locking | **Lock-free** |
| Cloud Support      | Extensive | No  | No                | No              | S3, B2, OpenStack | **S3, GCS, Azure, Dropbox, Backblaze B2, Google Drive, OneDrive, and Hubic**|
| Snapshot Migration | No        | No  | No                | No              | No                | **Yes**       |


## Performance Comparison with Other Backup Tools

Duplicacy is not only more feature-rich but also faster than other backup tools.  The following table lists the running times in seconds of backing up the [Linux code base](https://github.com/torvalds/linux) using Duplicacy and 3 other tools.  Clearly Duplicacy is the fastest by a significant margin.


|                    |   Duplicacy  |   restic   |   Attic    |  duplicity  | 
|:------------------:|:----------------:|:----------:|:----------:|:-----------:|
| Initial backup | 13.7 | 20.7 | 26.9 | 44.2 | 
| 2nd backup | 4.8  |  8.0 | 15.4 | 19.5 | 
| 3rd backup | 6.9  | 11.9 | 19.6 | 29.8 | 
| 4th backup | 3.3  | 7.0  | 13.7 | 18.6 | 
| 5th backup | 9.9  | 11.4 | 19.9 | 28.0 | 
| 6th backup | 3.8  | 8.0  | 16.8 | 22.0 | 
| 7th backup | 5.1  | 7.8  | 14.3 | 21.6 | 
| 8th backup | 9.5  | 13.5 | 18.3 | 35.0 | 
| 9th backup | 4.3  | 9.0  | 15.7 | 24.9 | 
| 10th backup | 7.9 | 20.2 | 32.2 | 35.0 | 
| 11th backup | 4.6 | 9.1  | 16.8 | 28.1 | 
| 12th backup | 7.4 | 12.0 | 21.7 | 37.4 | 


For more details and other speed comparison results, please visit https://github.com/gilbertchen/benchmarking.  There you can also find test scripts that you can use to run your own experiments.

## License

* Free for personal use or commercial trial
* Non-trial commercial use requires per-user licenses available from [duplicacy.com](https://duplicacy.com/customer) at a cost of $20 per year
* Modification and redistribution are permitted, but commercial use of derivative works is subject to the same requirements of this license
