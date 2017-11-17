# Duplicacy: A lock-free deduplication cloud backup tool

Duplicacy is a new generation cross-platform cloud backup tool based on the idea of [Lock-Free Deduplication](https://github.com/gilbertchen/duplicacy/wiki/Lock-Free-Deduplication).  It is the only cloud backup tool that allows multiple computers to back up to the same storage simultaneously without using any locks (thus readily amenable to various cloud storage services).

The repository hosts source code, design documents, and binary releases of the command line version.  There is also a Duplicacy GUI frontend built for Windows and Mac OS X available from https://duplicacy.com.

There is a special edition of Duplicacy developed for VMware vSphere (ESXi) named [Vertical Backup](https://www.verticalbackup.com) that can back up virtual machine files on ESXi to local drives, network or cloud storages.

## Features

Duplicacy currently supports major cloud storage providers (Amazon S3, Google Cloud Storage, Microsoft Azure, Dropbox, Backblaze B2, Google Drive, Microsoft OneDrive, and Hubic) and offers all essential features of a modern backup tool:

* Incremental backup: only back up what has been changed
* Full snapshot: although each backup is incremental, it must behave like a full snapshot for easy restore and deletion
* Deduplication: identical files must be stored as one copy (file-level deduplication), and identical parts from different files must be stored as one copy (block-level deduplication)
* Encryption: encrypt not only file contents but also file paths, sizes, times, etc.
* Deletion: every backup can be deleted independently without affecting others
* Concurrent access: multiple clients can back up to the same storage at the same time
* Snapshot migration: all or selected snapshots can be migrated from one storage to another

The key idea of **[Lock-Free Deduplication](https://github.com/gilbertchen/duplicacy/wiki/Lock-Free-Deduplication)** can be summarized as follows:

* Use variable-size chunking algorithm to split files into chunks
* Store each chunk in the storage using a file name derived from its hash, and rely on the file system API to manage chunks without using a centralized indexing database
* Apply a *two-step fossil collection* algorithm to remove chunks that become unreferenced after a backup is deleted

## Getting Started

* [A brief introduction](https://github.com/gilbertchen/duplicacy/wiki/Quick-Start)
* [Command references](https://github.com/gilbertchen/duplicacy/wiki)

## Storages

Duplicacy currently supports local file storage, SFTP, and many cloud storage providers:

* Local or networked drive
* SFTP
* Dropbox
* Amazon S3
* Wasabi
* Google Cloud Storage
* Microsoft Azure
* Backblaze B2
* Google Drive
* Microsoft OneDrive
* Hubic

Please consult the [wiki page](https://github.com/gilbertchen/duplicacy/wiki/Storage-Backends) on how to set up Duplicacy to work with each storage.
<details> <summary>Cost Comparison</summary>

| Type         |   Storage (monthly)    |   Upload           |    Download    |    API Charge   |
|:------------:|:-------------:|:------------------:|:--------------:|:-----------:|
| Amazon S3    | $0.023/GB | free | $0.09/GB | [yes](https://aws.amazon.com/s3/pricing/) |
| Wasabi       | $3.99 first 1TB <br> $0.0039/GB additional | free | $.04/GB | no |
| DigitalOcean Spaces| $5 first 250GB <br> $0.02/GB additional | free | first 1TB free <br> $0.01/GB additional| no |
| Backblaze B2 | $0.005/GB | free | $0.02/GB | [yes](https://www.backblaze.com/b2/b2-transactions-price.html) |
| Google Cloud Storage| $0.026/GB | free |$ 0.12/GB | [yes](https://cloud.google.com/storage/pricing) |
| Google Drive | 15GB free <br> $1.99/100GB <br> $9.99/TB | free | free | no |
| Microsoft Azure | $0.0184/GB | free | free | [yes](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/) |
| Microsoft OneDrive | 5GB free <br> $1.99/50GB <br> $5.83/TB | free | free | no |
| Dropbox | 2GB free <br> $8.25/TB | free | free | no |
| Hubic | 25GB free <br> €1/100GB <br> €5/10TB | free | free | no |

</details>

<details> <summary>Performance Comparison</summary>

A [performance comparison](https://github.com/gilbertchen/cloud-storage-comparison) of these storages measured the running times (in seconds) of backing up and restoring the [Linux code base](https://github.com/torvalds/linux) as follows:

| Storage              | initial backup | 2nd | 3rd | 4th | 5th | 6th | initial restore | 2nd | 3rd | 4th | 5th | 6th |
|:--------------------:|:------:|:----:|:-----:|:----:|:-----:|:----:|:-----:|:----:|:----:|:----:|:----:|:----:|
| SFTP                 |  31.5  | 6.6  | 20.6  | 4.3  | 27.0  | 7.4  | 22.5  | 7.8  | 18.4 | 3.6  | 18.7 | 8.7  | 
| Amazon S3            |  41.1  | 5.9  | 21.9  | 4.1  | 23.1  | 7.6  | 27.7  | 7.6  | 23.5 | 3.5  | 23.7 | 7.2  | 
| Wasabi               |  38.7  | 5.7  | 31.7  | 3.9  | 21.5  | 6.8  | 25.7  | 6.5  | 23.2 | 3.3  | 22.4 | 7.6  | 
| DigitalOcean Spaces  |  51.6  | 7.1  | 31.7  | 3.8  | 24.7  | 7.5  | 29.3  | 6.4  | 27.6 | 2.7  | 24.7 | 6.2  | 
| Backblaze B2         |  106.7 | 24.0 | 88.2  | 13.5 | 46.3  | 14.8 | 67.9  | 14.4 | 39.1 | 6.2  | 38.0 | 11.2 | 
| Google Cloud Storage |  76.9  | 11.9 | 33.1  | 6.7  | 32.1  | 12.7 | 39.5  | 9.9  | 26.2 | 4.8  | 25.5 | 10.4 | 
| Google Drive         |  139.3 | 14.7 | 45.2  | 9.8  | 60.5  | 19.8 | 129.4 | 17.8 | 54.4 | 8.4  | 67.3 | 17.4 | 
| Microsoft Azure      |  35.0  | 5.4  | 20.4  | 3.9  | 22.1  | 6.1  | 30.7  | 7.1  | 21.5 | 3.6  | 21.6 | 9.2  | 
| Microsoft OneDrive   |  250.0 | 31.6 | 80.2  | 16.9 | 82.7  | 36.4 | 333.4 | 26.2 | 82.0 | 12.9 | 71.1 | 24.4 |  
| Dropbox              |  267.2 | 35.8 | 113.7 | 19.5 | 109.0 | 38.3 | 164.0 | 31.6 | 80.3 | 14.3 | 73.4 | 22.9 | 

For more details please visit https://github.com/gilbertchen/cloud-storage-comparison.
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
* Non-trial commercial use requires per-user CLI licenses available from [duplicacy.com](https://duplicacy.com/buy) at a cost of $20 per year
* A user is defined as the computer account that creates or edits the files to be backed up; if a backup contains files created or edited by multiple users for commercial purposes, one CLI license is required for each user
* The computer with a valid commercial license for the GUI version may run the CLI version without a CLI license
* CLI licenses are not required to restore or manage backups; only the backup command requires valid CLI licenses
* Modification and redistribution are permitted, but commercial use of derivative works is subject to the same requirements of this license
