# Duplicacy: A lock-free deduplication cloud backup tool

Duplicacy is a new generation cross-platform cloud backup tool based on the idea of [Lock-Free Deduplication](https://github.com/gilbertchen/duplicacy/wiki/Lock-Free-Deduplication).

Our paper explaining the inner workings of Duplicacy has been accepted by [IEEE Transactions on Cloud Computing](https://ieeexplore.ieee.org/document/9310668) and will appear in a future issue this year.  The final draft version is available [here](https://github.com/gilbertchen/duplicacy/blob/master/duplicacy_paper.pdf) for those who don't have IEEE subscriptions. 

This repository hosts source code, design documents, and binary releases of the command line version of Duplicacy.  There is also a Web GUI frontend built for Windows, macOS, and Linux, available from https://duplicacy.com.

There is a special edition of Duplicacy developed for VMware vSphere (ESXi) named [Vertical Backup](https://www.verticalbackup.com) that can back up virtual machine files on ESXi to local drives, network or cloud storages.

## Features

There are 3 core advantages of Duplicacy over any other open-source or commercial backup tools:

* Duplicacy is the *only* cloud backup tool that allows multiple computers to back up to the same cloud storage, taking advantage of cross-computer deduplication whenever possible, without direct communication among them.  This feature turns any cloud storage server supporting only a basic set of file operations into a sophisticated deduplication-aware server.  

* Unlike other chunk-based backup tools where chunks are grouped into pack files and a chunk database is used to track which chunks are stored inside each pack file, Duplicacy takes a database-less approach where every chunk is saved independently using its hash as the file name to facilitate quick lookups.  The avoidance of a centralized chunk database not only produces a simpler and less error-prone implementation, but also makes it easier to develop advanced features, such as [Asymmetric Encryption](https://github.com/gilbertchen/duplicacy/wiki/RSA-encryption) for stronger encryption and [Erasure Coding](https://github.com/gilbertchen/duplicacy/wiki/Erasure-coding) for resilient data protection.

* Duplicacy is fast.  While the performance wasn't the top-priority design goal, Duplicacy has been shown to outperform other backup tools by a considerable margin, as indicated by the following results obtained from a [benchmarking experiment](https://github.com/gilbertchen/benchmarking) backing up the [Linux code base](https://github.com/torvalds/linux) using Duplicacy and 3 other open-source backup tools.

[![Comparison of Duplicacy, restic, Attic, duplicity](https://github.com/gilbertchen/duplicacy/blob/master/images/duplicacy_benchmark_speed.png "Comparison of Duplicacy, restic, Attic, duplicity")](https://github.com/gilbertchen/benchmarking)


## Getting Started

* [A brief introduction](https://github.com/gilbertchen/duplicacy/wiki/Quick-Start)
* [Command references](https://github.com/gilbertchen/duplicacy/wiki)
* [Building from source](https://github.com/gilbertchen/duplicacy/wiki/Installation)

## Storages

Duplicacy currently provides the following storage backends:

* Local disk
* SFTP
* Dropbox
* Amazon S3
* Wasabi
* DigitalOcean Spaces
* Google Cloud Storage
* Microsoft Azure
* Backblaze B2
* Google Drive
* Microsoft OneDrive
* Hubic
* OpenStack Swift
* WebDAV (under beta testing)
* pcloud (via WebDAV)
* Box.com (via WebDAV)
* File Fabric by [Storage Made Easy](https://storagemadeeasy.com/)

Please consult the [wiki page](https://github.com/gilbertchen/duplicacy/wiki/Storage-Backends) on how to set up Duplicacy to work with each cloud storage.

For reference, the following chart shows the running times (in seconds) of backing up the [Linux code base](https://github.com/torvalds/linux) to each of those supported storages:


[![Comparison of Cloud Storages](https://github.com/gilbertchen/duplicacy/blob/master/images/duplicacy_benchmark_cloud.png "Comparison of Cloud Storages")](https://github.com/gilbertchen/cloud-storage-comparison)


For complete benchmark results please visit https://github.com/gilbertchen/cloud-storage-comparison.

## Comparison with Other Backup Tools

[duplicity](http://duplicity.nongnu.org) works by applying the rsync algorithm (or more specific, the [librsync](https://github.com/librsync/librsync) library)
to find the differences from previous backups and only then uploading the differences.  It is the only existing backup tool with extensive cloud support -- the [long list](http://duplicity.nongnu.org/duplicity.1.html#sect7) of storage backends covers almost every cloud provider one can think of.  However, duplicity's biggest flaw lies in its incremental model -- a chain of dependent backups starts with a full backup followed by a number of incremental ones, and ends when another full backup is uploaded.  Deleting one backup will render useless all the subsequent backups on the same chain.  Periodic full backups are required, in order to make previous backups disposable.

[bup](https://github.com/bup/bup) also uses librsync to split files into chunks but save chunks in the git packfile format.  It doesn't support any cloud storage, or deletion of old backups.

[Duplicati](https://duplicati.com) is one of the first backup tools that adopt the chunk-based approach to split files into chunks which are then uploaded to the storage.  The chunk-based approach got the incremental backup model right in the sense that every incremental backup is actually a full snapshot.  As Duplicati splits files into fixed-size chunks,  deletions or insertions of a few bytes will foil the deduplication.  Cloud support is extensive, but multiple clients can't back up to the same storage location.

[Attic](https://attic-backup.org) has been acclaimed by some as the [Holy Grail of backups](https://www.stavros.io/posts/holy-grail-backups).  It follows the same incremental backup model like Duplicati but embraces the variable-size chunk algorithm for better performance and higher deduplication efficiency (not susceptible to byte insertion and deletion any more).  Deletions of old backup are also supported.  However, no cloud backends are implemented.  Although concurrent backups from multiple clients to the same storage is in theory possible by the use of locking, it is
[not recommended](http://librelist.com/browser//attic/2014/11/11/backing-up-multiple-servers-into-a-single-repository/#e96345aa5a3469a87786675d65da492b) by the developer due to chunk indices being kept in a local cache.
Concurrent access is not only a convenience; it is a necessity for better deduplication.  For instance, if multiple machines with the same OS installed can back up their entire drives to the same storage, only one copy of the system files needs to be stored, greatly reducing the storage space regardless of the number of machines.  Attic still adopts the traditional approach of using a centralized indexing database to manage chunks and relies heavily on caching to improve performance.  The presence of exclusive locking makes it hard to be extended to cloud storages.

[restic](https://restic.github.io) is a more recent addition. It uses a format similar to the git packfile format.  Multiple clients backing up to the same storage are still guarded by
[locks](https://github.com/restic/restic/blob/master/doc/Design.md#locks), and because a chunk database is used, deduplication isn't real-time (different clients sharing the same files will upload different copies of the same chunks).  A prune operation will completely block all other clients connected to the storage from doing their regular backups.  Moreover, since most cloud storage services do not provide a locking service, the best effort is to use some basic file operations to simulate a lock, but distributed locking is known to be a hard problem and it is unclear how reliable restic's lock implementation is.  A faulty implementation may cause a prune operation to accidentally delete data still in use, resulting in unrecoverable data loss.  This is the exact problem that we avoided by taking the lock-free approach.


The following table compares the feature lists of all these backup tools:


| Feature/Tool       | Bacula / Bareos | duplicity | bup | Duplicati         | Attic           | restic            | **Duplicacy** |
|:------------------:|:---------------:|:---------:|:---:|:-----------------:|:---------------:|:-----------------:|:-------------:|
| Incremental Backup | Limited*        | Yes       | Yes | Yes               | Yes             | Yes               | **Yes**       |
| Full Snapshot      | Yes             | No        | Yes | Yes               | Yes             | Yes               | **Yes**       |
| Compression        | Yes             | Yes       | Yes | Yes               | Yes             | No                | **Yes**       |
| Deduplication      | No              | Weak      | Yes | Weak              | Yes             | Yes               | **Yes**       |
| Encryption         | Yes             | Yes       | Yes | Yes               | Yes             | Yes               | **Yes**       |
| Deletion           | Yes             | No        | No  | Yes               | Yes             | No                | **Yes**       |
| Concurrent Access  | Yes             | No        | No  | No                | Not recommended | Exclusive locking | **Lock-free** |
| Cloud Support      | Limited         | Extensive | No  | Extensive         | No              | Limited           | **Extensive** |
| Snapshot Migration | No              | No        | No  | No                | No              | No                | **Yes**       |

* Bacula can only store files as a whole new version, without any delta to save disk space

## License

* Free for personal use or commercial trial
* Non-trial commercial use requires per-computer CLI licenses available from [duplicacy.com](https://duplicacy.com/buy.html) at a cost of $50 per year
* The computer with a valid commercial license for the GUI version may run the CLI version without a CLI license
* CLI licenses are not required to restore or manage backups; only the backup command requires valid CLI licenses
* Modification and redistribution are permitted, but commercial use of derivative works is subject to the same requirements of this license
