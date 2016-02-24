# Duplicacy User Guide

## Commands

#### Init

```
SYNOPSIS:
   duplicacy init - Initialize the storage if necessary and the current working directory as the repository

USAGE:
   duplicacy init [command options] <snapshot id> <storage url>

OPTIONS:
   -encrypt, -e                    encrypt the storage with a password
   -chunk-size, -c 4M              the average size of chunks
   -max-chunk-size, -max 16M       the maximum size of chunks (defaults to chunk-size * 4)
   -min-chunk-size, -min 1M        the minimum size of chunks (defaults to chunk-size / 4)
   -compression-level, -l <level>  compression level (defaults to -1)
```

The *init* command first connects to the storage specified by the storage URL.  If the storage has been already been
initailized before, it will download the storage configuration (stored in the file named *config*) and ignore the options provided in the command line.  Otherwise, it will create the configuration file from the options and upload the file.

After that, it will prepare the the current working directory as the repositor.  Under the hood, it will create a directory
named *.duplicacy* in the repository and put a file named *preferences* that stores the snapshot id and encryption and storage options.

The snapshot id is an id used to distinguish different repositories connected to the same storage.  It is required for each repository to have a unique snapshot id.

The -e option controls whether or not the encryption will be enabled for the storage.  If the encryption is enabled, you will be prompted to enter a password.

The chunk size parametes are passed to the variable-size chunking algorithm.  Their values are important to the overall performance, espeically for cloud storages.  If the chunk size is too small, a lot of overhead will be spent in sending requests and receiving responses.  If the chunk size is too large, the effect of deduplication will be less obvious as more data will need to be transferred with each chunk.

The compression level parameter is passed to the zlib library.  Valid values are -1 through 9, with 0 meaning no compression, 9 best compression (slowest), and -1 being the default value (equivalent to level 6).

Once a storage has been initialized with these parameters, there parameters cannot be modified any more.

#### Backup

```
SYNOPSIS:
   duplicacy backup - Save a snapshot of the repository to the storage

USAGE:
   duplicacy backup [command options]

OPTIONS:
   -hash                      detect file diferences by hash (rather than size and timestamp)
   -t <tag>                   assign a tag to the backup
   -stats                     show statistics during and after backup
   -vss                       enable the Volume Shadow Copy service (Windows only)
   -storage <storage name>    backup to the specified storage instead of the default one
```


#### Restore

#### List

#### Check

#### Cat

#### Diff

#### History

#### Prune

#### Password

#### Add

#### Set

#### Copy


