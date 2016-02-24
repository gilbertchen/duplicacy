# Duplicacy User Guide

## Commands

#### Init

```
SYNOPSIS:
   duplicacy init - Initialize the storage if necessary and the current directory as the repository

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

The *backup* command creates a snapshot of the repository and uploads it to the storage.  If -hash is not provided,
it will upload new or modified files since last backup by comparing file sizes and timestmpas.
Otherwise, every file is scanned to detect changes.

You can assign a tag to the snapshot so later you can refer to it by tag in other commands.

If the -stats option is specified, statistical information such as transfer speed, number of chunks will be displayed
throughout the backup procedure.

The -vss option works on Windows only to turn on the Volume Shadow Copy service such that files opened by other
processes with exclusive locks can be read as usual.

When the repository can have multiple storages (added by the *add* command), you can specifiy the storage to back up to
by the storage name.

You can specify patterns to include/exclude files by putthing them in a file named *.duplicacy/filters*.  Please refer to the Include/Exclude Patterns section for how to specify the patterns.

#### Restore
```
SYNOPSIS:
   duplicacy restore - Restore the repository to a previously saved snapshot

USAGE:
   duplicacy restore [command options] [--] [pattern] ...

OPTIONS:
   -r <revision>            the revision number of the snapshot (required)
   -hash                    detect file differences by hash (rather than size and timestamp)
   -overwrite               overwrite existing files in the repository
   -delete                  delete files not in the snapshot
   -stats                   show statistics during and after restore
   -storage <storage name>  restore from the specified storage instead of the default one
```

The *restore* command restores the repository to a previous revision.  By default the restore procedure will treat
files that have the same sizes and timestamps as those in the snapshot as unchanged files, but if -hash is specified, every file will be fully scanned to make sure they are in fact unchanged.

By default the restore procedure will not overwriting existing files, unless the -overwrite option is specified.

The -delete indicates that files not in the snapshot will be removed.

If the -stats option is specified, statistical information such as transfer speed, number of chunks will be displayed
throughout the restore procedure.

When the repository can have multiple storages (added by the *add* command), you can specifiy the storage to restore from
by the storage name.

Unlike the *backup* procedure that reading the include/exclude patterns from a file, the *restore* procedure reads them
from the command line.  If the patterns can cause confusion to the command line argument parse, -- should be prepended to
the patterns.  Please refer to the Include/Exclude Patterns section for how to specify patterns.


#### List
```
SYNOPSIS:
   duplicacy list - List snapshots

USAGE:
   duplicacy list [command options]  

OPTIONS:
   -all, -a                    list snapshots with any id
   -id <snapshot id>           list snapshots with the specified id rather than the default one
   -r <revision> [+]           the revision number of the snapshot
   -t <tag>                    list snaphots with the specified tag
   -files                      print the file list in each snapshot
   -chunks                     print chunks in each snapshot or all chunks if no snapshot specified
   -reset-password             take passwords from input rather than keychain/keyring or env
   -storage <storage name>     retrieve snapshots from the specified storage
```



#### Check
```
SYNOPSIS:
```

#### Cat
```
SYNOPSIS:
```

#### Diff
```
SYNOPSIS:
```

#### History
```
SYNOPSIS:
```

#### Prune
```
SYNOPSIS:
```

#### Password
```
SYNOPSIS:
```

#### Add
```
SYNOPSIS:
```

#### Set
```
SYNOPSIS:
```

#### Copy
```
SYNOPSIS:
```


