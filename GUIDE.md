# Duplicacy User Guide

## Commands

#### Init

```
   duplicacy init - Initialize the current working directory as the repository to be backed up

USAGE:
   duplicacy init [command options] <snapshot id> <storage url>

OPTIONS:
   -encrypt, -e 			Encrypt the storage with a password
   -chunk-size, -c 4M 			the average size of chunks
   -max-chunk-size, -max 16M 		the maximum size of chunks (defaults to chunk-size * 4)
   -min-chunk-size, -min 1M 		the minimum size of chunks (defaults to chunk-size / 4)
   -compression-level, -l <level>	compression level (defaults to -1)
```

The *init* command first connects to the storage specified by the storage URL.  If the storage has been already been
initailized before, it will download the storage configuration (stored in the file named *config*) and ignore the options provided in the command line.  Otherwise, it will create the configuration file from the options and upload the file.

After that, it will prepare the the current working directory as the repositor.  Under the hood, it will create a directory
named *.duplicacy* in the repository and put a file named *preferences* that stores encryption and storage options.



#### Backup

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


