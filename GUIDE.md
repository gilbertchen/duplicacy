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
initialized before, it will download the storage configuration (stored in the file named *config*) and ignore the options provided in the command line.  Otherwise, it will create the configuration file from the options and upload the file.

The initialized storage will then become the default storage for other commands if the -storage option is not specified
for those commands.  This default storage actually has a name, *default*.

After that, it will prepare the the current working directory as the repository to be backed up.  Under the hood, it will create a directory
named *.duplicacy* in the repository and put a file named *preferences* that stores the snapshot id and encryption and storage options.

The snapshot id is an id used to distinguish different repositories connected to the same storage.  Each repository must have a unique snapshot id.

The -e option controls whether or not encryption will be enabled for the storage.  If encryption is enabled, you will be prompted to enter a storage password.

The three chunk size parameters are passed to the variable-size chunking algorithm.  Their values are important to the overall performance, especially for cloud storages.  If the chunk size is too small, a lot of overhead will be in sending requests and receiving responses.  If the chunk size is too large, the effect of deduplication will be less obvious as more data will need to be transferred with each chunk.

The compression level parameter is passed to the zlib library.  Valid values are -1 through 9, with 0 meaning no compression, 9 best compression (slowest), and -1 being the default value (equivalent to level 6).

Once a storage has been initialized with these parameters, these parameters cannot be modified any more.

#### Backup

```
SYNOPSIS:
   duplicacy backup - Save a snapshot of the repository to the storage

USAGE:
   duplicacy backup [command options]

OPTIONS:
   -hash                      detect file differences by hash (rather than size and timestamp)
   -t <tag>                   assign a tag to the backup
   -stats                     show statistics during and after backup
   -vss                       enable the Volume Shadow Copy service (Windows only)
   -storage <storage name>    backup to the specified storage instead of the default one
```

The *backup* command creates a snapshot of the repository and uploads it to the storage.  If -hash is not provided,
it will upload new or modified files since last backup by comparing file sizes and timestamps.
Otherwise, every file is scanned to detect changes.

You can assign a tag to the snapshot so that later you can refer to it by tag in other commands.

If the -stats option is specified, statistical information such as transfer speed, number of chunks will be displayed
throughout the backup procedure.

The -vss option works on Windows only to turn on the Volume Shadow Copy service such that files opened by other
processes with exclusive locks can be read as usual.

When the repository can have multiple storages (added by the *add* command), you can select the storage to back up to
by giving a storage name.

You can specify patterns to include/exclude files by putting them in a file named *.duplicacy/filters*.  Please refer to the [Include/Exclude Patterns](https://github.com/gilbertchen/duplicacy-beta/blob/master/GUIDE.md#includeexclude-patterns) section for how to specify the patterns.

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
files that have the same sizes and timestamps as those in the snapshot as unchanged files, but with the -hash option, every file will be fully scanned to make sure they are in fact unchanged.

By default the restore procedure will not overwriting existing files, unless the -overwrite option is specified.

The -delete option indicates that files not in the snapshot will be removed.

If the -stats option is specified, statistical information such as transfer speed, number of chunks will be displayed
throughout the restore procedure.

When the repository can have multiple storages (added by the *add* command), you can select the storage to restore from by specifying the storage name.

Unlike the *backup* procedure that reading the include/exclude patterns from a file, the *restore* procedure reads them
from the command line.  If the patterns can cause confusion to the command line argument parser, -- should be prepended to
the patterns.  Please refer to the [Include/Exclude Patterns](https://github.com/gilbertchen/duplicacy-beta/blob/master/GUIDE.md#includeexclude-patterns) section for how to specify patterns.


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
   -t <tag>                    list snapshots with the specified tag
   -files                      print the file list in each snapshot
   -chunks                     print chunks in each snapshot or all chunks if no snapshot specified
   -reset-password             take passwords from input rather than keychain/keyring or env
   -storage <storage name>     retrieve snapshots from the specified storage
```

The *list* command lists information about specified snapshots.  By default it will list snapshots created from the
current repository, but you can list all snapshots stored in the storage by specifying the -all option, or list snapshots
with a different snapshot id using the -id option, and/or snapshots with a particular tag with the -t option.

The revision number is a number assigned to the snapshot when it is being created.  This number will keep increasing
every time a new snapshot is created from a repository.  You can refer to snapshots by their revision numbers using 
the -r option, which either takes a single revision number (-r 123) or a range (-r 123-456).
There can be multiple -r options.

If -files is specified, for each snapshot to be listed, this command will also print information about every file
contained in the snapshot.

If -chunks is specified, the command will also print out every chunk the snapshot references.

The -reset-password option is used to reset stored passwords and to allow passwords to be entered again.  Please refer to the [Managing Passwords](https://github.com/gilbertchen/duplicacy-beta/blob/master/GUIDE.md#managing-passwords) section for more information.

When the repository can have multiple storages (added by the *add* command), you can specify the storage to list
by specifying the storage name.

#### Check
```
SYNOPSIS:
   duplicacy check - Check the integrity of snapshots

USAGE:
   duplicacy check [command options]

OPTIONS:
   -all, -a                 check snapshots with any id
   -id <snapshot id>        check snapshots with the specified id rather than the default one
   -r <revision> [+]        the revision number of the snapshot
   -t <tag>                 check snapshots with the specified tag
   -fossils                 search fossils if a chunk can't be found
   -resurrect               turn referenced fossils back into chunks
   -files                   verify the integrity of every file
   -storage <storage name>  retrieve snapshots from the specified storage```
```
The *check* command checks, for each specified snapshot, that all referenced chunks exist in the storage.

By default the *check* command will check snapshots created from the
current repository, but you can check all snapshots stored in the storage at once by specifying the -all option, or
snapshots from a different repository using the -id option, and/or snapshots with a particular tag with the -t option.

The revision number is a number assigned to the snapshot when it is being created.  This number will keep increasing
every time a new snapshot is created from a repository.  You can refer to snapshots by their revision numbers using 
the -r option, which either takes a single revision number (-r 123) or a range (-r 123-456).
There can be multiple -r options.

By default the *check* command only verifies the existence of chunks.  To verify the full integrity of a snapshot,
you should specify the -files option, which will download chunks and compute file hashes in memory, to
make sure that all hashes match.

By default the *check* command does not find fossils. If the -fossils option is specified, it will find
the fossil if the referenced chunk does not exist.  if the -resurrect option is specified, it will turn the fossil back into a chunk.

When the repository can have multiple storages (added by the *add* command), you can specify the storage to check
by specifying the storage name.


#### Cat
```
SYNOPSIS:
   duplicacy cat - Print to stdout the specified file, or the snapshot content if no file is specified

USAGE:
   duplicacy cat [command options] [<file>]

OPTIONS:
   -id <snapshot id>        retrieve from the snapshot with the specified id
   -r <revision>            the revision number of the snapshot
   -storage <storage name>  retrieve the file from the specified storage
```

The *cat* command prints a file or the entire snapshot content if no file is specified.

The file must be specified with a path relative to the repository.

You can specify a different snapshot id rather than the default id.

The -r option is optional.  If not specified, the latest revision will be selected.

You can use the -storage option to select a different storage other than the default one.

#### Diff
```
SYNOPSIS:
   duplicacy diff - Compare two snapshots or two revisions of a file

USAGE:
   duplicacy diff [command options] [<file>]

OPTIONS:
   -id <snapshot id>        diff with the snapshot with the specified id
   -r <revision> [+]        the revision number of the snapshot
   -hash                    compute the hashes of on-disk files
   -storage <storage name>  retrieve files from the specified storage
```
The *diff* command compares the same file in two different snapshots if a file is given, otherwise compares the
two snapshots.

The file must be specified with a path relative to the repository.

You can specify a different snapshot id rather than the default snapshot id.

If only one revision is given by -r, the right hand side of the comparison will be the on-disk file.
The -hash option can then instruct this command to compute the hash of the file. 

You can use the -storage option to select a different storage other than the default one.

#### History
```
SYNOPSIS:
   duplicacy history - Show the history of a file

USAGE:
   duplicacy history [command options] <file>

OPTIONS:
   -id <snapshot id>        find the file in the snapshot with the specified id
   -r <revision> [+]        show history of the specified revisions
   -hash                    show the hash of the on-disk file
   -storage <storage name>  retrieve files from the specified storage
```

The *history* command shows how the hash, size, and timestamp of a file change over the specified set of revisions.

You can specify a different snapshot id rather than the default snapshot id, and multiple -r options to specify the
set of revisions.

The -hash option is to compute the hash of the on-disk file.  Otherwise, only the size and timestamp of the on-disk
file will be included.

You can use the -storage option to select a different storage other than the default one.

#### Prune
```
SYNOPSIS:
   duplicacy prune - Prune snapshots by revision, tag, or retention policy

USAGE:
   duplicacy prune [command options]

OPTIONS:
   -id <snapshot id>        delete snapshots with the specified id instead of the default one
   -all, -a                 match against all snapshot IDs
   -r <revision> [+]        delete snapshots with the specified revisions
   -t <tag> [+]             delete snapshots with the specified tags
   -keep <n:m> [+]          keep 1 snapshot every n days for snapshots older than m days
   -exhaustive              find all unreferenced chunks by scanning the storage
   -exclusive               assume exclusive access to the storage (disable two-step fossil collection)
   -dry-run, -d             show what would have been deleted
   -delete-only             delete fossils previously collected (if deletable) and don't collect fossils
   -collect-only            identify and collect fossils, but don't delete fossils previously collected
   -ignore <id> [+]         ignore the specified snapshot id when deciding if fossils can be deleted
   -storage <storage name>  prune snapshots from the specified storage
```

The *prune* command implements the two-step fossil collection algorithm.  It will first find fossil collection files
from previous runs and check if contained fossils are eligible for permanent deletion (the fossil deletion step).  Then it
will search for snapshots to be deleted, mark unreferenced chunks as fossils (by renaming) and save them in a new fossil
collection file stored locally (the fossil collection step).

If a snapshot id is specified, that snapshot id will be used instead of the default one.  The -a option will find
snapshots with any id.  Snapshots to be deleted can be specified by revision numbers, by a tag, by retention policies,
or by any combination of them.

The retention policies are specified by the -keep option, which accepts an argument in the form of two numbers *n:m*, where *n* indicates the number of days between two consecutive snapshots to keep, and *m* means that the policy only applies to snapshots at least *m* day old.  If *n* is zero, any snapshots older than *m* days will be removed.

Here are a few sample retention policies:

```sh
$ duplicacy prune -keep 1:7       # Keep 1 snapshot per day for snapshots older than 7 days
$ duplicacy prune -keep 7:30      # Keep 1 snapshot every 7 days for snapshots older than 30 days
$ duplicacy prune -keep 30:180    # Keep 1 snapshot every 30 days for snapshots older than 180 days
$ duplicacy prune -keep 0:360     # Keep no snapshots older than 360 days
```

Multiple -keep options must be sorted by their *m* values in decreasing order.  For instance, to combine the above policies into one line, it would become:

```sh
$ duplicacy prune -keep 0:360 -keep 30:180 -keep 7:30 -keep 1:7
```

The -exhaustive option will scan the list of all chunks in the storage, therefore it will find not only 
unreferenced chunks from deleted snapshots, but also chunks that become unreferenced for other reasons, such as
those from an incomplete backup.  It will also find any file that does not look like a chunk file.
In contrast, a default *prune* command will only identify 
chunks referenced by deleted snapshots but not any other snapshots.

The -exclusive option will assume that no other clients are accessing the storage, effectively disabling the 
*two-step fossil collection* algorithm.  With this option, the *prune* command will immediately remove unreferenced chunks.

The -dryrun option is used to test what changes the *prune* command would have done.  It is guaranteed not to make
any changes on the storage, not even creating the local fossil collection file.  The following command checks if the
chunk directory is clean (i.e., if there are any unreferenced chunks, temporary files, or anything else):

```
$ duplicacy prune -d -exclusive -exhaustive    #  Prints out nothing if the chunk directory is clean
```

The -delete-only option will skip the fossil collection step, while the -collect-only option will skip the fossil deletion step.

For fossils collected in the fossil collection step to be eligible for safe deletion in the fossil deletion step, at least
one new snapshot from *each* snapshot id must be created between two runs of the *prune* command.  However, some repository
may not be set up to back up with a regular schedule, and thus literally blocking other repositories from deleting any fossils.  Duplicacy by default will ignore repositories that have no new backup in the past 7 days.  It also provide an
-ignore option that can be used to skip certain repositories when deciding the deletion criteria.

You can use the -storage option to select a different storage other than the default one.


#### Password
```
SYNOPSIS:
   duplicacy password - Change the storage password

USAGE:
   duplicacy password [command options]

OPTIONS:
   -storage <storage name>  change the password used to access the specified storage
```

The *password* command decrypts the storage configuration file *config* using the old password, and re-encrypts the file
using a new password.  It does not change all the encryption keys used to encrypt and decrypt chunk files,
snapshot files, etc.

You can specify the storage to change the password for when working with multiple storages.


#### Add
```
SYNOPSIS:
   duplicacy add - Add an additional storage to be used for the existing repository

USAGE:
   duplicacy add [command options] <storage name> <snapshot id> <storage url>

OPTIONS:
   -encrypt, -e                    Encrypt the storage with a password
   -chunk-size, -c 4M              the average size of chunks
   -max-chunk-size, -max 16M       the maximum size of chunks (defaults to chunk-size * 4)
   -min-chunk-size, -min 1M        the minimum size of chunks (defaults to chunk-size / 4)
   -compression-level, -l <level>  compression level (defaults to -1)
   -copy <storage name>            make the new storage copy-compatible with an existing one
```

The *add* command connects another storage to the current repository.  Like the *init* command, if the storage has not
been initialized before, a storage configuration file derived from the command line options will be uploaded, but those
options will be ignored if the configuration file already exists in the storage.

A unique storage name must be given in order to distinguish it from other storages.

The -copy option is required if later you want to copy snapshots between this storage and another storage.
Two storages are copy-compatible if they have the same average chunk size, the same maximum chunk size,
the same minimum chunk size, the same chunk seed (used in calculating the rolling hash in the variable-size chunks
algorithm), and the same hash key.  If the -copy option is specified, these parameters will be copied from
the existing storage rather than from the command line.

#### Set
```
SYNOPSIS:
   duplicacy set - Change the options for the default or specified storage

USAGE:
   duplicacy set [command options]

OPTIONS:
   -encrypt, e[=true]       encrypt the storage with a password
   -no-backup[=true]        backup to this storage is prohibited
   -no-restore[=true]       restore from this storage is prohibited
   -no-save-password[=true] don't save password or access keys to keychain/keyring
   -key                     add a key/password whose value is supplied by the -value option
   -value  			          the value of the key/password
   -storage <storage name>  use the specified storage instead of the default one
```

The *set* command changes the options for the specified storage.

The -e option turns on the storage encryption.  If specified as -e=false, it turns off the storage encryption.

The -no-backup option will not allow backups from this repository to be created.

The -no-restore option will not allow restoring this repository to a different revision.

The -no-save-password option will require every password or token to be entered every time and not saved anywhere.

The -key and -value options are used to store (in plain text) access keys or tokens need by various storages.  Please
refer to the [Managing Passwords](https://github.com/gilbertchen/duplicacy-beta/blob/master/GUIDE.md#managing-passwords) section for more details.

You can select a storage to change options for by specifying a storage name.


#### Copy
```
SYNOPSIS:
   duplicacy copy - Copy snapshots between compatible storages

USAGE:
   duplicacy copy [command options]

OPTIONS:
   -id <snapshot id>     copy snapshots with the specified id instead of all snapshot ids
   -r <revision> [+]     copy snapshots with the specified revisions
   -from <storage name>  copy snapshots from the specified storage
   -to <storage name>    copy snapshots to the specified storage
```

The *copy* command copies snapshots from one storage to another storage.  They must be copy-compatible, i.e., some
configuration parameters must be the same.  One storage must be initialized with the -copy option provided by the *add* command.

Instead of copying all snapshots, you can specify a set of snapshots to copy by giving the -r options.  The *copy* command
preserves the revision numbers, so if a revision number already exists on the destination storage the command will fail.

If no -from option is given, the snapshots from the default storage will be copied.  The -to option specified the
destination storage and is required.

## Include/Exclude Patterns

An include pattern starts with -, and an exclude pattern starts with +.  Patterns may contain wildcard characters such as * and ? with their normal meaning.

When matching a path against a list of patterns, the path is compared with the part after + or -, one pattern at a time.  Therefore, the order of the patterns is significant.  If a match with an include pattern is found, the path is said to be included without further comparisons.  If a match with an exclude pattern is found, the path is said to be excluded without further comparison.  If a match is not found, the path will be excluded if all patterns are include patterns, but included otherwise.

Note that the path in Duplicacy for a directory always ends with a /, even on Windows.  The path of a file does not end with a /.  This can be used to exclude directories only.

For the *backup* command, the include/exclude patterns are read from a file named *filters* under the *.duplicacy* directory.

For the *restore* command, the include/exclude patterns are specified as the command line arguments.

## Managing Passwords

Duplicacy will attempt to retrieve in three ways the storage password and the storage-specific access tokens/keys.

* If a secret vault service is available, Duplicacy will store the password input by the user in such a secret vault and later retrieve it when needed.  On Mac OS X it is Keychain, and on Linux it is gnome-keyring.  On Windows the password is encrypted and decrypted by the Data Protection API, and encrypted password is stored in the file *.duplicacy/keyring*.  However, if the -no-save-password option is specified for the storage, then Duplicacy will not save passwords this way
* If an environment variable for a password is provided, Duplicacy will always take it.  The table below shows the name of the environment variable for each kind of password.  Note that if the storage is not the default one, the storage name will be included in the name of the environment variable.
* If a matching key and its value are saved to the preference file (.duplicacy/preferences) by the *set* command, the value will be used as the password.  The last column in the table below lists the name of the preference key for each type of password.

| password type | environment variable (default storage) | environment variable (non-default storage) | key in preferences  |
|:----------------:|:----------------:|:----------------:|:----------------:|
| storage password | DUPLICACY_PASSWORD | DUPLICACY_&lt;STORAGENAME&gt;_PASSWORD | password |
| sftp password | DUPLICACY_SSH_PASSWORD | DUPLICACY_&lt;STORAGENAME&gt;_SSH_PASSWORD | ssh_password |
| Dropbox Token | DUPLICACY_DROPBOX_TOKEN | DUPLICACY_&lt;STORAGENAME>&gt;_DROPBOX_TOEKN | dropbox_token |
| S3 Access ID | DUPLICACY_S3_ID | DUPLICACY_&lt;STORAGENAME&gt;_S3_ID | s3_id |
| S3 Secret Key | DUPLICACY_S3_KEY | DUPLICACY_&lt;STORAGENAME&gt;_S3_KEY | s3_key |
| BackBlaze Account ID | DUPLICACY_B2_ID | DUPLICACY_&lt;STORAGENAME&gt;_B2_ID | b2_id |
| Backblaze Application Key | DUPLICACY_B2_KEY | DUPLICACY_&lt;STORAGENAME&gt;_B2_KEY | b2_key |
| Azure Access Key | DUPLICACY_AZURE_KEY | DUPLICACY_&lt;STORAGENAME&gt;_AZURE_KEY | azure_key |

Note that the passwords stored in the environment variable and the preference need to be in plaintext and thus are insecure and should be avoided whenever possible.

## Scripts

You can instruct Duplicay to run a script before or after executing a command.  For example, if you create a bash script with the name *pre-prune* under the *.duplicacy/scripts* directory, this bash script will be run before the *prune* command starts.  A script named *post-prune* will be run after the *prune* command finishes.  This rule applies to all commands except *init*.
