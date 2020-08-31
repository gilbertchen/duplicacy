# Beta COLD Storage AKA Glacier support

## Objectif

 * Backup directly to Glacier
 * Restore directly from Glacier
 
## Usage

 * Add a new option "storageclass": "GLACIER" to your preference file
 * Use a compatible S3CStorage driver. Actually only tested on Scaleway's Object Storage and C14 Glacier
 * Data chunks will go directly to Glacier, snapshot chunks will stay in Standard storage. One can however move snapshot chunks to Glacier also
 
## Caveats

 * Corner cases not tested
 * When restore, duplicacy will try to move chunks from Glacier to Standard storage, *ONE BY ONE*, thus can be very slow. One can speed this up by moving all thunks to Standard at once
 * You cannot move the config file nor the snapshots folder to Glacier
  
