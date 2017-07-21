#!/bin/bash

# Testing backup and restore of sparse files

. ./test_functions.sh

fixture

pushd ${TEST_REPO}
${DUPLICACY} init integration-tests $TEST_STORAGE -c 1m

for i in `seq 1 10`; do
    dd if=/dev/urandom of=file3 bs=1000 count=1000 seek=$((100000 * $i))
done

ls -lsh file3

${DUPLICACY} backup 
${DUPLICACY} check --files -stats

rm file1 file3

${DUPLICACY} restore -r 1
${DUPLICACY} -v restore -r 1 -overwrite -stats -hash

ls -lsh file3

popd
