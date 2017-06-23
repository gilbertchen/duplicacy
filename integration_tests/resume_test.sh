#!/bin/bash


. ./test_functions.sh

fixture

pushd ${TEST_REPO}
${DUPLICACY} init integration-tests $TEST_STORAGE -c 4k

# Create 10 20k files
add_file file1 20000
add_file file2 20000
add_file file3 20000
add_file file4 20000
add_file file5 20000
add_file file6 20000
add_file file7 20000
add_file file8 20000
add_file file9 20000
add_file file10 20000

# Limit the rate to 10k/s so the backup will take about 10 seconds
${DUPLICACY} backup -limit-rate 10 -threads 4 &
# Kill the backup after 3 seconds
DUPLICACY_PID=$!
sleep 3
kill -2 ${DUPLICACY_PID}

# Try it again to test the multiple-resume case
${DUPLICACY} backup -limit-rate 10  -threads 4&
DUPLICACY_PID=$!
sleep 3
kill -2 ${DUPLICACY_PID}

# Fail the backup before uploading the snapshot
env DUPLICACY_FAIL_SNAPSHOT=true ${DUPLICACY} backup

# Now complete the backup
${DUPLICACY} backup
${DUPLICACY} check --files
popd
