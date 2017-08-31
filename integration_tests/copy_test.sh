#!/bin/bash


. ./test_functions.sh

fixture

pushd ${TEST_REPO}
${DUPLICACY} init integration-tests $TEST_STORAGE -c 1k
${DUPLICACY} add -copy default secondary integration-tests $SECONDARY_STORAGE
add_file file1
add_file file2
${DUPLICACY} backup 
${DUPLICACY}  copy -from default -to secondary
add_file file3
add_file file4
${DUPLICACY} backup 
${DUPLICACY}  copy -from default -to secondary
${DUPLICACY} check --files -stats
${DUPLICACY} check --files -stats -storage secondary
popd
