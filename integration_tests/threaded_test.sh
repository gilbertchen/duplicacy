#!/bin/bash


. ./test_functions.sh

fixture

pushd ${TEST_REPO}
${DUPLICACY} init integration-tests $TEST_STORAGE -c 1k

add_file file3
add_file file4


${DUPLICACY} backup -threads 16 
${DUPLICACY} check --files -stats
popd
