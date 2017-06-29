#!/bin/bash

# Sanity test for the fixed-size chunking algorithm

. ./test_functions.sh

fixture

pushd ${TEST_REPO}
${DUPLICACY} init integration-tests $TEST_STORAGE -c 64 -max 64 -min 64 

add_file file3
add_file file4


${DUPLICACY} backup 
${DUPLICACY} check --files -stats
popd
