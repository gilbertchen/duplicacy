#!/bin/bash


. ./test_functions.sh

fixture
init_repo_pref_dir

backup
add_file file3
backup
add_file file4
chmod u-r ${TEST_REPO}/file4
backup
add_file file5
restore
check

