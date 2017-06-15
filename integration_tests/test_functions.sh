#!/bin/bash

get_abs_filename() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

pushd () {
    command pushd "$@" > /dev/null
}

popd () {
    command popd "$@" > /dev/null
}


# Functions used to create integration tests suite

DUPLICACY=$(get_abs_filename ../duplicacy_main)

# Base directory where test repositories will be created
TEST_ZONE=$HOME/DUPLICACY_TEST_ZONE
# Test Repository
TEST_REPO=$TEST_ZONE/TEST_REPO

# Storage for test ( For now, only local path storage is supported by test suite)
TEST_STORAGE=$TEST_ZONE/TEST_STORAGE

# Extra storage for copy operation
SECONDARY_STORAGE=$TEST_ZONE/SECONDARY_STORAGE

# Preference directory ( for testing the -pref-dir option)
DUPLICACY_PREF_DIR=$TEST_ZONE/TEST_DUPLICACY_PREF_DIR

# Scratch pad for testing restore
TEST_RESTORE_POINT=$TEST_ZONE/RESTORE_POINT

# Make sure $TEST_ZONE is in know state



function fixture()
{
  # clean TEST_RESTORE_POINT
  rm -rf $TEST_RESTORE_POINT
  mkdir -p $TEST_RESTORE_POINT

  # clean TEST_STORAGE
  rm -rf $TEST_STORAGE
  mkdir -p $TEST_STORAGE

  # clean SECONDARY_STORAGE
  rm -rf $SECONDARY_STORAGE
  mkdir -p $SECONDARY_STORAGE
  

  # clean TEST_DOT_DUPLICACY
  rm -rf $DUPLICACY_PREF_DIR
  mkdir -p $DUPLICACY_PREF_DIR

  # Create test repository
  rm -rf ${TEST_REPO}
  mkdir -p ${TEST_REPO}
  pushd ${TEST_REPO}
    echo "file1" > file1
    mkdir dir1
    echo "file2" > dir1/file2
  popd
}

function init_repo()
{
  pushd ${TEST_REPO}
  ${DUPLICACY} init integration-tests $TEST_STORAGE
  ${DUPLICACY} add -copy default secondary integration-tests $SECONDARY_STORAGE
  ${DUPLICACY} backup
  popd

}

function init_repo_pref_dir()
{
  pushd ${TEST_REPO}
  ${DUPLICACY} init -pref-dir "${DUPLICACY_PREF_DIR}"  integration-tests ${TEST_STORAGE}
  ${DUPLICACY} add -copy default secondary integration-tests $SECONDARY_STORAGE
  ${DUPLICACY} backup
  popd

}

function add_file()
{
  FILE_NAME=$1
  pushd ${TEST_REPO}
  dd if=/dev/urandom of=${FILE_NAME} bs=1000 count=20000
    echo ${FILE_NAME} > "${FILE_NAME}"
  popd
}


function backup()
{
  pushd ${TEST_REPO}
  ${DUPLICACY} backup
  ${DUPLICACY} copy -from default -to secondary
  popd
}


function restore()
{
  pushd ${TEST_REPO}
  ${DUPLICACY} restore -r 2 -delete
  popd
}

function check()
{
  pushd ${TEST_REPO}
  ${DUPLICACY} check -files
  ${DUPLICACY} check -storage secondary -files
  popd
}
