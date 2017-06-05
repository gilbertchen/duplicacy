#!/bin/bash
set -x
get_abs_filename() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}
DUPLICACY=$(get_abs_filename ../duplicacy_main)

TEST_ZONE=$HOME/DUPLICACY_TEST_ZONE
TEST_REPO=$TEST_ZONE/TEST_REPO
TEST_STORAGE=$TEST_ZONE/TEST_STORAGE
TEST_DOT_DUPLICACY=$TEST_ZONE/TEST_DOT_DUPLICACY
TEST_RESTORE_POINT=$TEST_ZONE/RESTORE_POINT
function fixture()
{
  # clean TEST_RESTORE_POINT
  rm -rf $TEST_RESTORE_POINT
  mkdir -p $TEST_RESTORE_POINT

  # clean TEST_STORAGE
  rm -rf $TEST_STORAGE
  mkdir -p $TEST_STORAGE

  # clean TEST_DOT_DUPLICACY
  rm -rf $TEST_DOT_DUPLICACY
  mkdir -p $TEST_DOT_DUPLICACY
  
  # Create test repository
  rm -rf $TEST_REPO
  mkdir -p $TEST_REPO
  pushd $TEST_REPO
    echo "file1" > file1
    mkdir dir1
    echo "file2 >dir1/file2"
  popd  
}

function init_repo()
{
  pushd $TEST_REPO
  $DUPLICACY init integration-tests $TEST_STORAGE
  $DUPLICACY backup
  popd

}

function init_repo_pref_dir()
{
  pushd $TEST_REPO
  $DUPLICACY init -pref-dir "$TEST_ZONE/TEST_DOT_DUPLICACY"  integration-tests $TEST_STORAGE
  $DUPLICACY backup
  popd

}

function backup()
{
  pushd $TEST_REPO
  NOW=`date`
  echo $NOW > "file-$NOW"
  $DUPLICACY backup
  popd
}


function restore()
{
  pushd $TEST_REPO
  $DUPLICACY restore -r 2 -delete
  popd
}

fixture
init_repo_pref_dir

backup
backup
backup
restore

