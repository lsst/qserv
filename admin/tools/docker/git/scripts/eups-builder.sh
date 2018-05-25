#!/bin/bash
set -e
set -x

GIT_PROJECT=qserv_testdata
GIT_REF='tickets/DM-13979'
GIT_REPO="https://github.com/lsst/${GIT_PROJECT}.git"
BUILD_DIR='/tmp/build'
git clone -b "$GIT_REF" --single-branch --depth=1 "$GIT_REPO" "$BUILD_DIR"
cd "$BUILD_DIR"
GIT_HASH=$(git rev-parse --verify HEAD)
echo "Build from ${GIT_REPO} (git hash: $GIT_HASH)"

# Launch eups build 
. /qserv/stack/loadLSST.bash
setup -r . -t qserv-dev
eupspkg -er install
eupspkg -er decl -t qserv-dev

rm -rf "$BUILD_DIR"
