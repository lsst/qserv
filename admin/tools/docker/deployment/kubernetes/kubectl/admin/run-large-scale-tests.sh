#!/bin/bash

# Launch Qserv S15 large scale tests
# Initialize git_ref to use specific branch of
# qserv_testscale repository

# Set GIT_REF environement variable to use
# a given branch of https://github.com/lsst/qserv_testscale

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

TEST_DIR="$HOME/tmp/qserv_testscale"

if [ -z "$GIT_REF" ]; then
    GIT_REF="master"
else
    echo "Using branch $GIT_REF for qserv_testscale"
fi

rm -rf "$TEST_DIR"
git clone --depth 1 -b "$GIT_REF" --single-branch \
	https://github.com/lsst/qserv_testscale.git \
        "$TEST_DIR"

"$TEST_DIR"/S15/tests/run-all.sh -M "$MASTER"
