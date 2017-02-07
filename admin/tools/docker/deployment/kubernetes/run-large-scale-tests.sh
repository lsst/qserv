#!/bin/bash

# Launch Qserv multinode tests

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env-cluster.sh"

TEST_DIR="$HOME/tmp/qserv_testscale"
rm -rf "$TEST_DIR"
GIT_REF="master"
git clone --depth 1 -b "$GIT_REF" --single-branch \
	https://github.com/lsst/qserv_testscale.git "$TEST_DIR"

# Use k8s environment, instead of shmux
cp "${DIR}/env-testscale.sh" "$TEST_DIR"/S15/tests/env.sh
"$TEST_DIR"/S15/tests/run-all.sh -M "$ORCHESTRATOR"
