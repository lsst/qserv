#!/bin/sh

# print chunk lists for all nodes

# @author Fabrice Jammes IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

DATA_DIR="/qserv/data/mysql/LSST"
RESULT_DIR="out_chunks"

mkdir -p "$RESULT_DIR"

DIRECTOR_TABLE=Object

echo "List chunks in $DATA_DIR on all nodes"
parallel --results "$RESULT_DIR" "kubectl exec {} -- sh -c 'find  /qserv/data/mysql/LSST -name \"Object_*.frm\" | \
    grep -v \"1234567890.frm\" | \
    sed \"s;${DATA_DIR}/Object_\([0-9][0-9]*\)\.frm$;\1;\"'" ::: $MASTER_POD $WORKER_PODS
