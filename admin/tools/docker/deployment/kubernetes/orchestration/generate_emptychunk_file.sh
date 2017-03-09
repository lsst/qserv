#!/bin/sh

# Generate list of chunk file
# for given worker list

# @author Fabrice Jammes IN2P3

set -e

# First disabled worker
WORKER_FIRST_ID=7

# Last disabled worker
WORKER_LAST_ID=24

CHUNK_DIR="out_chunks/1"

WORKERS=$(seq --format "$CHUNK_DIR/worker-%g/stdout" \
    --separator=' ' "$WORKER_FIRST_ID" "$WORKER_LAST_ID")

CHUNK_FILE="chunk_for_workers-${WORKER_FIRST_ID}-to-${WORKER_LAST_ID}.txt"

cat $WORKERS > "$CHUNK_FILE"
echo "Chunk for workers "$WORKER_FIRST_ID" to "$WORKER_LAST_ID" stored in $CHUNK_FILE"

EMPTY_CHUNK_FILE="/qserv/data/qserv/empty_LSST.disable_$WORKER_FIRST_ID-to-$WORKER_LAST_ID.txt"

echo "Creating $EMPTY_CHUNK_FILE on Qserv master"
kubectl cp "$CHUNK_FILE" master:/qserv/run/tmp/ -c master
kubectl exec master -- sh -c "cat '/qserv/run/tmp/$CHUNK_FILE' \
	/qserv/data/qserv/empty_LSST.enable_all.txt > $EMPTY_CHUNK_FILE"
