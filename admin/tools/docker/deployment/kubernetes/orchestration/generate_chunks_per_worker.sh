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

OUT_FILE="chunk_for_workers-${WORKER_FIRST_ID}-to-${WORKER_LAST_ID}.txt"

cat $WORKERS > "chunk_for_workers-${WORKER_FIRST_ID}-to-${WORKER_LAST_ID}"
echo "Chunk for workers "$WORKER_FIRST_ID" to "$WORKER_LAST_ID" stored in $OUT_FILE"
