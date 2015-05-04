#!/bin/sh

set -e


EMPTY_CHUNK_PATH={{QSERV_DATA_DIR}}/qserv
DIR=$(cd "$(dirname "$0")"; pwd -P)
SQL_LOADER=${DIR}/tools/sql-loader.sh
SQL_FILE=qserv-czar.sql

mkdir -p $EMPTY_CHUNK_PATH

echo 
echo "-- Initializing Qserv czar database "
if [ -r "${SQL_LOADER}" ]; then
    . "${SQL_LOADER}"
else
    >&2 echo "Unable to source ${SQL_LOADER}"
    exit 1
fi

echo "INFO: Qserv Czar initialization SUCCESSFUL"
