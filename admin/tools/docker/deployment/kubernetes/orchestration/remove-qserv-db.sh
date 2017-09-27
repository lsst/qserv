#!/bin/bash

# Help removing qserv database 
# WARN: do not currently remove database but only metadata 

# @author Fabrice Jammes SLAC/IN2P3

set -e
set -x

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

DB=LSST20

echo "Delete '$DB' entry from qservw_worker.Dbs"
parallel -v "kubectl exec {} -c worker -- \
    bash -c \". /qserv/stack/loadLSST.bash && \
    setup mariadbclient && \
    mysql --socket /qserv/run/var/lib/mysql/mysql.sock \
    --user=root --password=changeme \
    -e \\\"DELETE FROM qservw_worker.Dbs WHERE db='$DB';\\\"\"" ::: $WORKER_PODS
