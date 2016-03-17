#!/bin/bash

# Run SQL queries against Qserv cluster

# @author Fabrice Jammes IN2P3

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

. /qserv/stack/loadLSST.bash

setup mysqlclient
time mysql --host "$MASTER" --port 4040 --user qsmaster LSST -e "SELECT ra, decl FROM Object WHERE deepSourceId = 2322920177142607;"

setup mysqlpython
python "${DIR}/runQueries.py"
