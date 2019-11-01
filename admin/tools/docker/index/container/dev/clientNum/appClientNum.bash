#! /bin/bash
# admin/tools/docker/loader/container/dev/clientNum/appClientNum.bash

_term() { 
  echo "Caught SIGTERM signal!" 
  kill -TERM "$child" 2>/dev/null
}

trap _term SIGTERM
trap _term SIGKILL

source /qserv/stack/loadLSST.bash
cd /qserv/dev/qserv
setup -r . -t qserv-dev

export LSST_LOG_CONFIG=/home/qserv/dev/qserv/admin/templates/configuration/etc/log4cxx.index.properties

echo appClientNum $1 $2 $3

/home/qserv/dev/qserv/build/loader/appClientNum $1 $2 /home/qserv/dev/qserv/core/modules/loader/config/$3 &

#/qserv/dev/qserv/build/loader/appClientNum $1 $2 /qserv/dev/qserv/core/modules/loader/config/client-k8s-a1.cnf &

child=$!
wait "$child"
