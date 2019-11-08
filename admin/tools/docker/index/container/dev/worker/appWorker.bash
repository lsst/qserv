#! /bin/bash
# admin/tools/docker/loader/container/dev/worker/appWorker.bash

_term() { 
  echo "Caught SIGTERM signal!" 
  kill -TERM "$child" 2>/dev/null
}

trap _term SIGTERM
trap _term SIGKILL

source /qserv/stack/loadLSST.bash
cd /home/qserv/dev/qserv
setup -r . -t qserv-dev

export LSST_LOG_CONFIG=/home/qserv/dev/qserv/admin/templates/configuration/etc/log4cxx.index.properties

/home/qserv/dev/qserv/build/loader/appWorker /home/qserv/dev/qserv/core/modules/loader/config/worker-k8s-a.cnf

child=$!
echo "child ${child}"
wait "$child"

sleep 10000
tail -f /dev/null