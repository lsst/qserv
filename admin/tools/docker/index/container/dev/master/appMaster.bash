#! /bin/bash -l
# admin/tools/docker/loader/container/dev/master/appMaster.bash

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

/home/qserv/dev/qserv/build/loader/appMaster /home/qserv/dev/qserv/core/modules/loader/config/master.cnf

child=$!
echo "child ${child}"
wait "$child"
