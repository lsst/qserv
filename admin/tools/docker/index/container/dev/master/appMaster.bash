#! /bin/bash
# admin/tools/docker/loader/container/dev/master/appMaster.bash

_term() { 
  echo "Caught SIGTERM signal!" 
  kill -TERM "$child" 2>/dev/null
}

trap _term SIGTERM
trap _term SIGKILL

source /qserv/stack/loadLSST.bash
cd /qserv/dev/qserv
setup -r . -t qserv-dev

/qserv/dev/qserv/build/loader/appMaster /qserv/dev/qserv/core/modules/loader/config/master.cnf

child=$!
wait "$child"
