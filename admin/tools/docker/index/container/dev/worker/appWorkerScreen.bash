#! /bin/bash
# admin/tools/docker/loader/container/dev/worker/appWorker.bash

_term() { 
  echo "Caught SIGTERM signal!" 
  kill -TERM "$child" 2>/dev/null
}

trap _term SIGTERM
trap _term SIGKILL

screen -dm /home/qserv/dev/qserv/admin/tools/docker/index/container/dev/worker/appWorker.bash

child=$!
echo "child ${child}"
wait "$child"

sleep 10000
tail -f /dev/null