#! /bin/bash -l
# admin/tools/docker/loader/container/dev/clientNum/appClientNum.bash

_term() { 
  echo "Caught SIGTERM signal!" 
  kill -TERM "$child" 2>/dev/null
}

trap _term SIGTERM
trap _term SIGKILL


echo appClientScreen $1 $2 $3

screen -dm /home/qserv/dev/qserv/admin/tools/docker/index/container/dev/clientNum/appClientNum $1 $2 $3 

child=$!
wait "$child"
tail -f /dev/null