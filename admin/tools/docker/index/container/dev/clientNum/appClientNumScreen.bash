#! /bin/bash -l
# admin/tools/docker/loader/container/dev/clientNum/appClientNumScreen.bash

echo appClientScreen $1 $2 $3

screen -dm /home/qserv/dev/qserv/admin/tools/docker/index/container/dev/clientNum/appClientNum $1 $2 $3 


tail -f /dev/null
