#!/bin/bash

# Launch Qserv multinode tests
# using Docker containers

set -x

. ./env.sh

shmux -c "docker rm -f qserv; nohup docker run --name qserv --rm -h $MASTER \
    -p 4040:4040 -p 1094:1094 -p 2131:2131 -p 12181:12181 -p 5012:5012 \
    fjammes/qserv:master-$MASTER \
    > qserv.out 2> qserv.err < /dev/null &" $MASTER

# hostname has to be evaluated remotely 
shmux -c 'docker rm -f qserv; nohup docker run --name qserv --rm -h $(hostname --fqdn) \
    -p 1094:1094 -p 5012:5012 \
    fjammes/qserv:worker-'$MASTER' \
    > qserv.out 2> qserv.err < /dev/null &' $WORKERS

shmux -c "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && setup qserv_distrib && \
    echo \"$(cat nodes.example.css)\" | qserv-admin.py && qserv-test-integration.py'" $MASTER
