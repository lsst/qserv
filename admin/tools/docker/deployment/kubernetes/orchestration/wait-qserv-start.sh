#!/bin/bash

# Start Qserv on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

# Retry Qserv startup test because K8s pod might crash for unknow reason:
# k8s might try to restart pods in-between, so there is no garantee qserv
# containers are started, race condition is unavoidable
MAX_RETRY=10
TIME_WAIT=3
retry=0
GO_TPL='{{range .items}}{{.metadata.name}} {{end}}'
PODS=$(kubectl get pods -l app=qserv -o go-template --template "$GO_TPL")
for qserv_pod in $PODS
do
    retry=0
    started=false
    while [ $started = false ]; do
        echo "Wait for pod '$qserv_pod' to start"
        if [ "$qserv_pod" = master ]
        then
            kubectl exec "$qserv_pod" /qserv/scripts/wait.sh && started=true
        else
            # this check should be enough because:
            # - xrootd is the latest service to be started
            # - if mysql or qserv-wmgr crashes, then related container will
            #   crash, preventing pod to be in RUNNING state
            kubectl exec "$qserv_pod" -c worker -- \
                sh -c "/qserv/run/etc/init.d/xrootd status > /dev/null 2>&1" \
                && started=true
        fi
        if [ $started = true ]
        then
            echo "Succeed to start pod '$qserv_pod'"
        else
            retry=$((retry+1));
            if [ $retry -gt $MAX_RETRY ]
            then
                echo "ERROR: Fail to start pod $qserv_pod"
                exit 1
            fi
            sleep "$TIME_WAIT"
        fi
    done
done
