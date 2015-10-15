. ./env.sh

shmux -c "docker pull fjammes/qserv:master-${MASTER}" $MASTER 
shmux -c "docker pull fjammes/qserv:worker-${MASTER}" $WORKERS 
