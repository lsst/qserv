. ./env.sh

shmux -c 'docker rm -f qserv' $MASTER $WORKERS 
