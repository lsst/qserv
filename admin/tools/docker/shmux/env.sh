MASTER=clrqserv00
WORKERS=$(echo clrqserv0{1..3})

MASTER_IMAGE=fjammes/qserv:master_master_clrqserv00.in2p3.fr
WORKER_IMAGE=fjammes/qserv:master_worker_clrqserv00.in2p3.fr
#alias shmux=/usr/local/bin/shmux
