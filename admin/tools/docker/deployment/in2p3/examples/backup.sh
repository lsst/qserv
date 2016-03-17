set -x

/opt/shmux/bin/shmux -c "sudo -u qserv sh -c 'mkdir -p /qserv/backup/worker-innodb/ && mv /qserv/data/mysql/ib* /qserv/backup/worker-innodb/'" ccqserv{126..149}.in2p3.fr
