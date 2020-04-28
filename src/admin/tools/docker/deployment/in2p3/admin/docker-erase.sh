set -x

/opt/shmux/bin/shmux -c "echo \"$PASSWORD\" | sudo -S sh -c 'rm -rf /qserv/docker/*'" ccqserv{100..149}.in2p3.fr
