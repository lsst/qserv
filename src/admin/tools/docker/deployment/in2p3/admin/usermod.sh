set -x

/opt/shmux/bin/shmux -c "echo \"$PASSWORD\" | sudo -S sh -c 'usermod -a  -G docker qserv'" ccqserv{125..149}.in2p3.fr
