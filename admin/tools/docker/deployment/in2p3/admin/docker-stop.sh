set -x

/opt/shmux/bin/shmux -c "echo \"$PASSWORD\" | sudo -S sh -c 'service docker stop'" ccqserv{100..149}.in2p3.fr 
