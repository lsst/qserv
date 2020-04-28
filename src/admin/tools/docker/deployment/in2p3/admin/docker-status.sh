set -x

/opt/shmux/bin/shmux -c "echo \"$PASSWORD\" | sudo -S sh -c 'service docker status'" ccqserv{125..149}.in2p3.fr

