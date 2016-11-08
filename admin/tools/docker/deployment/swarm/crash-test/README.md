To use crash tests:

* Run multinode tests using swarm
* Extract data from container to host disk
* Edit manager/env-docker.sh so that container mount /qserv/data on host
* Restart swarm cluster
* Run crash tests
