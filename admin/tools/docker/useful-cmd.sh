# These commands require Docker on your machine and that you belong to 'docker' group

##
## Download and run a docker image containing qserv latest release
##

# -v command will mount host $CWD on vm /some/path
docker run --hostname="qserv-host" -it --rm -v `pwd`:/some/path "fjammes/qserv:latest" /bin/bash

# Below, we assume your Docker username is jdoe, create it here: https://hub.docker.com

##
## Create an image from a container
##

# Show all containers and choose one you've worked on
docker ps -all
CONTAINER_ID=f98249856123

# Dump container console
docker logs ${CONTAINER_ID}

# Commit your current container state in image jdoe/qserv:dm-1234
docker commit --message="DM-1234" --author="John Doe" ${CONTAINER_ID} jdoe/qserv:dm-1234

##
## Build an image using a Dockerfile
##
docker build --tag="jdoe/qserv:2014_10" - < Dockerfile

##
## Upload image jdoe/qserv:dm-1234 to your Docker repository
##
docker push jdoe/qserv:dm-1234

##
## Cleanup
##

# Clean old local containers
docker ps -a | grep 'hours ago' | awk '{print $1}' | xargs --no-run-if-empty docker rm

# Clean old local images
docker images | grep 'hours ago' | awk '{print $3}' | xargs --no-run-if-empty docker rmi

##
## Develop
##

# Launch a shell on a docker container named my_qserv
docker run -it --rm --name my_qserv  -h $(hostname)-docker \
    -v $HOME/src/qserv:/home/dev/src/qserv \
    -v $HOME/qserv-run/:/home/dev/qserv-run \
    -u dev fjammes/qserv:dev-uid bash

# Launch a shell with a different user on that container
docker exec -it -u qserv my_qserv bash

# Commit this version of the container to image fjammes/qserv:dev-uid
docker commit -m "Add xrootd 4.2.3.lsst1" my_qserv fjammes/qserv:dev-uid
