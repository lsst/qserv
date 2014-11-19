# These commands require Docker on your machine and that you belong to 'docker' group

#
# Download and run a docker image containing qserv 2014_10
#
docker run --hostname="qserv-host" --interactive=true --tty "fjammes/qserv:2014_10" /bin/bash

# Below, we assume your Docker username is jdoe, create it here: https://hub.docker.com

#
# Create an image from a container
#
# Show all containers and choose one you've worked on
docker ps -all
CONTAINER_ID=f98249856123
# Commit your current container state in image jdoe/qserv:dm-1234
docker commit --message="DM-1234" --author="John Doe" ${CONTAINER_ID} jdoe/qserv:dm-1234

#
# Build an image using a Dockerfile
#
docker build --tag="jdoe/qserv:2014_10" - < Dockerfile

#
# Upload image jdoe/qserv:dm-1234 to your Docker repository
#
docker push jdoe/qserv:dm-1234

#
# Cleanup
#
# Clean old local images
docker images | grep 'hours ago' | awk '{print $3}' | xargs --no-run-if-empty docker rmi
# Clean old local containers
docker ps -a | grep 'hours ago' | awk '{print $1}' | xargs --no-run-if-empty docker rm
