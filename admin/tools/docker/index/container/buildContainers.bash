#! /bin/bash

# qserv/admin/tools/docker/loader/container/buildContainers.bash
# cd back to base qserv directory as the docker COPY needs the entire project
cd ../../../../../../qserv
docker build -f admin/tools/docker/loader/container/dev/Dockerfile -t qserv/indexbase:dev .



# go to worker directory to avoid context copy
cd admin/tools/docker/loader/container/dev/worker/
docker build -t qserv/indexworker:dev .


