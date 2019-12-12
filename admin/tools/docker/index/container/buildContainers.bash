#! /bin/bash

# qserv/admin/tools/docker/loader/container/buildContainers.bash
# cd back to base qserv directory as the Dockerfile COPY needs the entire project
# in the docker context.
cd ../../../../../../qserv
docker build -f admin/tools/docker/index/container/dev/Dockerfile -t qserv/indexbase:dev .

# go to individual directories to minimize the size of docker's context copy
# worker
cd admin/tools/docker/index/container/dev/worker/ && docker build -t qserv/indexworker:dev .
#docker build -f admin/tools/docker/index/container/dev/worker/Dockerfile -t qserv/indexworker:dev .

# master
cd ../master/ && docker build -t qserv/indexmaster:dev .
#docker build -f admin/tools/docker/index/container/dev/master/Dockerfile -t qserv/indexmaster:dev .

# clientNum
cd ../clientNum/ && docker build -t qserv/indexclientnum:dev .
#docker build -f admin/tools/docker/index/container/dev/clientNum/Dockerfile -t qserv/indexclientnum:dev .



docker build -f admin/tools/docker/index/container/dev/Dockerfile -t qserv/indexbase:dev . && \
cd admin/tools/docker/index/container/dev/worker/ && docker build -t qserv/indexworker:dev . && \
cd ../master/ && docker build -t qserv/indexmaster:dev . && \
cd ../clientNum/ && docker build -t qserv/indexclientnum:dev . && \
cd ../../../../../../../../qserv
docker push qserv/indexmaster:dev && docker push qserv/indexworker:dev && docker push qserv/indexclientnum:dev


