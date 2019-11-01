#! /bin/bash

# qserv/admin/tools/docker/loader/container/buildContainers.bash
# cd back to base qserv directory as the docker COPY needs the entire project
cd ../../../../../../qserv
docker build -f admin/tools/docker/index/container/dev/Dockerfile -t qserv/indexbase:dev .

# go to worker directory to avoid context copy
cd admin/tools/docker/index/container/dev/worker/
docker build -t qserv/indexworker:dev .

# go to master directory to avoid context copy
cd ../master/
docker build -t qserv/indexmaster:dev .

# go to clientNum directory to avoid context copy
cd ../clientNum/
docker build -t qserv/indexclientnum:dev .

