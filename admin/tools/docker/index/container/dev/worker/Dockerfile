#
#
# cd ~/work/qserv/admin/tools/docker/index/container/dev/worker
# docker build -t qserv/indexworker:dev .
FROM qserv/indexbase:dev 

USER 0

RUN yum update --assumeyes && yum install --assumeyes bind-utils

USER 1001
    
ENTRYPOINT ["/qserv/dev/qserv/admin/tools/docker/index/container/dev/worker/appWorker.bash"]
