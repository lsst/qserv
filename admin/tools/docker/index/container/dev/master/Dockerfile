# Run the following build command from the qserv base directory (could be ~/work/qserv or ~/development/qserv)
#   The COPY command can only access files below $PWD in the file tree.
# cd ~/work/qserv/admin/tools/docker/index/container/dev/master
# docker build -t qserv/indexmaster:dev .
FROM qserv/indexbase:dev 

USER 0

RUN yum update --assumeyes && yum install --assumeyes bind-utils

USER 1000
    
ENTRYPOINT ["/home/qserv/dev/qserv/admin/tools/docker/index/container/dev/master/appMaster.bash"]
