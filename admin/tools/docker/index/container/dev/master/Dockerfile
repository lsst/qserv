# Run the following build command from the qserv base directory (could be ~/work/qserv or ~/development/qserv)
#   The COPY command can only access files below $PWD in the file tree.
# docker build -f admin/tools/docker/index/container/dev/master/Dockerfile -t qserv/indexmaster:dev .
FROM qserv/indexbase:dev 

USER 1001
    
ENTRYPOINT ["/qserv/dev/qserv/admin/tools/docker/index/container/dev/master/appMaster.bash"]
