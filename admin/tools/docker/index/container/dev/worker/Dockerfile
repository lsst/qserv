#
#
# cd ~/work/qserv/admin/tools/docker/index/container/dev/worker
# docker build -t qserv/indexworker:dev .
FROM qserv/indexbase:dev 

USER 1001
    
ENTRYPOINT ["/qserv/dev/qserv/admin/tools/docker/index/container/dev/worker/appWorker.bash"]
