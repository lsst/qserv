#
#
# cd ~/work/qserv/admin/tools/docker/index/container/dev/clientNum
# docker build -t qserv/indexclientnum:dev .
FROM qserv/indexbase:dev 

USER 0

RUN yum update --assumeyes && yum install --assumeyes bind-utils

USER 1000
    
ENTRYPOINT ["/home/qserv/dev/qserv/admin/tools/docker/index/container/dev/clientNum/appClientNum.bash"]
CMD ["1 500000"]
