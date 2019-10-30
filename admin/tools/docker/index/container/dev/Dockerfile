# docker build -f admin/tools/docker/index/container/dev/Dockerfile -t qserv/indexbase:dev .
#
# Using the development toolchain

FROM qserv/qserv:dev

USER 1000 

RUN mkdir /home/qserv/dev/ && \
    chown -R qserv:qserv /home/qserv

COPY --chown=qserv:qserv . /home/qserv/dev/qserv 

RUN bash -lc "rm -rf /home/qserv/dev/qserv/build /home/qserv/dev/qserv/share /home/qserv/dev/qserv/bin && \
    cd /qserv/stack/ && source ./loadLSST.bash && \
    cd /home/qserv/dev/qserv && setup -r . -t qserv-dev && \
    printenv && \
    scons -j10 install && \
    mkdir -p /home/qserv/run && \
    qserv-configure.py --all -R /home/qserv/run"
