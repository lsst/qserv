ARG TAG=latest

#------------------------------------------------------------------------------

FROM qserv/lite-build:${TAG} AS builder

RUN cd /tmp \
    && git clone https://github.com/lsst/qserv.git \
    && cd qserv \
    && git checkout tickets/DM-23308 \
    && git submodule update --init \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make -j8 install

#------------------------------------------------------------------------------

FROM qserv/lite-run-base:${TAG} as lite-qserv

COPY --from=builder /tmp/qserv/build/install/bin/ /usr/local/bin/
COPY --from=builder /tmp/qserv/build/install/lib64/ /usr/local/lib64/
COPY --from=builder /tmp/qserv/build/install/lua/ /usr/local/lua/
COPY --from=builder /tmp/qserv/build/install/python/ /usr/local/python/
COPY --from=builder /tmp/qserv/build/install/qserv/ /usr/local/qserv/

RUN echo /usr/local/lib64 >> /etc/ld.so.conf.d/local.conf && ldconfig

ENV PYTHONPATH "${PYTHONPATH}:/usr/local/python"

RUN useradd --create-home --uid 1000 --shell /bin/bash qserv

RUN mkdir -p /qserv/data /config-etc /qserv/run/tmp \
    && chown qserv:qserv /qserv/data /config-etc /qserv/run/tmp

RUN setcap cap_ipc_lock=+ep /usr/local/bin/xrootd
USER qserv

VOLUME /home/qserv
WORKDIR /home/qserv
