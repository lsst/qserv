FROM qserv/lite-build:2020.04.0 AS builder

WORKDIR /home/qserv

RUN git clone https://github.com/lsst/qserv.git

WORKDIR /home/qserv/qserv

RUN git checkout tickets/DM-23308 && \
    git submodule update --init && \
    mkdir /home/qserv/qserv/build

WORKDIR /home/qserv/qserv/build

RUN cmake ..; make install;

#------------------------------------------------------------------------------

FROM qserv/lite-run-base:2020.04.0 as qserv_ci

COPY --from=builder /home/qserv/qserv/build/install/bin/ /usr/local/bin/
COPY --from=builder /home/qserv/qserv/build/install/lib64/ /usr/local/lib64/
COPY --from=builder /home/qserv/qserv/build/install/lua/ /usr/local/lua/
COPY --from=builder /home/qserv/qserv/build/install/python/ /usr/local/python/
COPY --from=builder /home/qserv/qserv/build/install/qserv/ /usr/local/qserv/

RUN echo /usr/local/lib64 >> /etc/ld.so.conf.d/local.conf && ldconfig

ENV PYTHONPATH "${PYTHONPATH}:/usr/local/python"

RUN useradd --create-home --uid 1000 --shell /bin/bash qserv

RUN mkdir -p /qserv/data && \
    mkdir /config-etc && \
    mkdir -p /qserv/run/tmp && \
    chown qserv:qserv /qserv/data /config-etc /qserv/run/tmp

USER qserv

VOLUME /home/qserv
WORKDIR /home/qserv

