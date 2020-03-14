FROM qserv-run

COPY bin/ /usr/local/bin/
COPY lib64/ /usr/local/lib64/
COPY lua/ /usr/local/lua/
COPY python/ /usr/local/python/
COPY qserv/ /usr/local/qserv/

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

