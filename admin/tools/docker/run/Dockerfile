FROM qserv-run

COPY bin/ /usr/local/bin/
COPY lib64/ /usr/local/lib64/
COPY lua/ /usr/local/lua/
COPY python/ /usr/local/python/
COPY smig/ /usr/local/qserv/smig/

ENV PYTHONPATH "${PYTHONPATH}:/usr/local/python"

RUN echo /usr/local/lib64 >> /etc/ld.so.conf.d/local.conf \
    && ldconfig
