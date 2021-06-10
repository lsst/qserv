FROM qserv/lite-run-base

COPY bin/ /usr/local/bin/
COPY lib64/ /usr/local/lib64/
COPY lua/ /usr/local/lua/
COPY python/ /usr/local/python/
COPY qserv/ /usr/local/qserv/
COPY etc/ /usr/local/etc/

RUN ldconfig

USER qserv
