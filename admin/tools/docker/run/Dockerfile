FROM qserv/lite-run-base

RUN dnf install -y 'dnf-command(config-manager)' \
    && dnf config-manager --set-enabled powertools \
    && dnf install -y epel-release \
    && dnf update -y \
    && dnf install -y \
       netcat \
       lsof \
    && dnf clean all \
    && rm -rf /var/cache/yum

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
    mkdir -p /var/run/xrootd && \
    chown qserv:qserv /qserv/data /config-etc /qserv/run/tmp /var/run/xrootd

USER qserv

VOLUME /home/qserv
WORKDIR /home/qserv
