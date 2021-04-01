FROM ubuntu:focal AS builder

RUN apt-get update \
    && apt-get install -y software-properties-common \
    && apt-key adv --fetch-keys 'https://mariadb.org/mariadb_release_signing_key.asc' \
    && add-apt-repository 'deb [arch=amd64,arm64,ppc64el] http://mirrors.piconets.webwerks.in/mariadb-mirror/repo/10.4/ubuntu focal main' \
    && apt-get update \
    && apt-get install -y g++ make git python3 libmariadb-dev \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.8 2

RUN cd /tmp \
    && git clone https://github.com/smonkewitz/scisql \
    && cd scisql \
    && git checkout tags/0.3.9 -b 0.3.9 \
    && ./configure --mysql-includes=/usr/include/mariadb \
    && make -j8 install

RUN cd /tmp/scisql \
    && PYTHONPATH=/usr/local/python python -c "\
from scisql import configure; \
configure.init_config(None, None, None, None, None); \
configure.apply_templates('/usr/local/templates', 'build'); \
"

RUN chmod a+r /tmp/scisql/build/deploy.mysql

#------------------------------------------------------------------------------

FROM mariadb:10.4 AS mariadb-scisql
COPY --from=builder /tmp/scisql/build/libscisql-scisql_0.3.so /usr/lib/mysql/plugin
COPY --from=builder /tmp/scisql/build/deploy.mysql /docker-entrypoint-initdb.d/scisql.sql

#------------------------------------------------------------------------------

FROM mariadb-scisql AS lite-mariadb

RUN useradd --uid 1000 qserv

RUN mkdir -p /qserv/data /config-etc /var/log/mysql \
    && chown qserv:qserv /qserv/data /config-etc /var/log/mysql

USER qserv
