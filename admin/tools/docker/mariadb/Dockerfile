FROM ubuntu:bionic AS builder

RUN apt update
RUN apt-get install -y software-properties-common
RUN apt-key adv --fetch-keys 'https://mariadb.org/mariadb_release_signing_key.asc'
RUN add-apt-repository 'deb [arch=amd64,arm64,ppc64el] http://mirrors.piconets.webwerks.in/mariadb-mirror/repo/10.4/ubuntu bionic main'
RUN apt update

RUN apt-get install -y g++ make git python3 libmariadb-dev
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.6 2

WORKDIR /root
RUN git clone https://github.com/smonkewitz/scisql
WORKDIR /root/scisql
RUN git checkout tags/0.3.9 -b 0.3.9
RUN ./configure --mysql-includes=/usr/include/mariadb
RUN make
RUN make install

RUN PYTHONPATH=/usr/local/python python -c "\
from scisql import configure; \
configure.init_config(None, None, None, None, None); \
configure.apply_templates('/usr/local/templates', 'build'); \
"
RUN chmod a+r build/deploy.mysql

FROM mariadb:10.4 AS mariadb-scisql
COPY --from=builder /root/scisql/build/libscisql-scisql_0.3.so /usr/lib/mysql/plugin
COPY --from=builder /root/scisql/build/deploy.mysql /docker-entrypoint-initdb.d/scisql.sql

FROM mariadb-scisql AS mariadb-scisql-qserv
RUN useradd --uid 1000 qserv

RUN mkdir -p /qserv/data && \
    mkdir /config-etc && \
    mkdir -p /var/log/mysql && \
    chown qserv:qserv /qserv/data /config-etc /var/log/mysql

USER qserv
