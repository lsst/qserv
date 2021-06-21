#-------------------------------------------------------------------------------------------------------------
# MariaDB + scisql image targets for Qserv
#
# These images are expected to change relatively infrequently (built by CI perhaps once or twice yearly; not
# routinely built by developers), only as MariaDB or scisql upgrades are needed by Qserv.
#
# First up: the 'mariadb-scisql-builder' target, which builds a container image in which the scisql extension
# is built. The starting point here should be the same base image as the targeted off-the-shelf MariaDB image
# (see Dockerfile 'FROM' line for the targeted MariaDB release in https://github.com/MariaDB/mariadb-docker.)
#
# Scisql is cloned, built from source, and left installed to /usr/local.
#-------------------------------------------------------------------------------------------------------------

FROM ubuntu:focal AS mariadb-scisql-builder

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

#-------------------------------------------------------------------------------------------------------------
# The 'mariadb-scisql' target layers just the runtime artifacts from the builder target above on top of an
# off-the-shelf mariadb image.  This produces a generic "mariadb with scisql installed" container image. 
#-------------------------------------------------------------------------------------------------------------

FROM mariadb:10.4 AS mariadb-scisql
COPY --from=mariadb-scisql-builder /tmp/scisql/build/libscisql-scisql_0.3.so /usr/lib/mysql/plugin
COPY --from=mariadb-scisql-builder /tmp/scisql/build/deploy.mysql /docker-entrypoint-initdb.d/scisql.sql

#-------------------------------------------------------------------------------------------------------------
# The 'lite-mariadb' target further customizes the generic 'mariadb-scisql' image above by adding a 'qserv'
# user, creating directories with appropriate ownership at locations expected to be used for bind-mounts in
# the qserv operating environment, and setting the default runtime user to 'qserv'.  This image is the
# artifact to be used for all MariaDB instantiations within a Qserv deployment.  
#-------------------------------------------------------------------------------------------------------------

FROM mariadb-scisql AS lite-mariadb

RUN useradd --uid 1000 qserv

RUN mkdir -p /qserv/data /config-etc /var/log/mysql \
    && chown qserv:qserv /qserv/data /config-etc /var/log/mysql

USER qserv
