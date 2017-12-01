ARG  MARIADB_VERSION=latest
FROM mariadb:$MARIADB_VERSION
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

# Start with this long step not to re-run it on
# each Dockerfile update
# TODO manage scisql dependencies with eups
RUN apt-get -y update && \
    apt-get -y install apt-utils && \
    apt-get -y install gcc git make libmariadbclient-dev \
                       python python-dev python-pip && \
    apt-get -y clean

RUN apt-get -y install libssl-dev openssl

# TODO manage scisql dependencies with eups
RUN pip install MySQL-python future

# All data files belong to user 1000 on host
#
RUN usermod -u 1000 mysql && \
    groupmod -g 1000 mysql

# Create directory for qserv data and logs
#
RUN mkdir /qserv && \
    chown mysql:mysql /qserv

# Install scisql
# TODO use eups?
#
RUN cd /usr/local/src && \
    git clone https://github.com/smonkewitz/scisql.git && \
    cd scisql && \
    git checkout tickets/DM-11126 && \
    ./configure --mysql-includes=/usr/include/mysql --&& \
    make && \
    make install && \
    cp /usr/local/lib/libscisql-scisql_0.3.so /usr/lib/mysql/plugin/ 
ENV PYTHONPATH=/usr/local/python

# Override default mysql configuration
# Add mysql, scisql, qserv dabatase initialisation scripts
# WARN: cannot copy to / because /tmp acl are changed
#
COPY resources/tmp /tmp/
