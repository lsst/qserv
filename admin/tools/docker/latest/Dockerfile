FROM debian:jessie
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN sh -c "echo \"BUILD ID: $(date '+%Y%m%d_%H%M%S')\" > /BUILD_ID"

# Start with this long step not to re-run it on
# each Dockerfile update
RUN echo "deb http://ftp.debian.org/debian jessie-backports main" >> /etc/apt/sources.list
RUN apt-get -y update && \
    apt-get -y install apt-utils && \
    apt-get -y upgrade && \
    apt-get -y clean

# Install LSST stack dependencies
#
RUN apt-get -y install bison \
        bzip2 \
        cmake \
        curl \
        flex \
        g++ \
        gettext \
        git \
        libbz2-dev \
        libglib2.0-dev \
        libpthread-workqueue-dev \
        libreadline-dev \
        libssl-dev \
        make \
        ncurses-dev \
        openjdk-7-jre-headless \
        openssl \
        patch \
        uuid-dev \
        zlib1g-dev

RUN apt-get --yes -t jessie-backports install cmake

RUN groupadd qserv && \
    useradd -m -g qserv qserv && \
    usermod -s /bin/bash qserv && \
    mkdir /qserv && \
    chown qserv:qserv /qserv

ARG EUPS_TAG

# Install development and debugging tools
#
RUN if [ "$EUPS_TAG" = "qserv-dev" ] ; then \
    apt-get -y install byobu dnsutils \
        gdb graphviz lsof \
        net-tools vim ; \
    fi

USER qserv

# Install LSST stack
#
ENV STACK_DIR /qserv/stack
RUN mkdir $STACK_DIR && cd $STACK_DIR && \
    curl -OL \
    https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh && \
    bash newinstall.sh -b

RUN bash -c ". $STACK_DIR/loadLSST.bash && eups distrib install qserv_distrib -t '$EUPS_TAG'"

COPY scripts/*.sh /qserv/scripts/

WORKDIR /home/qserv

ENV QSERV_RUN_DIR /qserv/run

# Generate /qserv/run/sysconfig/qserv and /qserv/run/etc/init.d/qserv-functions
# required by k8s setup
RUN bash -c ". /qserv/stack/loadLSST.bash && \
             setup qserv -t qserv-dev && \
             cp \"\$SCISQL_DIR\"/lib/libscisql-scisql_?.?.so \"\$MARIADB_DIR\"/lib/plugin && \
             qserv-configure.py --init --force --qserv-run-dir \"$QSERV_RUN_DIR\" && \
             qserv-configure.py --etc --qserv-run-dir \"$QSERV_RUN_DIR\" --force && \
             rm $QSERV_RUN_DIR/qserv-meta.conf"

# Allow install of additional packages in pods and ease install scripts
# execution
USER root
