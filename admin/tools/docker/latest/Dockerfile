FROM debian:jessie
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN echo "deb http://ftp.debian.org/debian jessie-backports main" >> /etc/apt/sources.list
COPY scripts/install-deps.sh /root/install-deps.sh

# Start with this long step not to re-run it on
# each Dockerfile update
RUN apt-get -y update && \
    apt-get -y install apt-utils && \
    /root/install-deps.sh && \
    apt-get -y upgrade && \
    apt-get -y clean

RUN groupadd qserv && \
    useradd -m -g qserv qserv && \
    usermod -s /bin/bash qserv && \
    mkdir /qserv && \
    chown qserv:qserv /qserv

USER qserv

WORKDIR /qserv

# Copying only install-stack.sh to avoid re-installing if
# other files changes in scripts/
ARG EUPS_TAG
COPY scripts/install-stack.sh scripts/install-stack.sh

RUN /bin/bash /qserv/scripts/install-stack.sh 2>&1 /qserv/STACK_INSTALL.out

COPY scripts/*.sh scripts/
