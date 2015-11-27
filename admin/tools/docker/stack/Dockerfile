FROM debian:jessie
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apt-get -y update && apt-get -y install apt-utils
RUN apt-get -y upgrade && apt-get -y clean
RUN apt-get -y install curl python make
RUN apt-get -y install flex 
RUN apt-get -y install bison 
RUN apt-get -y install g++ 

RUN groupadd qserv && \
    useradd -m -g qserv qserv && \
    usermod -s /bin/bash qserv && \
    mkdir /qserv && \
    chown qserv:qserv /qserv

USER qserv

WORKDIR /qserv

# Copying only install-stack.sh to avoid re-installing if
# other files changes in scripts/
COPY scripts/install-stack.sh scripts/install-stack.sh

# Hack to enable diagnose of stack install failure
RUN /bin/bash /qserv/scripts/install-stack.sh 2>&1 /qserv/STACK_INSTALL.out || echo 'INSTALL FAILED' > /qserv/STACK_INSTALL.err && cat $(find /qserv/stack -name *.log)

COPY scripts/*.sh scripts/

USER root

RUN chown -R qserv:qserv /qserv/scripts/
