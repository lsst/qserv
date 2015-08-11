FROM debian:jessie
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apt-get -y update
RUN apt-get -y install apt-utils
RUN apt-get -y upgrade
RUN apt-get -y clean

# Start with this long step not to re-run it on
# each Dockerfile update
COPY scripts/install-deps.sh /root/install-deps.sh
RUN /root/install-deps.sh

RUN groupadd qserv
RUN useradd -m -g qserv qserv
RUN usermod -s /bin/bash qserv
RUN mkdir /qserv
RUN chown qserv:qserv /qserv

USER qserv
# Copying only install-stack.sh to avoid re-installing if
# other files changes in scripts/
COPY scripts/install-stack.sh /qserv/scripts/install-stack.sh
# Hack to enable diagnose of stack install failure
RUN /bin/bash /qserv/scripts/install-stack.sh || echo 'INSTALL FAILED' > /qserv/STACK_INSTALL_ERROR

COPY scripts/*.sh /qserv/scripts/

# Keep access to root account
USER root

RUN chown -R qserv:qserv /qserv/scripts/

WORKDIR /qserv
