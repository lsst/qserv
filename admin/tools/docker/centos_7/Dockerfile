FROM centos:centos7
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
# Start with this long step not to re-run it on
# each Dockerfile update
COPY scripts/install-deps.sh /root/install-deps.sh
RUN /root/install-deps.sh

RUN groupadd -r qserv
RUN useradd -r -m -g qserv qserv
RUN mkdir /qserv
RUN chown qserv:qserv /qserv

USER qserv
COPY scripts/install-stack.sh /qserv/install-stack.sh
COPY scripts/mono-node-test.sh /qserv/mono-node-test.sh

# Keep access to root account
USER root

WORKDIR /qserv

# FIXME: development tools and system update
# should be performed elsewhere
RUN yum install --assumeyes vim byobu
# FIXME: Broken on Centos7
# RUN yum update --assumeyes
