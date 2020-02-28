FROM lsstsqre/centos:7-stackbase-devtoolset-8

# Provide newer git for newinstall and eupspkg
#
RUN yum install -y rh-git218 devtoolset-8-toolchain lsof && \
    yum clean all && \
    echo ". /opt/rh/rh-git218/enable" > "/etc/profile.d/rh-git218.sh"


# Install Qserv prerequisites
#
RUN yum install -y \
        initscripts \
    && yum clean all

RUN groupadd qserv && \
    useradd -m -g qserv qserv && \
    usermod -s /bin/bash qserv && \
    mkdir /qserv && \
    chown qserv:qserv /qserv


USER qserv

COPY scripts/*.sh /qserv/scripts/

# Install LSST stack
#

ENV STACK_DIR /qserv/stack

RUN ["/bin/bash", "-lc", "/qserv/scripts/newinstall.sh"]

ENV EUPS_TAG qserv-dev
RUN ["/bin/bash", "-lc", "/qserv/scripts/installqserv.sh"]

WORKDIR /home/qserv

RUN mkdir src

ENV QSERV_RUN_DIR /qserv/run

# Generate /qserv/run/sysconfig/qserv and /qserv/run/etc/init.d/qserv-functions
# required by k8s setup
RUN ["/bin/bash", "-lc", "/qserv/scripts/initconfig.sh"]

# Allow install of additional packages in pods and ease install scripts
# execution
USER root

ARG DEPS_TAG
RUN echo "BUILD ID: $DEPS_TAG"
