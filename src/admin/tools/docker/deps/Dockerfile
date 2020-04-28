FROM lsstdm/scipipe-base:7

# Install Qserv prerequisites
#
RUN yum install -y \
        initscripts \
        lsof \
    && yum clean all

RUN groupadd qserv && \
    useradd -m -g qserv qserv && \
    usermod -s /bin/bash qserv && \
    mkdir /qserv && \
    chown qserv:qserv /qserv

COPY ca-trust/* /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust

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
