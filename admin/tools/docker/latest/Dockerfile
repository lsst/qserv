FROM lsstsqre/centos:7-stackbase-devtoolset-6
LABEL maintainer="Fabrice Jammes <fabrice.jammes@in2p3.fr>"

RUN sh -c "echo \"BUILD ID: $(date '+%Y%m%d_%H%M%S')\" > /BUILD_ID"

# Provide newer git for newinstall and eupspkg
#
RUN yum install -y rh-git29 && \
    yum clean all && \
    echo ". /opt/rh/rh-git29/enable" > "/etc/profile.d/rh-git29.sh"

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

ARG EUPS_TAG

# Install development and debugging tools
#
RUN if [ "$EUPS_TAG" = "qserv-dev" ] ; then \
        yum install -y \
            byobu \
            dnsutils \
            gdb \
            lsof \
            net-tools \
        && yum clean all; \
    fi

USER qserv

# Install LSST stack
#
ENV STACK_DIR /qserv/stack
RUN bash -lc "mkdir $STACK_DIR && cd $STACK_DIR && \
              curl -OL \
              https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh && \
              bash newinstall.sh -bt"

RUN bash -lc ". $STACK_DIR/loadLSST.bash && eups distrib install pytest -t '$EUPS_TAG'"

RUN bash -lc ". $STACK_DIR/loadLSST.bash && \
              curl -sSL https://raw.githubusercontent.com/lsst/shebangtron/master/shebangtron | python"

RUN bash -lc ". $STACK_DIR/loadLSST.bash && eups distrib install qserv_distrib -t '$EUPS_TAG'"

COPY scripts/*.sh /qserv/scripts/

WORKDIR /home/qserv

RUN mkdir src

ENV QSERV_RUN_DIR /qserv/run

# Generate /qserv/run/sysconfig/qserv and /qserv/run/etc/init.d/qserv-functions
# required by k8s setup
RUN bash -lc ". /qserv/stack/loadLSST.bash && \
              setup qserv -t qserv-dev && \
              cp \"\$SCISQL_DIR\"/lib/libscisql-scisql_?.?.so \"\$MARIADB_DIR\"/lib/plugin && \
              qserv-configure.py --init --force --qserv-run-dir \"$QSERV_RUN_DIR\" && \
              qserv-configure.py --etc --qserv-run-dir \"$QSERV_RUN_DIR\" --force && \
              rm $QSERV_RUN_DIR/qserv-meta.conf"

# Allow install of additional packages in pods and ease install scripts
# execution
USER root
