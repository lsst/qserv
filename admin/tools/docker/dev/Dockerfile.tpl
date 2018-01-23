# dev and release are the same at release time
FROM <DOCKER_REPO>:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER qserv

# Update to latest qserv dependencies, need to be publish on distribution server
# see https://confluence.lsstcorp.org/display/DM/Qserv+Release+Procedure
RUN bash -c ". /qserv/stack/loadLSST.bash && eups distrib install qserv_distrib -t qserv-dev -vvv"

# Generate /qserv/run/sysconfig/qserv and /qserv/run/etc/init.d/qserv-functions
# required by k8s setup
RUN bash -c ". /qserv/stack/loadLSST.bash && \
             setup qserv -t qserv-dev && \
             qserv-configure.py --init --force --qserv-run-dir "$QSERV_RUN_DIR" && \
             qserv-configure.py --etc --qserv-run-dir "$QSERV_RUN_DIR" --force "

# Allow install of additional packages in pods and ease install scripts
# execution
USER root
