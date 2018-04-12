# dev and release are the same at release time
FROM <DOCKER_REPO>:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER qserv

# Update to latest qserv dependencies, need to be publish on distribution server
# see https://confluence.lsstcorp.org/display/DM/Qserv+Release+Procedure
RUN bash -c ". /qserv/stack/loadLSST.bash && eups distrib install qserv_distrib -t qserv-dev -vvv"

ENV QSERV_RUN_DIR /qserv/run

# Allow install of additional packages in pods and ease install scripts
# execution
USER root
