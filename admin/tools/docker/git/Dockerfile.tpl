# dev and release are the same at release time
FROM <DOCKER_REPO>:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER root

COPY src/qserv /home/qserv/src/qserv
RUN chown -R qserv:qserv /home/qserv/src/qserv

USER qserv

WORKDIR /home/qserv

# * Update to latest qserv dependencies:
#   dependencies need to be publish on distribution server, see
#   https://confluence.lsstcorp.org/display/DM/Qserv+Release+Procedure
# * eupspkg command:
#   - builds Qserv
#   - installs Qserv inside LSST stack (i.e. /qserv/stack/Linux64/qserv/branch-version)
#   - declares it using 'qserv-dev' tag
RUN bash -c ". /qserv/stack/loadLSST.bash && \
    cp -r /home/qserv/src/qserv /tmp && \
    cd /tmp/qserv && \
    setup -r . -t qserv-dev && \
    eupspkg -er install && \
    eupspkg -er decl -t qserv-dev && \
    rm -rf /tmp/qserv"
