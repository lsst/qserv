FROM <DOCKER_IMAGE>
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

# Allow the start.sh script to modify the local timezone settings
# if requested.

USER root

RUN chown qserv:qserv /etc/localtime && chown qserv:qserv /etc/timezone

WORKDIR /qserv

USER qserv

# Respectively xrootd, qserv-wmanager ports
# Used on both worker and master
EXPOSE 1094 5012

# Respectively cmsd, mysql-proxy ports
# Used on master only
<COMMENT_ON_WORKER>EXPOSE 2131 4040

COPY scripts/*.sh scripts/

RUN bash -c ". /qserv/stack/loadLSST.bash && setup qserv -t qserv-dev && /qserv/scripts/configure.sh <NODE_TYPE>"

# WARNING: Unsafe because it is pushed in Docker Hub
# TODO: use consul to manage secret
COPY wmgr.secret /qserv/run/etc/wmgr.secret.example

# Set this environment variable to true to set timezone on container start.
ENV SET_CONTAINER_TIMEZONE false

# Default container timezone as found under the directory /usr/share/zoneinfo/.
ENV CONTAINER_TIMEZONE Etc/UTC

# This script does not exit
CMD /qserv/scripts/start.sh
