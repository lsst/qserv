FROM {{DOCKER_IMAGE_OPT}}
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

WORKDIR /qserv

USER qserv

# Respectively qserv-watcher xrootd ports
# Used on both worker and master
EXPOSE 5012 1094

# Respectively cmsd mysql-proxy ports
# Used on master only
{{COMMENT_ON_WORKER_OPT}}EXPOSE 2131 4040

COPY scripts/*.sh scripts/

RUN bash -c ". /qserv/stack/loadLSST.bash && setup qserv -t qserv-dev && /qserv/scripts/configure.sh {{NODE_TYPE_OPT}}"

# WARNING: Unsafe because it is pushed in Docker Hub
# TODO: use consul to manage secret
COPY wmgr.secret /qserv/run/etc/wmgr.secret.example

# This script does not exit
CMD /qserv/scripts/start.sh
