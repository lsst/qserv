FROM {{DOCKER_IMAGE_OPT}}
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

WORKDIR /qserv

USER qserv

RUN bash -c ". /qserv/stack/loadLSST.bash && setup qserv -t qserv-dev && /qserv/scripts/configure.sh {{NODE_TYPE_OPT}} {{MASTER_FQDN_OPT}}"

# WARNING: Unsafe because it is pushed in Docker Hub
# TODO: use consul to manage secret
COPY wmgr.secret /qserv/run/etc/

# This script does not exit 
CMD /qserv/scripts/qserv-start.bash
