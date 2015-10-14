FROM fjammes/qserv:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER qserv

WORKDIR /qserv

RUN /qserv/scripts/configure.sh {{NODE_TYPE_OPT}} {{MASTER_FQDN_OPT}}

# WARNING: Unsafe because it is pushed in Docker Hub
# TODO: use consul to manage secret
COPY wmgr.secret /qserv/run/etc/

# 'tail -F' allow container not to exit
CMD /qserv/run/bin/qserv-start.sh && tail -F /qserv/run/var/log/worker/xrootd.log
