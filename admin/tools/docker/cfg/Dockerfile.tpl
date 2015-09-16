FROM fjammes/qserv:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER qserv

WORKDIR /qserv

RUN /qserv/scripts/configure.sh {{NODE_TYPE_OPT}} {{MASTER_FQDN_OPT}}

# 'tail -F' allow container not to exit
CMD /qserv/run/bin/qserv-start.sh && tail -F /qserv/run/var/log/worker/xrootd.log
