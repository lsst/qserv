FROM fjammes/qserv:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER qserv

WORKDIR /qserv

RUN /bin/bash /qserv/scripts/configure.sh {{NODE_TYPE_OPT}} {{MASTER_FQDN_OPT}}

CMD ["/qserv/run/bin/qserv-start.sh"]
