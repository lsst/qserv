# dev and release are the same at release time
FROM qserv/qserv:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER root
RUN apt-get -y update && apt-get -y install byobu dnsutils \
    gdb git lsof \
    net-tools vim

USER qserv
WORKDIR /home/qserv
# Update to latest qserv dependencies, need to be publish on distribution server
# see https://confluence.lsstcorp.org/display/DM/Qserv+Release+Procedure
RUN bash -c ". /qserv/stack/loadLSST.bash && eups distrib install qserv_distrib -t qserv-dev -vvv"

