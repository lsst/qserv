# Using the development toolchain
FROM centos/devtoolset-6-toolchain-centos7

USER 0

RUN curl -sO http://www.slac.stanford.edu/exp/lsst/qserv/2016_09/_downloads/qserv-install-deps-rhel7.sh
RUN chmod +x qserv-install-deps-rhel7.sh
RUN /bin/bash qserv-install-deps-rhel7.sh

RUN curl -sO https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh
RUN chmod +x newinstall.sh

RUN mkdir -p /stack
RUN cd /stack && /bin/bash $HOME/newinstall.sh -b

RUN source /stack/loadLSST.bash && eups distrib install --tag qserv-dev qserv_distrib
