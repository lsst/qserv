FROM lsstsqre/centos:7-stackbase-devtoolset-8

USER 0

# Provide newer git for newinstall and eupspkg
RUN yum install -y rh-git29 devtoolset-8-toolchain lsof && yum clean all && \
    source scl_source enable rh-git29

# Install Qserv prerequisites
RUN yum install -y initscripts && yum clean all

# Install LSST stack
ENV STACK_DIR /stack
RUN bash -lc "mkdir $STACK_DIR && cd $STACK_DIR && \
             curl -OL https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh && \
             bash newinstall.sh -bt"
ENV EUPS_TAG qserv-dev
RUN /bin/bash -lc ". $STACK_DIR/loadLSST.bash && eups distrib install qserv_distrib -t $EUPS_TAG"
