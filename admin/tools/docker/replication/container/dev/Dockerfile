FROM lsstsqre/centos:7-stackbase-devtoolset-6

USER 0

# Provide newer git for newinstall and eupspkg
RUN yum install -y rh-git29 && yum clean all && \
    echo ". /opt/rh/rh-git29/enable" > "/etc/profile.d/rh-git29.sh"

# Install Qserv prerequisites
RUN yum install -y initscripts && yum clean all

# Install LSST stack
ENV EUPS_TAG qserv-dev
ENV STACK_DIR /stack
RUN bash -lc "mkdir $STACK_DIR && cd $STACK_DIR && \
              curl -OL \
              https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh && \
              bash newinstall.sh -bt"

RUN bash -lc ". $STACK_DIR/loadLSST.bash && eups distrib install pytest -t '$EUPS_TAG'"

RUN bash -lc ". $STACK_DIR/loadLSST.bash && \
              curl -sSL https://raw.githubusercontent.com/lsst/shebangtron/master/shebangtron | python"

RUN bash -lc ". $STACK_DIR/loadLSST.bash && eups distrib install qserv_distrib -t '$EUPS_TAG'"
