FROM lsstdm/scipipe-base:7

USER 0

# Install LSST stack
ENV STACK_DIR /stack
RUN bash -lc "mkdir $STACK_DIR && cd $STACK_DIR && \
             curl -OL https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh && \
             LSST_SPLENV_REF=ceb6bb6 bash newinstall.sh -b"
ENV EUPS_TAG qserv-dev
RUN /bin/bash -lc ". $STACK_DIR/loadLSST.bash && eups distrib install qserv_distrib -t $EUPS_TAG"
