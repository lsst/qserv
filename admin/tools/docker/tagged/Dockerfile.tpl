FROM qserv/qserv:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER root

RUN apt-get update && apt-get -y install byobu git vim gdb lsof

USER qserv 

ENV QSERV_SRC_DIR /home/qserv/src/qserv

# Force execution of lines below by changing timestamp
RUN git clone -b {{GIT_TAG_OPT}} --single-branch https://github.com/LSST/qserv $QSERV_SRC_DIR # Build performed on {{TIMESTAMP}}
WORKDIR $QSERV_SRC_DIR
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && eupspkg -er install"
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && eupspkg -er decl -t qserv-dev"
