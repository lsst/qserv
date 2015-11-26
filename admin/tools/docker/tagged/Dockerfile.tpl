FROM qserv/qserv:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER root

RUN apt-get update && apt-get -y install byobu git vim gdb lsof

USER qserv 

WORKDIR /home/qserv

RUN mkdir src && cd src && git clone -b {{GIT_TAG_OPT}} --single-branch https://github.com/LSST/qserv
WORKDIR /home/qserv/src/qserv
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && eupspkg -er build"
# Force execution of lines below by changing timestamp
RUN git pull # Build performed on {{TIMESTAMP}}
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && eupspkg -er install"
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && eupspkg -er decl -t qserv-dev"
