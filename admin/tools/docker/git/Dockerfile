# dev and release are the same at release time
FROM qserv/qserv:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

COPY src/qserv_src.tar /tmp/

USER qserv 

RUN mkdir -p /home/qserv/src/qserv
WORKDIR /home/qserv/src/qserv

RUN tar --directory . -xf /tmp/qserv_src.tar

# Update to latest qserv dependencies, need to be publish on distribution server
# see https://confluence.lsstcorp.org/display/DM/Qserv+Release+Procedure
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && scons install" 
RUN bash -c ". /qserv/stack/loadLSST.bash && eups declare -r . -t qserv-dev" 

