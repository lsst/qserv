# dev and release are the same at release time
FROM qserv/qserv:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER root

COPY src/qserv /home/qserv/src/qserv
RUN chown -R qserv:qserv /home/qserv/src/qserv

USER qserv 

WORKDIR /home/qserv/src/qserv

# Update to latest qserv dependencies, need to be publish on distribution server
# see https://confluence.lsstcorp.org/display/DM/Qserv+Release+Procedure
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . -t qserv-dev && scons install" 
RUN bash -c ". /qserv/stack/loadLSST.bash && eups declare -r . -t qserv-dev" 

