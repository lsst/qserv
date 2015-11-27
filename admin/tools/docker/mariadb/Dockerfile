FROM qserv/stack:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER root
RUN apt-get install -y git 

USER qserv
RUN mkdir -p /home/qserv/src
WORKDIR /home/qserv/src
RUN git clone https://github.com/fjammes/mariadb.git
WORKDIR /home/qserv/src/mariadb

# Install MariaDB dependencies
USER root
RUN apt-get install -y cmake
USER qserv
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . && eupspkg -er prep"

USER root
RUN apt-get install -y libncurses5-dev
USER qserv
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . && eupspkg -er config"

USER root
RUN apt-get install -y liblzma-dev
USER qserv
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . && eupspkg -er build"
RUN bash -c ". /qserv/stack/loadLSST.bash && setup -r . && eupspkg -er install"
