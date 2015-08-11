FROM fjammes/qserv:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apt-get -y install byobu
RUN apt-get -y install git 
RUN apt-get -y install vim 

RUN groupadd dev
RUN useradd -m -g dev -G qserv dev
RUN usermod -s /bin/bash dev 

# Allow dev user to set log 
RUN chmod g+w /qserv/stack/
# Workaround to install scisql plugin in mysql
RUN chmod -R g+w /qserv/stack/Linux64/mysql

COPY scripts/startup.py /home/dev/.eups/
COPY scripts/*.sh /home/dev/scripts/

RUN  chown -R dev:dev /home/dev/.eups
RUN  chown -R dev:dev /home/dev/scripts

USER dev

RUN mkdir /home/dev/src

# Source code should be mounted using 'docker run -v'
RUN git clone git://github.com/LSST/qserv /home/dev/src/qserv

# Keep access to root account
USER root

WORKDIR /home/dev
