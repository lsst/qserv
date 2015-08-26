FROM fjammes/qserv:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

RUN apt-get update && apt-get -y install byobu git vim

RUN groupadd dev && useradd -m -g dev -G qserv dev && usermod -s /bin/bash dev

COPY scripts/startup.py /home/dev/.eups/
COPY scripts/*.sh /home/dev/scripts/

# 1st chmod: Allow dev user to set eups lock
# 2nd : Workaround to install scisql plugin in mysql
RUN chown -R dev:dev /home/dev/.eups && \
    chown -R dev:dev /home/dev/scripts && \
    chmod g+w stack/ && \
    chmod -R g+w stack/Linux64/mysql

USER dev

WORKDIR /home/dev
RUN mkdir src

# Source code should be mounted using 'docker run -v'
RUN git clone git://github.com/LSST/qserv src/qserv
