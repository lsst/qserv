FROM <DOCKER_REPO>:dev
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>

USER root

RUN groupadd dev && useradd -m -g dev -G qserv dev && usermod -s /bin/bash dev

COPY scripts/startup.py /home/dev/.eups/
COPY scripts/*.sh /home/dev/scripts/

# 1st chmod: Allow dev user to set eups lock
# 2nd : Workaround to install scisql plugin in mysql
RUN chown -R dev:dev /home/dev/.eups && \
    chown -R dev:dev /home/dev/scripts && \
    chmod g+w /qserv/stack/ && \
    chmod -R g+w /qserv/stack/Linux64/mariadb/*/lib/plugin

USER dev

WORKDIR /home/dev

# Source code should be mounted from host to /home/dev/qserv using 'docker run -v'.

# Having a initial qserv directory here will allow to performs 'eups declare -r ~/src/qserv -t git'.
# But eups database doesn't seems to support uid/gid changes, so this command
# has to be delayed and performed at the end of change-uid.sh
RUN mkdir -p src/qserv/ups
RUN touch src/qserv/ups/qserv.table
