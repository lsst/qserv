FROM debian:jessie
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
RUN apt-get clean
RUN apt-get update
ADD install-deps.sh /tmp/install-deps.sh
RUN /tmp/install-deps.sh
# development tools
RUN apt-get install -y vim byobu
RUN groupadd -r qserv
RUN useradd -r -m -g qserv qserv
USER qserv
ENV HOME /home/qserv
WORKDIR /home/qserv
COPY scripts/install-stack.sh install-stack.sh 
COPY scripts/mono-node-test.sh mono-node-test.sh
