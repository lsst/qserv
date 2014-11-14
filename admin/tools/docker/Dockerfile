FROM debian:stable
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
RUN apt-get update
RUN apt-get install -y git
RUN groupadd -r qserv
RUN useradd -r -m -g qserv qserv
RUN apt-get -y install python-numpy g++ zlib1g-dev python-dev \
    ncurses-dev openssl libreadline-dev \
    libglib2.0-dev cmake bison bzip2 curl flex gettext libbz2-dev \
    make openjdk-7-jre-headless python-setuptools libpthread-workqueue-dev
# development tools
RUN apt-get -y install vim byobu
USER qserv
ENV HOME /home/qserv
RUN mkdir /home/qserv/stack
WORKDIR /home/qserv/stack
RUN curl -O "https://sw.lsstcorp.org/eupspkg/newinstall.sh"
# Running newinstall in batch mode should remove above line: DM-1495
RUN GIT=yes; ANACONDA=no; printf "$GIT\n$ANACONDA\n" > /tmp/answers.txt
# LSST stack require bash
RUN bash newinstall.sh < /tmp/answers.txt
RUN rm /tmp/answers.txt
RUN bash -c ". loadLSST.bash; eups distrib install qserv_distrib -t qserv"
