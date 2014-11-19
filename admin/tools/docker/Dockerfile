FROM debian:stable
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
RUN apt-get update
RUN apt-get install -y git
RUN groupadd -r qserv
RUN useradd -r -m -g qserv qserv
RUN apt-get --yes install bash \
    bison \
    bzip2 \
    cmake \
    curl \
    flex \
    g++ \
    gettext \
    libbz2-dev \
    libglib2.0-dev \
    libpthread-workqueue-dev \
    libreadline-dev \
    make \
    ncurses-dev \
    openjdk-7-jre-headless \
    openssl \
    python-dev \
    python-numpy \
    python-setuptools \
    zlib1g-dev
# Should be removed: cf. DM-1516
RUN apt-get -y install swig
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
RUN bash -c ". loadLSST.bash && eups distrib install qserv_distrib -t qserv"
# Mono-node configuration and tests
# RUN bash -c ". loadLSST.bash && setup qserv -t qserv && qserv-configure.py \
# --all -R $HOME/qserv-run"
# RUN bash -c ". loadLSST.bash && setup qserv_testdata -t qserv && \
# $HOME/qserv-run/bin/qserv-start.sh && qserv-test-integration.py && \
# $HOME/qserv-run/bin/qserv-stop.sh"
