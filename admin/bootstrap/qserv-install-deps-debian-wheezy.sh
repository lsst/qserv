#!/bin/sh

apt-get --yes install bash \
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
    python-setuptools \
    zlib1g-dev

# deprecated: remove in 2015_01
apt-get --yes install swig python-numpy
