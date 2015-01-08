#!/bin/bash

# Supported version :
# lsb_release -a
# LSB Version:
# :core-4.1-amd64:core-4.1-noarch:cxx-4.1-amd64:cxx-4.1-noarch:desktop-4.1-amd64:desktop-4.1-noarch:languages-4.1-amd64:languages-4.1-noarch:printing-4.1-amd64:printing-4.1-noarch
# Distributor ID: Fedora
# Description:    Fedora release 19 (Schrödinger’s Cat)
# Release:        19
# Codename:       Schrödinger’sCat


shopt -s expand_aliases
alias yum="yum -y"

#
# Scientific Linux 6 dependencies
#

# sconsUtils
yum install scons gettext flex bison


# data partitioning dependency
yum install numpy
            
# redhat-lsb
yum install redhat-lsb perl-ExtUtils-MakeMaker

# eups
yum install patch bzip2 bzip2-devel

# xrootd
yum install gcc gcc-c++ git zlib-devel cmake

# zope_interface
yum install python-devel

# mysql
yum install ncurses-devel glibc-devel

# qserv
yum install boost-devel openssl-devel java

# lua
yum install readline-devel

# mysql-proxy
yum install glib2-devel

# css
yum install python-argparse
