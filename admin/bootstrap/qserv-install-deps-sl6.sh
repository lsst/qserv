#!/bin/bash

shopt -s expand_aliases
alias yum="yum -y"

#
# Scientific Linux 6 dependencies
#
yum install scons gettext

# data partitioning dependency
yum install numpy
            
# redhat-lsb ?
yum install perl-ExtUtils-MakeMaker

# eups
yum install patch bzip2 bzip2-devel

# xrootd
yum install gcc gcc-c++ git zlib-devel cmake

# zope_interface
yum install python-devel

# mysql
yum install ncurses-devel glibc-devel

# qserv
yum install boost-devel openssl-devel antlr swig java

# lua
yum install readline-devel

# mysql-proxy
yum install glib2-devel

