#!/bin/bash

shopt -s expand_aliases
alias yum="yum -y"

yum install scons

# Scientific Linux 6 dependencies
# data partitioning dependency
yum install numpy;

# xrootd
yum install gcc-c++ git zlib-devel

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

