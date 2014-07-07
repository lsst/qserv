=====
Goals
=====

In 2014, Qserv installation procedure was splitted in next steps :
- download, build and install using eups,
- configuration.

eups install directory must only contains immutable data like binaries.
That's why Qserv configuration tool will create a new directory which will
contains :
- configuration files and data,
- sky data,
- execution informations (pid files, log files).

Thereafter, this directory will be called QSERV_EXEC_DIR.

This will allow to switch Qserv version (using eups), without having to
re-configure Qserv from scratch.

========
Approach
========

The configuration tool is written in pure-python and offer many options.
It doesn't on LSST standards as it seems nothing is yet provided for
configuration.

========
Features
========

- a -a option allow to create QSERV_EXEC_DIR from scratch 
- default procedure doesn't remove QSERV_EXEC_DIR but only update it
- runs mysql, scisql and xrootd configuration scripts

=========
Prospects
=========

- check for configuration compatibility between Qserv versions (see https://jira.lsstcorp.org/browse/DM-895)
in order to switch safely Qserv version for a given QSERV_EXEC_DIR. 

