***********************
Configuration procedure
***********************

Goals
=====

Qserv installation procedure consists of next steps :

- download, build and install Qserv using eups,
- configure Qserv.

The goal is to have a modular procedure in order to ease future evolutions and maintenance.
That's why Qserv configuration procedure must be as independant as possible of eups and other steps eups.

Approach
========

eups install directory must only contains immutable data like, for example, binaries.
That's why Qserv configuration tool create a separate directory which contains :

- configuration files and data,
- execution informations (pid files, log files).
- business data (i.e. sky data),

Thereafter, this directory will be called QSERV_RUN_DIR.
This method will allow to switch Qserv version (using eups for example), without having to re-configure Qserv from scratch.
The configuration tool is written in pure-python and offers many options (see --help) in order to be very flexible. Nevertheless it doesn't relies on LSST standards (it seems nothing is yet provided for configuration).
The default procedure aims to be straightforward, so that new users can quickly set up a standard mono-node Qserv configuration. 

Features
========

- creates services configuration files in QSERV_RUN_DIR/etc/qserv.conf using a meta-configuration file in QSERV_RUN_DIR/qserv.conf, 
- runs mysql, scisql and xrootd configuration scripts,
- creates client configuration,
- default procedure doesn't remove QSERV_RUN_DIR but only update it,
- '-all' option allow to create QSERV_RUN_DIR from scratch.

Tickets completed
=================

- https://jira.lsstcorp.org/browse/DM-622
- https://jira.lsstcorp.org/browse/DM-930

Prospects
=========

This procedure should also be able to configure a multi-node instance, see https://jira.lsstcorp.org/browse/DM-595

Migration
=========

see https://jira.lsstcorp.org/browse/DM-895

- check for configuration compatibility between Qserv versions in order to switch safely Qserv versions for a given QSERV_RUN_DIR.
- create a migration procedure

Output
======

see https://jira.lsstcorp.org/browse/DM-954

- produce a clearer output
