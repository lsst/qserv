***********************
Configuration procedure
***********************

This documentation presents Qserv configuration tool, designed to simplify and automate Qserv installation process.

Goals
=====

Qserv installation procedure consists of next steps :

.. _eups https://github.com/RobertLuptonTheGood/eups

- download, build and install Qserv using eups_,
- configure Qserv.

The goal is to have a modular procedure in order to ease future evolutions and maintenance.
That's why Qserv configuration procedure must be as independant as possible of eups and other steps eups.

Approach
========

eups install directory must only contains immutable data like, for example, binaries.
That's why Qserv configuration tool creates two directories:

- QSERV_RUN_DIR which contains configuration (file and data) and execution informations (pid files, log files)
- QSERV_DATA_DIR which contains scientific data (i.e. sky data),

The configuration tool is written in pure-python and offers several options (see --help).

.. code-block:: bash

  qserv-configure.py --help

Features
========

- Use meta-configuration file QSERV_RUN_DIR/qserv-meta.conf, and templates in QSERV_DIR/cfg/templates to generate all configuration
- Run configuration scripts for Qserv services when required (i.e. for mysql, scisql and xrootd)
- Protect existing data, if located outside of configuration directory
- Create client configuration in ~/.lsst

Tickets completed
=================

- https://jira.lsstcorp.org/browse/DM-622
- https://jira.lsstcorp.org/browse/DM-930
- https://jira.lsstcorp.org/browse/DM-595
- https://jira.lsstcorp.org/browse/DM-895
- https://jira.lsstcorp.org/browse/DM-2595

