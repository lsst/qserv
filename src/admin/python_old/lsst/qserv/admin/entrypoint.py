# This file is part of qserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License

"""Launcher for Qserv components."""

import os
import subprocess

from lsst.qserv.qmeta import smig


czar_node_name = "master"
worker_node_name = "worker"
cmsd_conf_file = "/usr/local/qserv/configuration/xrootd.cf"
xrootd_conf_file = "/usr/local/qserv/configuration/xrootd.cf"
xrdssi_conf_file = "/usr/local/qserv/configuration/xrdssi.cnf"
lua_script_file = "/usr/local/lua/qserv/scripts/mysqlProxy.lua"
lua_cpath = "/usr/local/lua/qserv/lib/czarProxy.so"
mysql_proxy_cfg = "/usr/local/qserv/configuration/my-proxy.cnf"
config_folder = "/config-etc"

# Assume/require that the migration script directory structure reflect the
# directory structure under the `src` folder in the qserv source code. Callers
# must provide the path to the `src` folder, or equivalent folder in the
# installed location.
qmeta_scripts_dir = os.path.join(FILE_DIR, "qmeta", "schema")


def launchMysqld():
    """Launch mysqld.

    Looks for a configuration file provided in a mounted volume and if found
    creates a link from a location mysqld will search for config files to the
    mounted config file.
    """
    os.symlink(os.path.join(config_folder, "my.cnf"),
               "/etc/mysql/my.cnf")

    result = subprocess.run(args=["mysqld"])

    # Raises a CalledProcessError if return code is not 0. Is that how we want
    # to end it if things go bad here?
    result.check_returncode()


def launchMySqlProxy():
    result = subprocess.run(
        args=[
            "mysql-proxy",
            "--daemon",
            f"--proxy-lua-script={lua_script_file}",
            f"--lua-cpath={lua_cpath}",
            f"--defaults-file={mysql_proxy_cfg}",
        ]
    )
    # Raises a CalledProcessError if return code is not 0. Is that how we want
    # to end it if things go bad here?
    result.check_returncode()


def launchXrootd():
    """Launch xrootd.

    This is only expected to be called on workers.

    Parameters
    ----------
    name : `str`
        Typical use in qserv is either "master" or "worker"
    """

    # this command:
    # - must wait for a mysqld to be configured & running
    # - write some kind of worker id(s?) to a text file (VNID_FILE).
    #   TBD what that file is used for, maybe we don't need it.
    # - waits for xrootd redirector readyness
    # - finally, launches xrootd service, as below, but it needs to get run as
    #   the qserv user.
    # needs to run as the qserv user

    result = subprocess.run(
        args=[
            "xrootd",
            "-c",
            xrootd_conf_file,
            "-l",
            "@libXrdSsiLog.so",  # direct messages to the plugin in this shared library
            "-n",
            "worker",  # the name of this xrootd instance
            "-I",
            "v4",  # restrict to hosts with IPV4 addresses
            "-+xrdssi",
            xrdssi_conf_file,  # pass command line argument to plugin
        ]
    )

    # Raises a CalledProcessError if return code is not 0. Is that how we want
    # to end it if things go bad here?
    result.check_returncode()


def launchCmsd(name):
    """Launch cmsd.

    Parameters
    ----------
    name : `str`
        Typical use in qserv is either "master" or "worker"
    """
    result = subprocess.run(
        args=[
            "cmsd",
            "-c",
            xrootd_conf_file,
            "-l",
            "@libXrdSsiLog.so",  # direct messages to the plugin in this shared library
            "-n",
            name,  # the name of this xrootd instance
            "-I",
            "v4",  # restrict to hosts with IPV4 addresses
            "-+xrdssi",
            xrdssi_conf_file,  # pass command line argument to plugin
        ]
    )

    # Raises a CalledProcessError if return code is not 0. Is that how we want
    # to end it if things go bad here?
    result.check_returncode()


def launchWatcher():
    # what container does this run in?

    result = subprocess.run(
        args=[
            # todo these args need fixing
            # "python3",
            # "watcher.py",  # do we need to pass a folder? is it on the path?
            # "-c",
            # "/qserv/run/etc/qserv-watcher.cnf",
            # "-v",  # we still want verbose?
        ]
    )

    # Raises a CalledProcessError if return code is not 0. Is that how we want
    # to end it if things go bad here?
    result.check_returncode()


def launchWmgr():
    result = subprocess.run(
        args=[
            # todo these args need fixing
            # "python3",
            # "qservWmgr.py",  # do we need to pass a folder?
            # "-c",
            # "/qserv/run/etc/qserv-wmgr.cnf",
            # "-v",
        ]
    )

    # Raises a CalledProcessError if return code is not 0. Is that how we want
    # to end it if things go bad here?
    result.check_returncode()


def smigQmeta(connection, scriptsBaseDir):
    """Apply qmeta schema migration scripts to a database.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database.
    scriptsBaseDir : `str`
        The path to the top level migration sources folder. Will look for
        migration scripts below this folder at the location defined by
        qmeta_scripts_dir in this file.
    """
    smig(
        verbose=False,
        do_migrate=True,
        check=False,
        final=None,
        scripts=os.path.join(scriptsBaseDir, qmeta_scripts_dir),
        connection,
        config_file=None, # could use this instead of connection string
        config_section=None, # goes with config_file
        module="qmeta",
    )


def smigCzar(connection, scriptsBaseDir):
    smigQmeta()


def enterCzar():
    smigCzar(connection, scriptsBaseDir)
    launchXrootd(czar_node_name)
    launchCmsd(czar_node_name)


def enterWorker():
    launchXrootd(worker_node_name)
    launchCmsd(czar_node_name)


def enterMariadb():
    launchMaraidb()
    launchMysqld()
    # launchWatcher?
    # launchWmgr?


if __name__ == "__main__":
    main()
