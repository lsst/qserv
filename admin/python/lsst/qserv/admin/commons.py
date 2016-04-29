# LSST Data Management System
# Copyright 2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
Commons functions for Qserv administration tools

@author  Fabrice Jammes, IN2P3

"""
from __future__ import absolute_import

# --------------------------------
#  Imports of standard modules --
# -------------------------------
import os
import hashlib
import getpass
import logging
import re
import subprocess
import sys
import ConfigParser

# ----------------------------
# Imports for other modules --
# ----------------------------

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

# -----------------------
# Exported definitions --
# -----------------------

# Qserv service states
NO_STATUS_SCRIPT = -1
DOWN = 127
UP = 0

config = dict()

def read_user_config():
    config_file = os.path.join(os.getenv("HOME"), ".lsst", "qserv.conf")
    _LOG.debug("Read user config file: %r", config_file)
    config = read_config(config_file)
    return config


def read_config(config_file):

    _LOG.debug('Reading config file %r' % config_file)
    if not os.path.isfile(config_file):
        _LOG.fatal("qserv configuration file not found: %r" % config_file)
        exit(1)

    parser = ConfigParser.SafeConfigParser()
    # parser.readfp(io.BytesIO(const.DEFAULT_CONFIG))
    parser.read(config_file)

    _LOG.debug("Build configuration : ")
    for section in parser.sections():
        _LOG.debug("===")
        _LOG.debug("[%r]" % section)
        _LOG.debug("===")
        config[section] = dict()
        for option in parser.options(section):
            _LOG.debug("%r = %r" % (option, parser.get(section, option)))
            config[section][option] = parser.get(section, option)

    # normalize directories names
    for section in config.keys():
        for option in config[section].keys():
            if re.match(".*_dir", option):
                config[section][option] = os.path.normpath(
                    config[section][option])

    # TODO : manage special characters for pass (see config file comments for
    # additional information)
    config['mysqld']['pass'] = parser.get("mysqld", "pass", raw=True)
    if parser.has_option('mysqld', 'port'):
        config['mysqld']['port'] = parser.getint('mysqld', 'port')

    config['mysql_proxy']['port'] = parser.getint('mysql_proxy', 'port')

    return config


def getConfig():
    return config

def restart(service_name):
    config = getConfig()
    if len(config) == 0:
        raise RuntimeError("Qserv configuration is empty")
    initd_path = os.path.join(config['qserv']['qserv_run_dir'], 'etc', 'init.d')
    daemon_script = os.path.join(initd_path, service_name)
    out = os.system("%s stop" % daemon_script)
    out = os.system("%s start" % daemon_script)

def status(qserv_run_dir):
    """
    Check if Qserv services are up
    @param qserv_run_dir: Qserv run directory of Qserv instance to ckeck
    @return: the exit code of qserv-status.sh, i.e.:
             id status file doesn't exists: -1 (NO_STATUS_SCRIPT)
             if all Qserv services are up:   0 (UP)
             if all Qserv services are down: 255 (DOWN)
             else the number of stopped Qserv services
    """
    script_path = os.path.join(qserv_run_dir, 'bin', 'qserv-status.sh')
    if not os.path.exists(script_path):
        return NO_STATUS_SCRIPT
    with open(os.devnull, "w") as fnull:
        retcode = subprocess.call([script_path], stdout=fnull, stderr=fnull, shell=False)
    return retcode

def run_command(cmd_args, stdin_file=None, stdout=None, stderr=None,
                loglevel=logging.INFO):
    """
    Run a shell command
    @cmdargs  command arguments
    @stdin    can be a filename, or None
    @stdout   can be sys.stdout, a filename, or None
              which redirect to current processus output
    @stderr   same as stdout
    @loglevel print stdin, stdout and stderr if current module logger
              verbosity is greater than loglevel
    """

    cmd_str = ' '.join(cmd_args)
    _LOG.log(loglevel, "Run shell command: {0}".format(cmd_str))

    sin = None
    if stdin_file:
        _LOG.log(loglevel, "stdin file: %r" % stdin_file)
        sin = open(stdin_file, "r")

    sout = None
    if stdout==sys.stdout:
        sout=sys.stdout
    elif stdout:
        _LOG.log(loglevel, "stdout file: %r" % stdout)
        sout = open(stdout, "w")
    else:
        sout = subprocess.PIPE

    serr = None
    if stderr==sys.stderr:
        serr=sys.stderr
    elif stderr:
        _LOG.log(loglevel, "stderr file: %r" % stderr)
        serr = open(stderr, "w")
    else:
        serr = subprocess.PIPE

    try:
        process = subprocess.Popen(
            cmd_args, stdin=sin, stdout=sout, stderr=serr
        )

        (stdoutdata, stderrdata) = process.communicate()

        if stdoutdata != None and len(stdoutdata) > 0:
            _LOG.info("\tstdout :\n--\n%r--" % stdoutdata)
        if stderrdata != None and len(stderrdata) > 0:
            _LOG.info("\tstderr :\n--\n%r--" % stderrdata)

        if process.returncode != 0:
            _LOG.fatal(
                "Error code returned by command : {0} ".format(cmd_str))
            sys.exit(1)

    except OSError as e:
        _LOG.fatal("Error: %r while running command: %r" %
                   (e, cmd_str))
        sys.exit(1)
    except ValueError as e:
        _LOG.fatal("Invalid parameter: %r for command: %r" %
                   (e, cmd_str))
        sys.exit(1)
