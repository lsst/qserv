#!/usr/bin/env python

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
Logging module for Qserv administration tools

@author  Fabrice Jammes, IN2P3

"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import logging.config
import os

# ----------------------------
# Imports for other modules --
# ----------------------------

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

# -----------------------
# Exported definitions --
# -----------------------


def get_default_log_conf():
    default_log_conf = "{0}/.lsst/logging.ini".format(os.path.expanduser('~'))
    return default_log_conf


def add_logfile_opt(parser):
    """
    Add option to command line interface in order to set path to standard
    configuration file for python logger
    Input object isn't duplicated here.
    @param parser: a parser which may contains some options (mutable object)
    @type: argparse.ArgumentParser
    @rtype : argparse.ArgumentParser
    """

    parser.add_argument("-V", "--log-cfg", dest="log_conf",
                        default=get_default_log_conf(),
                        help="Absolute path to file containing python" +
                        "logger standard configuration file")
    return parser


def setup_logging(path='logging.ini',
                  default_level=logging.INFO):
    """
    Setup logging configuration from yaml file
    if the yaml file doesn't exists:
    - return false
    - configure logging to default_level
    """
    if os.path.exists(path):
        with open(path, 'r') as f:
            logging.config.fileConfig(f)
        return True
    else:
        logging.basicConfig(level=default_level)
        return False


def init_default_logger(log_file_prefix, level=logging.DEBUG, log_path="."):
    if level == logging.DEBUG:
        fmt = '%(asctime)s {%(pathname)s:%(lineno)d} %(levelname)s %(message)s'
    else:
        fmt = '%(asctime)s %(levelname)s %(message)s'
    add_console_logger(level, fmt)
    logger = add_file_logger(log_file_prefix, level, log_path, fmt)
    return logger


def add_console_logger(level=logging.DEBUG, fmt='%(asctime)s %(levelname)s %(message)s'):
    logger = logging.getLogger()
    formatter = logging.Formatter(fmt)
    logger.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def add_file_logger(log_file_prefix, level=logging.DEBUG, log_path=".",
                    format='%(asctime)s %(levelname)s %(message)s'):

    logger = logging.getLogger()
    formatter = logging.Formatter(format)
    # this level can be reduce for each handler
    logger.setLevel(level)
    logfile = os.path.join(log_path, log_file_prefix + '.log')
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
