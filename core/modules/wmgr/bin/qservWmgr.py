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
Application which provides worker management service (HTTP-based).

@author  Andy Salnikov, SLAC
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import argparse
import logging
import sys

# -----------------------------
# Imports for other modules --
# -----------------------------
from flask import Flask
from lsst.qserv.wmgr import auth, config, dbMgr, procMgr, xrdMgr

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# ------------------------
# Exported definitions --
# ------------------------


def main():

    parser = argparse.ArgumentParser(description='Single-node data loading script for Qserv.')

    parser.add_argument('-v', '--verbose', dest='verbose', default=[], action='append_const',
                        const=None, help='More verbose output, can use several times.')
    parser.add_argument('-c', '--config', dest='configFile', default=None, metavar='PATH',
                        help='Read configuration from provided file. By default configuration'
                        ' is loaded from a file specified with env. variable WMGRCONFIG.')
    args = parser.parse_args()

    # configure logging
    verbosity = len(args.verbose)
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    level = levels.get(verbosity, logging.DEBUG)
    fmt = "%(asctime)s [PID:%(process)d] [%(levelname)s] (%(funcName)s() at %(filename)s:%(lineno)d) " \
          "%(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt)

    # instanciate and configure app
    app = Flask(__name__)
    if args.configFile is not None:
        app.config.from_pyfile(args.configFile)
    else:
        app.config.from_envvar('WMGRCONFIG')
    config.Config.init(app)

    # add few blueprints
    app.register_blueprint(dbMgr.dbService, url_prefix='/dbs')
    app.register_blueprint(procMgr.procService, url_prefix='/services')
    app.register_blueprint(xrdMgr.xrdService, url_prefix='/xrootd')

    # check authentication before every request
    authCheck = auth.Auth(app.config)
    app.before_request(authCheck.checkAuth)

    # port number comes from SERVER_HOST configuration
    host = app.config.get('WMGR_INTERFACE')
    port = app.config.get('WMGR_PORT')
    app.run(host, port, threaded=True)


if __name__ == '__main__':
    sys.exit(main())
