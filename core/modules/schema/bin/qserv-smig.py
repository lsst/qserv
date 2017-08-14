#!/usr/bin/env python

# LSST Data Management System
# Copyright 2017 AURA/LSST.
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

"""Application which implements migration process for qserv databases.
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import argparse
import configparser
import importlib
import logging
import os
import sys

# -----------------------------
# Imports for other modules --
# -----------------------------
from lsst.db import engineFactory
import sqlalchemy
from sqlalchemy.engine.url import make_url

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

_def_scripts = os.path.join(os.environ.get("QSERV_DIR", ""), "share/qserv/schema")

_mig_module_name = "schema_migration"
_factory_method_name = "make_migration_manager"


def _load_migration_mgr(mod_name, engine, scripts_dir):
    """Dynamic loading of the migration manager based on module name.

    Parameters
    ----------
    mod_name : `str`
        Module name, e.g. "qmeta"
    engine : object
        Sqlalchemy engine instance.
    scripts_dir : `str`
        Path where migration scripts are located, this is system-level directory,
        per-module scripts are usually located in sub-directories.

    Returns
    -------
    Object which manages migrations for that module.

    Raises
    ------
    Exception is raised for any error.
    """

    # load module "lsst.qserv.<module>.schema_migration"
    try:
        mod_instance = importlib.import_module("lsst.qserv." + mod_name + "." + _mig_module_name)
    except ImportError:
        logging.error("Failed to load %s module from lsst.qserv.%s package",
                      _mig_module_name, mod_name)
        raise

    # find a method with name "make_migration_manager"
    try:
        factory = getattr(mod_instance, _factory_method_name)
    except AttributeError:
        logging.error("Module %s does not contain factory method %s.",
                      _mig_module_name, _factory_method_name)
        raise

    # call factory method, pass all needed arguments
    mgr = factory(name=mod_name, engine=engine, scripts_dir=scripts_dir)

    return mgr


def _normalizeConfig(config):
    """Make connection parameters out of config.

    We have a mess in our INI files in how we specify connection parameters
    depending on which piece of C++ or Python code uses those parameters.
    For our purposes I need to convert that mess into a mess acceptable by
    getEngineFromArgs() method.

    Parameters
    ----------
    config : dict
        Parameters read from configuration file

    Returns
    -------
    Dictionary with parameters passed to getEngineFromArgs()
    """
    res = {}
    if config.get("technology") == "mysql":
        res["drivername"] = "mysql+mysqldb"
    elif config.get("technology") is not None:
        raise ValueError("Unexpected technology specified for connection:"
                         " {}".format(config.get("technology")))
    res["username"] = config.get("username") or config.get("user")
    res["password"] = config.get("password") or config.get("passwd") or config.get("pass")
    res["host"] = config.get("hostname") or config.get("host")
    res["port"] = config.get("port")
    res["database"] = config.get("database") or config.get("db")
    socket = config.get("unix_socket") or config.get("socket")
    if socket:
        res["query"] = dict(unix_socket=socket)

    return res

# ------------------------
# Exported definitions --
# ------------------------


def main():

    parser = argparse.ArgumentParser(description="Qserv database schema migration.")

    parser.add_argument("-v", "--verbose", default=0, action="count",
                        help="Use one -v for INFO logging, two for DEBUG.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-m", "--do-migrate", default=False, action="store_true",
                       help="Do migration, without this option script prints various info "
                       "and exits.")
    group.add_argument("--check", default=False, action="store_true",
                       help="Check that migration is needed, script returns 0 if schema is "
                       "up-to-date, 1 otherwise.")
    parser.add_argument("-n", "--final", default=None, action="store", type=int,
                        metavar="VERSION",
                        help="Stop migration at given version, by default update to "
                        "latest version.")
    parser.add_argument("--scripts", default=_def_scripts, action="store",
                        metavar="PATH",
                        help="Location for migration scripts, def: %(default)s.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-c", "--connection", metavar="CONNECTION",
                       help="Connection string in format mysql://user:pass@host:port/database.")
    group.add_argument("-f", "--config-file", metavar="PATH",
                       help="Name of configuration file in INI format with connection parameters.")
    parser.add_argument("-s", "--config-section", metavar="NAME",
                        help="Name of configuration section in configuration file.")

    parser.add_argument("module",
                        help="Name of Qserv module for which to update schema, e.g. qmeta.")

    args = parser.parse_args()

    # configure logging
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    level = levels.get(args.verbose, logging.DEBUG)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt)

    if args.connection:
        url = make_url(args.connection)
        engine = sqlalchemy.create_engine(url)
    elif args.config_file:
        if not args.config_section:
            parser.error("-s options required with -f")

        cfg = configparser.SafeConfigParser()
        if not cfg.read([args.config_file]):
            # file was not found, generate exception which should happen
            # if we tried to open that file
            raise IOError(2, "No such file or directory: '{}'".format(args.config_file))

        # will throw is section is missing
        config = dict(cfg.items(args.config_section))

        # instantiate database engine
        config = _normalizeConfig(config)
        engine = engineFactory.getEngineFromArgs(**config)

    # make an object which will manage migration process
    mgr = _load_migration_mgr(args.module, engine=engine, scripts_dir=args.scripts)

    current = mgr.current_version()
    print("Current schema version: {}".format(current))

    latest = mgr.latest_version()
    print("Latest schema version: {}".format(latest))

    migrations = mgr.migrations()
    print("Known migrations:")
    for v0, v1, script in migrations:
        tag = " (X)" if v0 >= current else ""
        print("  {} -> {} : {}{}".format(v0, v1, script, tag))

    if args.check:
        return 0 if mgr.current_version() == mgr.latest_version() else 1

    # do the migrations
    final = mgr.migrate(args.final, args.do_migrate)
    if final is None:
        print("No migration was needed")
    else:
        if args.do_migrate:
            print("Database was migrated to version {}".format(final))
        else:
            print("Database would be migrated to version {}".format(final))


if __name__ == "__main__":
    sys.exit(main())
