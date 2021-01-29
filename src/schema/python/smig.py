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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import configparser
import importlib
import logging
from lsst.db import engineFactory
import mysql.connector
import sqlalchemy
from sqlalchemy.engine.url import make_url
from urllib.parse import urlparse

_mig_module_name = "schema_migration"
_factory_method_name = "make_migration_manager"


def _load_migration_mgr(mod_name, connection, scripts_dir):
    """Dynamic loading of the migration manager based on module name.

    Parameters
    ----------
    mod_name : `str`
        Module name, e.g. "qmeta"
    connection : dbapi connection
        The database connection to use.
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
        mod_instance = importlib.import_module(
            "lsst.qserv." + mod_name + "." + _mig_module_name
        )
    except ImportError:
        logging.error(
            "Failed to load %s module from lsst.qserv.%s package",
            _mig_module_name,
            mod_name,
        )
        raise

    # find a method with name "make_migration_manager"
    try:
        factory = getattr(mod_instance, _factory_method_name)
    except AttributeError:
        logging.error(
            "Module %s does not contain factory method %s.",
            _mig_module_name,
            _factory_method_name,
        )
        raise

    # call factory method, pass all needed arguments
    mgr = factory(name=mod_name, connection=connection, scripts_dir=scripts_dir)

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
        raise ValueError(
            "Unexpected technology specified for connection:"
            " {}".format(config.get("technology"))
        )
    res["username"] = config.get("username") or config.get("user")
    res["password"] = (
        config.get("password") or config.get("passwd") or config.get("pass")
    )
    res["host"] = config.get("hostname") or config.get("host")
    res["port"] = config.get("port")
    res["database"] = config.get("database") or config.get("db")
    socket = config.get("unix_socket") or config.get("socket")
    if socket:
        res["query"] = dict(unix_socket=socket)

    return res


def smig(
    verbose,
    do_migrate,
    check,
    final,
    scripts,
    connection,
    config_file,
    config_section,
    module,
):
    """Execute schema migration.

    Parameters
    ----------
    Parameters match declared arguments from main.

    Raises
    ------
    IOError
        If the config_file directory does not exist.
    ImportError
        If the module can not be imported.
    AttributeError
        If the module does not have a migration manager factory method.
    ValueErrors
        If the config_file's 'technology' parameter specifies an unrecognized
        techology for database connection.
    """

    # configure logging
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    level = levels.get(verbose, logging.DEBUG)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt)

    if not connection and not config_file:
        raise RuntimeError("A connection or config file is required.")

    if connection:
        c = urlparse(connection)
        connection = mysql.connector.connect(
            user=c.username,
            password=c.password,
            host=c.hostname,
            port=c.port,
        )
    # TODO do we still need to support smig cfg file?
    #      needs fixing to use mysql-connector instead of sqlalchmy
    #      (use of `engine` is removed)
    # elif config_file:
    #     if not config_section:
    #         parser.error("-s options required with -f")

    #     cfg = configparser.SafeConfigParser()
    #     if not cfg.read([config_file]):
    #         # file was not found, generate exception which should happen
    #         # if we tried to open that file
    #         raise IOError(2, "No such file or directory: '{}'".format(config_file))

    #     # will throw is section is missing
    #     config = dict(cfg.items(config_section))

    #     # instantiate database engine
    #     config = _normalizeConfig(config)
    #     engine = engineFactory.getEngineFromArgs(**config)

    # make an object which will manage migration process
    mgr = _load_migration_mgr(module, connection=connection, scripts_dir=scripts)

    current = mgr.current_version()
    print("Current schema version: {}".format(current))

    latest = mgr.latest_version()
    print("Latest schema version: {}".format(latest))

    migrations = mgr.migrations
    print("Known migrations:")
    for migration in migrations:
        tag = " (X)" if migration.from_version >= current else ""
        print(f"  {migration.from_version} -> {migration.to_version} : {migration.name}{tag}")
    if check:
        return 0 if mgr.current_version() == mgr.latest_version() else 1

    # do the migrations
    final = mgr.migrate(final, do_migrate)
    if final is None:
        print("No migration was needed")
    else:
        if do_migrate:
            print("Database was migrated to version {}".format(final))
        else:
            print("Database would be migrated to version {}".format(final))
