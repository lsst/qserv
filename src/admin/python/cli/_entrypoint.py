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


"""Command line tool for launching qserv components inside a qserv container.
"""


import click
from functools import partial
import logging
import socket

from . import _load
from . import _query
from .options import (
    cmsd_manager_option,
    connection_option,
    db_qserv_user_option,
    db_scheme_option,
    debug_option,
    instance_id_option,
    log_level_option,
    mysql_monitor_password_option,
    mysql_user_qserv_option,
    repl_connection_option,
    repl_ctrl_domain_name_option,
    repl_ctrl_port_option,
    run_option,
    vnid_option,
    xrd_port_option,
    xrootd_manager_option,
)
from . import script
from .utils import split_kv
from ..template import save_template_cfg


@click.group()
@log_level_option()
def entrypoint(log_level):
    logging.basicConfig(level=log_level)


@entrypoint.command()
@click.option("-d", "--delete", is_flag=True, help="Remove the test database from qserv.")
def load(**kwargs):
    "Load a small test dataset into qserv."
    _load.load(**kwargs)


@entrypoint.command()
@click.argument("STATEMENT")
@click.option("-h", "--host", help="The sql server.", required=True)
@click.option("-p", "--port", help="The sql server port.", required=True, type=click.INT)
def query(**kwargs):
    """Run an sql statement and print the results."""
    _query.query(**kwargs)


@entrypoint.command()
@connection_option()
@db_scheme_option()
@mysql_user_qserv_option()
@mysql_monitor_password_option()
@repl_ctrl_domain_name_option()
@xrootd_manager_option()
@click.option("--czar-db-host", help="The name of the czar database host.", default="")
@click.option("--czar-db-port", help="The port number of the czar database host.", default="")
@click.option("--czar-db-socket", help="""The unix socket of the czar database host.
This can be used if the proxy container and the database are running on the same filesystem (e.g. in a pod).
""", default="")
def proxy(**kwargs):
    script.enter_proxy(**kwargs)


@entrypoint.command()
@click.option("--cms-delay-servers",
              help="The value for 'cms.delay servers' in the cmsd-manager.cf file.",
              default="80%")
def cmsd_manager(**kwargs):
    script.enter_manager_cmsd(**kwargs)


@entrypoint.command()
@cmsd_manager_option(required=True)
def xrootd_manager(**kwargs):
    script.enter_xrootd_manager(**kwargs)


@entrypoint.command()
@connection_option()
@vnid_option(required=True)
@cmsd_manager_option(required=True)
@debug_option()
def worker_cmsd(**kwargs):
    script.enter_worker_cmsd(**kwargs)


@entrypoint.command()
@debug_option()
@xrd_port_option()
@connection_option()
@vnid_option(required=True)
@cmsd_manager_option(required=True)
@repl_ctrl_domain_name_option()
@mysql_monitor_password_option()
@db_qserv_user_option()
def worker_xrootd(**kwargs):
    script.enter_worker_xrootd(**kwargs)


@entrypoint.command()
@vnid_option(required=True)
@instance_id_option(required=True)
@connection_option()
@repl_connection_option()
@debug_option()
@run_option()
def worker_repl(**kwargs):
    script.enter_worker_repl(**kwargs)


@entrypoint.command()
@instance_id_option(required=True)
@db_scheme_option()
@connection_option()
@repl_connection_option()
@click.option(
    "--worker", "workers",
    help=("The settings for each worker in the system. "
          "The value must be in the form 'key1=val1,key2=val,...'"),
    multiple=True,
)
@click.option(
    "--xrootd-manager",
    help="The host name of the xrootd manager node.",
)
@click.option(
    "--qserv-czar-db",
    help="The connection string for the czar database."
)
@run_option()
def replication_controller(**kwargs):
    script.enter_replication_controller(**kwargs)


@entrypoint.command()
@click.option(
    "--dashboard-port",
    help="The port the dashboard will serve on.",
)
@click.option(
    "--dashboard-html",
    help="The path to the folder with the html sources for the nginx dashboard."
)
@repl_ctrl_domain_name_option()
@repl_ctrl_port_option()
def init_dashboard(**kwargs):
    script.init_dashboard(**kwargs)
