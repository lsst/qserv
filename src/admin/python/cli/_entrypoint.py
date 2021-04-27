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

from .options import (
    cmsd_manager_option,
    connection_option,
    db_host_option,
    db_password_option,
    db_port_option,
    db_qserv_user_option,
    db_scheme_option,
    db_user_option,
    debug_option,
    http_server_port_option,
    instance_id_option,
    log_level_option,
    mysql_monitor_password_option,
    mysql_user_qserv_option,
    qserv_db_pswd_option,
    repl_connection_option,
    repl_ctrl_domain_name_option,
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
@connection_option()
@db_scheme_option()
@mysql_user_qserv_option()
@mysql_monitor_password_option()
@repl_ctrl_domain_name_option()
@xrootd_manager_option()
def proxy(**kwargs):
    script.enter_proxy(**kwargs)


@entrypoint.command()
def cmsd_manager():
    script.enter_cmsd_manager()


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
@repl_connection_option()
@debug_option()
@run_option()
def worker_repl(**kwargs):
    script.enter_worker_repl(**kwargs)


@entrypoint.command()
@db_password_option()
@db_port_option()
@click.option("--czar-domain-name", help="The domain name of the czar.")
@click.option("--wmgr-secret", help="The username:password pair for the wmgr.")
def worker_wmgr(**kwargs):
    script.enter_worker_wmgr(**kwargs)


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
@run_option()
def replication_controller(**kwargs):
    script.enter_replication_controller(**kwargs)
