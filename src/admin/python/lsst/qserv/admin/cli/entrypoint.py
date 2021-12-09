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
import logging
import sys
from typing import List, Optional

from .options import (
    case_option,
    cmsd_manager_option,
    compare_results_option,
    czar_connection_option,
    db_uri_option,
    db_admin_uri_option,
    db_qserv_user_option,
    debug_option,
    instance_id_option,
    load_option,
    log_level_option,
    options_file_option,
    mysql_monitor_password_option,
    pull_option,
    reload_option,
    repl_connection_option,
    repl_ctrl_domain_name_option,
    run_option,
    run_tests_option,
    tests_yaml_option,
    unload_option,
    vnid_option,
    worker_connection_option,
    xrootd_manager_option,
)
from . import script
from ..watcher import watch


socket_option_help = f"""Accepts query key {click.style('socket',
bold=True)}: The path to a socket file used to connect to the database.
"""

socket_option_description = f"""For URI options that accept a socket: if
{click.style('host', bold=True)} and {click.style('port', bold=True)} are
provided then node excution will be paused early, until the database TCP
connection is available for connections. If {click.style('socket', bold=True)}
is provided then the {click.style('host', bold=True)} and
{click.style('port', bold=True)} part of the URI are not required. If
{click.style('host', bold=True)}, {click.style('port', bold=True)}, and
{click.style('socket', bold=True)} are provided then node excution will be
paused until the database is available via TCP connection, and the
{click.style('socket', bold=True)} will be used for subsequent database
communication."""


worker_db_help = "Non-admin URI to the worker database. " + socket_option_help
admin_worker_db_help = "Admin URI to the worker database. " + socket_option_help


help_order  =[
  "proxy",
  "cmsd-manager",
  "xrootd-manager",
  "worker-cmsd",
  "worker-repl",
  "worker-xrootd",
  "replication-controller",
  "smig-update",
  "integration-test",
  "delete-database",
  "ingest-table",
  "load-simple",
  "watcher",
]


class EntrypointCommandGroup(click.Group):
    """Group class for custom entrypoint command behaviors."""

    def list_commands(self, ctx: click.Context) -> List[str]:
        """List the qserv commands in the order specified by help_order.

        Returns
        -------
        commands : Sequence [ str ]
            The list of commands, in the order they should appear in --help.
        """
        # make sure that all the commands are named in our help_order list:
        missing = set(help_order).symmetric_difference(self.commands.keys())
        if missing:
            raise RuntimeError(f"{missing} is found in help_order or commands but not both.")
        return help_order


@click.group(cls=EntrypointCommandGroup)
@log_level_option()
def entrypoint(log_level: str) -> None:
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@entrypoint.command()
@click.argument("repl_ctrl_uri")
def load_simple(repl_ctrl_uri: str) -> None:
    """Load a small test dataset into qserv.

    REPL_CTRL_URI is the uri to the replication controller.
    """
    script.load_simple(repl_ctrl_uri)


@entrypoint.command()
@repl_connection_option(
    help=repl_connection_option.keywords["help"]
    + " If provided will wait for the replication system to be responsive before loading data (does not guarantee system readyness)."
)
@pull_option()
@load_option()
@unload_option()
@reload_option()
@run_tests_option()
@compare_results_option()
@case_option()
@tests_yaml_option()
def integration_test(
    repl_connection: str,
    pull: Optional[bool],
    unload: bool,
    load: Optional[bool],
    reload: bool,
    cases: List[str],
    run_tests: bool,
    tests_yaml: str,
    compare_results: bool,
) -> None:
    """Run integration tests using ingested test data.

    TESTS_YAML is the yaml file paths that contains connection information & describes tests to load and run.
    """
    results = script.integration_test(
        repl_connection=repl_connection,
        pull=pull,
        unload=unload,
        load=load,
        reload=reload,
        cases=cases,
        run_tests=run_tests,
        tests_yaml=tests_yaml,
        compare_results=compare_results,
    )
    click.echo(str(results))
    sys.exit(0 if results.passed else 1)


@entrypoint.command()
@click.argument("DATABASE")
@click.argument("repl_ctrl_uri")
@click.option(
    "--admin",
    help="Use the admin insetad of user auth key.",
    is_flag=True,
)
def delete_database(repl_ctrl_uri: str, database: str, admin: bool) -> None:
    """Remove a database.

    It is NOT recommended to use this for integration test data; because the
    reference database will not be removed, this only removes a database from
    qserv.

    This deletes a named database from a qserv instance.
    !!Does not verify or ask the user to confirm!!

    In the future we should add a credential check...?

    DATABASE is the name of the database to remove."

    REPL_CTRL_URI is the uri to the replication controller.
    """
    script.delete_database(repl_ctrl_uri=repl_ctrl_uri, database=database, admin=admin)


@entrypoint.command(help=f"Start as a qserv proxy node.\n\n{socket_option_description}")
@db_uri_option(
    help="The non-admin URI to the proxy's database, used for non-smig purposes. " + socket_option_help,
    required=True,
)
@db_admin_uri_option(
    help="The admin URI to the proxy's database, used for schema initialization. " + socket_option_help,
    required=True,
)
@mysql_monitor_password_option()
@repl_ctrl_domain_name_option()
@xrootd_manager_option(required=True)
@click.option(
    "--proxy-backend-address",
    default="127.0.0.1:3306",
    show_default=True,
    help="This is the same as the proxy-backend-address option to mysql proxy. This value is substitued "
    "into the proxy-backend-address parameter in 'my-proxy.cnf.jinja'."
)
@options_file_option()
def proxy(
    db_uri: str,
    db_admin_uri: str,
    mysql_monitor_password: str,
    repl_ctl_dn: str,
    xrootd_manager: str,
    proxy_backend_address: str,
) -> None:
    """Start as a qserv-proxy node.
    """
    script.enter_proxy(
        db_uri=db_uri,
        db_admin_uri=db_admin_uri,
        repl_ctl_dn=repl_ctl_dn,
        mysql_monitor_password=mysql_monitor_password,
        xrootd_manager=xrootd_manager,
        proxy_backend_address=proxy_backend_address,
    )


@entrypoint.command()
@click.option(
    "--cms-delay-servers",
    help="The value for 'cms.delay servers' in the cmsd-manager.cf file.",
    default="80%",
)
@options_file_option()
def cmsd_manager(cms_delay_servers: str) -> None:
    """Start as a cmsd manager node.
    """
    script.enter_manager_cmsd(cms_delay_servers=cms_delay_servers)


@entrypoint.command()
@cmsd_manager_option(required=True)
@options_file_option()
def xrootd_manager(cmsd_manager: str) -> None:
    """Start as an xrootd manager node.
    """
    script.enter_xrootd_manager(cmsd_manager=cmsd_manager)


@entrypoint.command(help=f"Start as a worker cmsd node.\n\n{socket_option_description}")
@db_uri_option(help=worker_db_help)
@vnid_option(required=True)
@cmsd_manager_option(required=True)
@debug_option()
@options_file_option()
def worker_cmsd(cmsd_manager: str, vnid: str, debug_port: Optional[int], db_uri: str) -> None:
    script.enter_worker_cmsd(
        cmsd_manager=cmsd_manager, vnid=vnid, debug_port=debug_port, db_uri=db_uri
    )


@entrypoint.command(help=f"Start as a worker xrootd node.\n\n{socket_option_description}")
@debug_option()
@db_uri_option(help=worker_db_help)
@db_admin_uri_option(help=admin_worker_db_help)
@vnid_option(required=True)
@cmsd_manager_option(required=True)
@repl_ctrl_domain_name_option()
@mysql_monitor_password_option()
@db_qserv_user_option()
@options_file_option()
def worker_xrootd(
    debug_port: Optional[int],
    db_uri: str,
    db_admin_uri: str,
    vnid: str,
    cmsd_manager: str,
    repl_ctl_dn: str,
    mysql_monitor_password: str,
    db_qserv_user: str,
) -> None:
    script.enter_worker_xrootd(
        debug_port=debug_port,
        db_uri=db_uri,
        db_admin_uri=db_admin_uri,
        vnid=vnid,
        cmsd_manager=cmsd_manager,
        repl_ctl_dn=repl_ctl_dn,
        mysql_monitor_password=mysql_monitor_password,
        db_qserv_user=db_qserv_user,
    )


@entrypoint.command(help=f"Start as a replication worker node.\n\n{socket_option_description}")
@instance_id_option(required=True)
@db_admin_uri_option(help="The admin URI to the worker's database, used for replication and ingest. " + socket_option_help)
@repl_connection_option()
@debug_option()
@run_option()
@options_file_option()
def worker_repl(
    instance_id: str,
    db_admin_uri: str,
    repl_connection: str,
    debug_port: Optional[int],
    run: bool,
) -> None:
    script.enter_worker_repl(
        instance_id=instance_id,
        db_admin_uri=db_admin_uri,
        repl_connection=repl_connection,
        debug_port=debug_port,
        run=run,
    )


@entrypoint.command(help=f"Start as a replication controller node.\n\n{socket_option_description}")
@instance_id_option(required=True)
@db_uri_option(
    help="The non-admin URI to the replication controller's database, used for non-smig purposes.",
    required=True,
)
@db_admin_uri_option(
    help="The admin URI to the proxy's database, used for schema initialization. " + socket_option_help,
    required=True,
)
@click.option(
    "--worker",
    "workers",
    help=(
        "The settings for each worker in the system. "
        "The value must be in the form 'key1=val1,key2=val,...'"
    ),
    multiple=True,
)
@click.option(
    "--xrootd-manager",
    help="The host name of the xrootd manager node.",
)
@click.option(
    "--qserv-czar-db",
    help="The connection string for the czar database.",
)
@run_option()
@options_file_option()
def replication_controller(
    instance_id: str,
    db_uri: str,
    db_admin_uri: str,
    workers: List[str],
    xrootd_manager: str,
    qserv_czar_db: str,
    run: bool,
) -> None:
    """Start as a replication controller node."""
    script.enter_replication_controller(
        db_uri=db_uri,
        db_admin_uri=db_admin_uri,
        workers=workers,
        instance_id=instance_id,
        run=run,
        xrootd_manager=xrootd_manager,
        qserv_czar_db=qserv_czar_db
    )


@entrypoint.command()
@click.option(
    "--cluster-id",
    required=True,
    help="The name/identifier of the cluster to show in alerts and log messages.",
)
@click.option(
    "--notify-url-file",
    help="The path to the file that contains the url to receive alerts. "
    "Accepts 'None' to not send notifications (useful for debugging).",
    required=True,
    callback=lambda ctx, par, val: None if val == "None" else val,
)
@click.option(
    "--qserv",
    default="qserv://qsmaster@czar-proxy:4040",
    help="The url to the qserv instance to watch.",
)
@click.option(
    "--timeout-sec",
    help="The threshold time, in seconds, queries taking longer than this will trigger an alert.",
    default=600,  # 10 minutes
)
@click.option(
    "--interval-sec",
    help="How long to wait (in seconds) between checks.",
    default=60,  # 1 minute
)
@click.option(
    "--show-query",
    help="Show the query in alerts.",
    default=False,
    show_default=True,
    is_flag=True,
)
def watcher(
    cluster_id: str,
    notify_url_file: str,
    qserv: str,
    timeout_sec: int,
    interval_sec: int,
    show_query: bool,
) -> None:
    """Run a watcher algorithm that sends notifications if querys appear to stop
    processing."""
    watch(
        cluster_id=cluster_id,
        notify_url_file=notify_url_file,
        qserv=qserv,
        timeout_sec=timeout_sec,
        interval_sec=interval_sec,
        show_query=show_query,
    )


@entrypoint.command()
@czar_connection_option()
@worker_connection_option()
@repl_connection_option()
@options_file_option()
def smig_update(czar_connection: str, worker_connections: List[str], repl_connection: str) -> None:
    """Run schema update on nodes."""
    script.smig_update(
        czar_connection=czar_connection,
        worker_connections=worker_connections,
        repl_connection=repl_connection,
    )
