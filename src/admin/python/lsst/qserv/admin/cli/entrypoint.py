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
from collections import OrderedDict
from dataclasses import dataclass, field
from functools import partial
import logging
import os
import sys
from typing import Any, Callable, Dict, List, Optional

from click.decorators import pass_context

from .options import (
    case_option,
    cmd_option,
    cmsd_manager_name_option,
    cmsd_manager_count_option,
    compare_results_option,
    czar_connection_option,
    db_uri_option,
    db_admin_uri_option,
    db_qserv_user_option,
    debug_option,
    load_option,
    log_cfg_file_option,
    log_level_option,
    OptionGroup,
    options_file_option,
    mysql_monitor_password_option,
    reload_option,
    repl_auth_key_option,
    repl_admin_auth_key_option,
    repl_connection_option,
    repl_instance_id_option,
    repl_registry_host_option,
    repl_registry_port_option,
    repl_http_port_option,
    results_dirname_option,
    results_protocol_option,
    run_option,
    run_tests_option,
    targs_options,
    tests_yaml_option,
    unload_option,
    vnid_config_option,
    worker_connection_option,
    xrootd_manager_option,
)
from . import utils
from .render_targs import render_targs
from . import script
from ..watcher import watch


_log = logging.getLogger(__name__)


template_dir = "/usr/local/qserv/templates/"
mysql_proxy_cfg_template = os.path.join(template_dir, "proxy/etc/my-proxy.cnf.jinja")
czar_cfg_template = os.path.join(template_dir, "proxy/etc/qserv-czar.cnf.jinja")
cmsd_manager_cfg_template = os.path.join(template_dir, "xrootd/etc/cmsd-manager.cf.jinja")
cmsd_worker_cfg_template = os.path.join(template_dir, "xrootd/etc/cmsd-worker.cf.jinja")
xrdssi_cfg_template = os.path.join(template_dir, "xrootd/etc/xrdssi.cf.jinja")
xrootd_manager_cfg_template = os.path.join(template_dir, "xrootd/etc/xrootd-manager.cf.jinja")

mysql_proxy_cfg_path = "/config-etc/my-proxy.cnf"
czar_cfg_path = "/config-etc/qserv-czar.cnf"
cmsd_manager_cfg_path = "/config-etc/cmsd-manager.cnf"
cmsd_worker_cfg_path = "/config-etc/cmsd-worker.cf"
xrdssi_cfg_path = "/config-etc/xrdssi-worker.cf"
xrootd_manager_cfg_path = "/config-etc/xrootd-manager.cf"

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

extended_args_description = """Options and arguments may be passed directly to
{app} by adding '--', and then adding those options and arguments."""


worker_db_help = f"""Non-admin URI to the worker database. {socket_option_help}
 Populates 'hostname', 'port', and 'socket' under '[mysql]' in the xrdssi config
file. Also used to wait for schema to be at the correct version in this
database.
"""


admin_worker_db_help = "Admin URI to the worker database. " + socket_option_help


@dataclass
class CommandInfo:
    default_cmd: Optional[str] = None

# Commands are in the ordered dict in "help order" - the order they
# appear in `entrypoint --help`
commands = OrderedDict((
    ("proxy", CommandInfo(
        "mysql-proxy --proxy-lua-script=/usr/local/lua/qserv/scripts/mysqlProxy.lua "
        "--lua-cpath=/usr/local/lua/qserv/lib/czarProxy.so --defaults-file={{proxy_cfg_path}}",
    )),
    ("cmsd-manager", CommandInfo(
        "cmsd -c {{cmsd_manager_cfg_path}} -n manager -I v4",
    )),
    ("xrootd-manager", CommandInfo("xrootd -c {{xrootd_manager_cfg_path}} -n manager -I v4")),
    ("worker-cmsd", CommandInfo(
        "cmsd -c {{cmsd_worker_cfg_path}} -n worker -I v4 -l @libXrdSsiLog.so -+xrdssi {{xrdssi_cfg_path}}",
    )),
    ("worker-repl", CommandInfo(
        "qserv-replica-worker "
        "--qserv-worker-db={{db_admin_uri}} "
        "--config={{config}} {% for arg in extended_args %}{{arg}}  {% endfor %}"
    )),
    ("worker-xrootd", CommandInfo(
        "xrootd -c {{cmsd_worker_cfg_path}} -n worker -I v4 -l @libXrdSsiLog.so -+xrdssi {{xrdssi_cfg_path}}",
    )),
    ("replication-controller", CommandInfo(
        "qserv-replica-master-http "
        "--config={{db_uri}} "
        "--http-root={{http_root}} "
        "--qserv-czar-db={{qserv_czar_db}} "
        "{% for arg in extended_args %}{{arg}} {% endfor %}"
    )),
    ("replication-registry", CommandInfo(
        "qserv-replica-registry "
        "--config={{db_uri}} "
        "{% for arg in extended_args %}{{arg}} {% endfor %}"
    )),
    ("smig-update", CommandInfo()),
    ("integration-test", CommandInfo()),
    ("delete-database", CommandInfo()),
    ("load-simple", CommandInfo()),
    ("watcher", CommandInfo()),
    ("prepare-data", CommandInfo()),
    ("spawned-app-help", CommandInfo()),
))


cmsd_worker_cfg_file_option = partial(
    click.option,
    "--cmsd-worker-cfg-file",
    help="Path to the cmsd worker config file.",
    default=cmsd_worker_cfg_template,
    show_default=True,
)


cmsd_worker_cfg_path_option = partial(
    click.option,
    "--cmsd-worker-cfg-path",
    help="Location to render cmsd_worker_cfg_file.",
    default=cmsd_worker_cfg_path,
    show_default=True,
)


xrdssi_cfg_file_option = partial(
    click.option,
    "--xrdssi-cfg-file",
    help="Path to the xrdssi config file.",
    default=xrdssi_cfg_template,
    show_default=True,
)


xrdssi_cfg_path_option = partial(
    click.option,
    "--xrdssi-cfg-path",
    help="Location to render xrdssi-cfg-file.",
    default=xrdssi_cfg_path,
    show_default=True,
)


class EntrypointCommandExArgs(click.Command):
    """Command class for custom entrypoint subcommand behaviors.

    * Provides command support for the "--" option pass-thru arguments; removes
      all args after "--" and puts the args (without the "--") into the context
      for separate handling by the command function. (use the `@pass_context`
      decorator on the command function to be passed the context)
    """

    @dataclass
    class ContextObj:
        """Click allows for the context to have a `obj` parameter, opaque to
        click, for passing data to command functions.
        This is the context object type for EntrypointExtendedArgs.
        (Factor this dataclass and/or command class as you need for greater
        command type polymorphism.)
        """
        extended_args: List[str] = field(default_factory=list)

    def parse_args(self, ctx: click.Context, args: List[str]) -> List[str]:
        """Remove args after "--" and put them in the context, then parse as
        normal.
        """
        separator = "--"
        ctx.obj = self.ContextObj()
        if separator in args:
            if separator in args:
                ctx.obj.extended_args = args[args.index(separator)+1:]
                args = args[:args.index(separator)]
        args = super().parse_args(ctx, args)
        return args


class EntrypointCommandGroup(click.Group):
    """Group class for custom entrypoint command behaviors.

    * Provides ordering for list of subcommands in --help
    """

    def list_commands(self, ctx: click.Context) -> List[str]:
        """List the qserv commands in the order specified by help_order.

        Returns
        -------
        commands : Sequence [ str ]
            The list of commands, in the order they should appear in --help.
        """
        # make sure that all the commands are named in our help_order list:
        missing = set(commands.keys()).symmetric_difference(self.commands.keys())
        if missing:
            raise RuntimeError(f"{missing} is found in help_order or commands but not both.")
        return list(commands.keys())


def cmd_default(ctx: click.Context, param: click.core.Option, value: str) -> None:
    """Sets the default value for --cmd for the current function in the
    context's default map.
    """
    if not ctx.command.name:
        return
    ctx.default_map = ctx.default_map or {}
    _log.debug(
        "Changing the %s default_map value for --cmd from \"%s\" to \"%s\"",
        ctx.command.name,
        ctx.default_map.get("cmd", "None"),
        default_cmd := commands[ctx.command.name].default_cmd,
    )
    ctx.default_map.update({"cmd": default_cmd})


# cmd_default_option updates the default_map to have the default value for the --cmd option.
cmd_default_option = partial(
    click.option,
    "--cmd-default",
    callback=cmd_default,
    is_eager=True,  # required to show the default in --help
    expose_value=False,
    hidden=True,
)


class cmd_options(OptionGroup):  # noqa: N801
    """Applies the cmd_option and the cmd_default_option decorators to a
    click.command function.
    """

    @property
    def decorators(self) -> List[Callable]:
        return [
            cmd_option(),
            cmd_default_option(),
        ]


@click.group(cls=EntrypointCommandGroup)
@log_level_option()
def entrypoint(log_level: str) -> None:
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@entrypoint.command()
@repl_auth_key_option()
@click.argument("repl_ctrl_uri")
def load_simple(repl_ctrl_uri: str, repl_auth_key: str) -> None:
    """Load a small test dataset into qserv.

    REPL_CTRL_URI is the uri to the replication controller.
    """
    script.load_simple(repl_ctrl_uri, auth_key=repl_auth_key)


@entrypoint.command()
@repl_connection_option(
    help=repl_connection_option.keywords["help"]
    + " If provided will wait for the replication system to be responsive before loading data (does not guarantee system readyness)."
)
@unload_option()
@load_option()
@reload_option()
@case_option()
@run_tests_option()
@tests_yaml_option()
@compare_results_option()
def integration_test(
    repl_connection: str,
    unload: bool,
    load: Optional[bool],
    reload: bool,
    cases: List[str],
    run_tests: bool,
    tests_yaml: str,
    compare_results: bool,
) -> None:
    """Run integration tests using ingested test data.

    TESTS_YAML is the yaml file path that contains connection information and describes tests to load and run.
    """

    results = script.integration_test(
        repl_connection=repl_connection,
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
@tests_yaml_option()
def prepare_data(
    tests_yaml: str,
) -> None:
    """Unzip and partition test datasets.

    TESTS_YAML is the yaml file paths that contains connection information & describes tests to load and run.
    """

    ok = script.prepare_data(
        tests_yaml=tests_yaml,
    )
    click.echo(str(ok))
    sys.exit(0 if ok else 1)

@entrypoint.command()
@click.argument("DATABASE")
@click.argument("repl_ctrl_uri")
@click.option(
    "--admin",
    help="Use the admin insetad of user auth key.",
    is_flag=True,
)
@repl_auth_key_option()
@repl_admin_auth_key_option()
def delete_database(
    repl_ctrl_uri: str,
    database: str,
    admin: bool,
    repl_auth_key: str,
    repl_admin_auth_key: str,
) -> None:
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
    script.delete_database(
        repl_ctrl_uri=repl_ctrl_uri,
        database=database,
        admin=admin,
        auth_key=repl_auth_key,
        admin_auth_key=repl_admin_auth_key,
    )


@entrypoint.command(help=f"Start as a qserv proxy node.\n\n{socket_option_description}")
@pass_context
@db_uri_option(
    help="The non-admin URI to the proxy's database, used for non-smig purposes. " + socket_option_help,
    required=True,
)
@db_admin_uri_option(
    help="The admin URI to the proxy's database, used for schema initialization. " + socket_option_help,
    required=True,
)
@mysql_monitor_password_option()
@xrootd_manager_option(required=True)
@click.option(
    "--proxy-backend-address",
    default="127.0.0.1:3306",
    show_default=True,
    help="This is the same as the proxy-backend-address option to mysql proxy. This value is substitued "
    "into the proxy-backend-address parameter in 'my-proxy.cnf.jinja'."
)
@click.option(
    "--proxy-cfg-file",
    help="Path to the mysql proxy config file.",
    default=mysql_proxy_cfg_template,
    show_default=True,
)
@click.option(
    "--proxy-cfg-path",
    help="Location to render the mysql proxy config file.",
    default=mysql_proxy_cfg_path,
    show_default=True,
)
@click.option(
    "--czar-cfg-file",
    help="Path to the czar config file.",
    default=czar_cfg_template,
    show_default=True,
)
@click.option(
    "--czar-cfg-path",
    help="Location to render the czar config file.",
    default=czar_cfg_path,
    show_default=True,
)
@log_cfg_file_option()
@targs_options()
@cmd_options()
@options_file_option()
def proxy(ctx: click.Context, **kwargs: Any) -> None:
    """Start as a qserv-proxy node.
    """
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_proxy(
        targs=targs,
        db_uri=targs["db_uri"],
        db_admin_uri=targs["db_admin_uri"],
        proxy_backend_address=targs["proxy_backend_address"],
        proxy_cfg_file=targs["proxy_cfg_file"],
        proxy_cfg_path=targs["proxy_cfg_path"],
        czar_cfg_file=targs["czar_cfg_file"],
        czar_cfg_path=targs["czar_cfg_path"],
        cmd=targs["cmd"],
        log_cfg_file=targs["log_cfg_file"],
    )


@entrypoint.command()
@pass_context
@click.option(
    "--cms-delay-servers",
    help="Populates 'cms.delay servers' in the cmsd manager config file.",
)
@click.option(
    "--cmsd_manager_cfg_file",
    help="Path to the cmsd manager config file.",
    default=cmsd_manager_cfg_template,
    show_default=True,
)
@click.option(
    "--cmsd-manager-cfg-path",
    help="Location to render cmsd_manager_cfg_file",
    default=cmsd_manager_cfg_path,
    show_default=True,
)
@targs_options()
@cmd_options()
@options_file_option()
def cmsd_manager(ctx: click.Context, **kwargs: Any) -> None:
    """Start as a cmsd manager node.
    """
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_manager_cmsd(
        targs=targs,
        cmsd_manager_cfg_file=targs["cmsd_manager_cfg_file"],
        cmsd_manager_cfg_path=targs["cmsd_manager_cfg_path"],
        cmd=targs["cmd"],
    )


@entrypoint.command()
@pass_context
@cmsd_manager_name_option()
@cmsd_manager_count_option()
@click.option(
    "--xrootd_manager-cfg-file",
    help="Path to the xrootd manager config file.",
    default=xrootd_manager_cfg_template,
    show_default=True,
)
@click.option(
    "--xrootd-manager-cfg-path",
    help="Location to render xrootd_manager_cfg_file.",
    default=xrootd_manager_cfg_path,
    show_default=True,
)
@targs_options()
@cmd_options()
@options_file_option()
def xrootd_manager(ctx: click.Context, **kwargs: Any) -> None:
    """Start as an xrootd manager node.
    """
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_xrootd_manager(
        targs=targs,
        xrootd_manager_cfg_file=targs["xrootd_manager_cfg_file"],
        xrootd_manager_cfg_path=targs["xrootd_manager_cfg_path"],
        cmd=targs["cmd"],
    )


@entrypoint.command(help=f"Start as a worker cmsd node.\n\n{socket_option_description}")
@pass_context
@db_uri_option(help=worker_db_help)
@vnid_config_option(required=True)
@vnid_config_option(required=True)
@repl_instance_id_option(required=True)
@repl_auth_key_option(required=True)
@repl_admin_auth_key_option(required=True)
@repl_registry_host_option(required=True)
@repl_registry_port_option(required=True)
@repl_http_port_option(required=True)
@results_dirname_option()
@results_protocol_option()
@cmsd_manager_name_option()
@cmsd_manager_count_option()
@debug_option()
@cmsd_worker_cfg_file_option()
@cmsd_worker_cfg_path_option()
@xrdssi_cfg_file_option()
@xrdssi_cfg_path_option()
@log_cfg_file_option()
@targs_options()
@cmd_options()
@options_file_option()
def worker_cmsd(ctx: click.Context, **kwargs: Any) -> None:
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_worker_cmsd(
        targs=targs,
        db_uri=targs["db_uri"],
        cmsd_worker_cfg_file=targs["cmsd_worker_cfg_file"],
        cmsd_worker_cfg_path=targs["cmsd_worker_cfg_path"],
        xrdssi_cfg_file=targs["xrdssi_cfg_file"],
        xrdssi_cfg_path=targs["xrdssi_cfg_path"],
        log_cfg_file=targs["log_cfg_file"],
        cmd=targs["cmd"],
    )


@entrypoint.command(help=f"Start as a worker xrootd node.\n\n{socket_option_description}")
@pass_context
@debug_option()
@db_uri_option(help=worker_db_help)
@db_admin_uri_option(help=admin_worker_db_help)
@vnid_config_option(required=True)
@repl_instance_id_option(required=True)
@repl_auth_key_option(required=True)
@repl_admin_auth_key_option(required=True)
@repl_registry_host_option(required=True)
@repl_registry_port_option(required=True)
@repl_http_port_option(required=True)
@results_dirname_option()
@results_protocol_option()
@cmsd_manager_name_option()
@cmsd_manager_count_option()
@mysql_monitor_password_option()
@db_qserv_user_option()
@cmsd_worker_cfg_file_option()
@cmsd_worker_cfg_path_option()
@xrdssi_cfg_file_option()
@xrdssi_cfg_path_option()
@log_cfg_file_option()
@targs_options()
@cmd_options()
@options_file_option()
def worker_xrootd(ctx: click.Context, **kwargs: Any) -> None:
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_worker_xrootd(
        targs=targs,
        db_uri=targs["db_uri"],
        db_admin_uri=targs["db_admin_uri"],
        cmsd_worker_cfg_file=targs["cmsd_worker_cfg_file"],
        cmsd_worker_cfg_path=targs["cmsd_worker_cfg_path"],
        xrdssi_cfg_file=targs["xrdssi_cfg_file"],
        xrdssi_cfg_path=targs["xrdssi_cfg_path"],
        log_cfg_file=targs["log_cfg_file"],
        cmd=targs["cmd"],
    )


@entrypoint.command(
    help="Start as a replication worker node.\n\n"
         "{socket_option_description}\n\n"
         f"{extended_args_description.format(app='qserv-replica-worker')}",
    cls=EntrypointCommandExArgs,
)
@pass_context
@db_admin_uri_option(help="The admin URI to the worker's database, used for replication and ingest. " + socket_option_help)
@repl_connection_option(
    help=f"{repl_connection_option.keywords['help']} {socket_option_help}"
)
@debug_option()
@cmd_options()
@click.option(
    "--config",
    help="The path to the configuration database for qserv-replica-worker.",
    default="{{repl_connection}}",
    show_default=True,
)
@log_cfg_file_option()
@targs_options()
@run_option()
@options_file_option()
def worker_repl(ctx: click.Context, **kwargs: Any) -> None:
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_worker_repl(
        db_admin_uri=targs["db_admin_uri"],
        repl_connection=targs["repl_connection"],
        log_cfg_file=targs["log_cfg_file"],
        cmd=targs["cmd"],
        run=targs["run"],
    )


@entrypoint.command(
    help=f"Start as a replication controller node.\n\n{socket_option_description}\n\n"
         f"{extended_args_description.format(app='qserv-replica-master-http')}",
    cls=EntrypointCommandExArgs,
)
@pass_context
@db_uri_option(
    help="The non-admin URI to the replication controller's database, used for non-smig purposes.",
    required=True,
)
@db_admin_uri_option(
    help="The admin URI to the replication controller's database, used for schema initialization. " + socket_option_help,
    required=True,
)
@click.option(
    "--xrootd-manager",
    help="The host name of the xrootd manager node.",
)
@log_cfg_file_option()
@cmd_options()
@click.option(
    "--http-root",
    help="The root folder for the static content to be served by the built-in HTTP service.",
    default="/usr/local/qserv/www",
    show_default=True,
)
@click.option(
    "--qserv-czar-db",
    help="The connection URL to the MySQL server of the Qserv master database."
)
@targs_options()
@run_option()
@options_file_option()
def replication_controller(ctx: click.Context, **kwargs: Any) -> None:
    """Start as a replication controller node."""
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_replication_controller(
        db_uri=targs["db_uri"],
        db_admin_uri=targs["db_admin_uri"],
        log_cfg_file=targs["log_cfg_file"],
        cmd=targs["cmd"],
        run=targs["run"],
    )


@entrypoint.command(
    help=f"Start as a replication registry node.\n\n{socket_option_description}\n\n"
         f"{extended_args_description.format(app='qserv-replica-registry')}",
    cls=EntrypointCommandExArgs,
)
@pass_context
@db_uri_option(
    help="The non-admin URI to the replication systems's database, used for non-smig purposes.",
    required=True,
)
@db_admin_uri_option(
    help="The admin URI to the proxy's database, used for schema initialization. " + socket_option_help,
    required=True,
)
@log_cfg_file_option()
@cmd_options()
@targs_options()
@run_option()
@options_file_option()
def replication_registry(ctx: click.Context, **kwargs: Any) -> None:
    """Start as a replication registry node."""
    targs = utils.targs(ctx)
    targs = render_targs(targs)
    script.enter_replication_registry(
        db_uri=targs["db_uri"],
        db_admin_uri=targs["db_admin_uri"],
        log_cfg_file=targs["log_cfg_file"],
        cmd=targs["cmd"],
        run=targs["run"],
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


@entrypoint.command()
@click.argument("COMMAND")
def spawned_app_help(
    command: str,
) -> None:
    """Print the help output of a spawned command for a given
    entrypoint subcommand or a message indicating why help is not available.

    Help is available for entrypoint commands that spawn an app. If the named
    command does not exist or is misspelled is misnamed the message will say it
    does not exist. Some entrypoint commands perform work without spawning an
    app, in which case it will say that no spawned app help is available for
    that command.
    """
    if command not in commands:
        click.echo(f"{command} is not an entrypoint command.")
        return
    if (cmd := commands[command].default_cmd) is not None:
        script.spawned_app_help(cmd)
    else:
        click.echo(f"No spawned app help is available for {command}.")
