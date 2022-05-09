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

"""Shared options for CLI functions in qserv.
"""


from abc import abstractmethod
import click
from functools import partial
from typing import Callable, List

from .utils import process_targs, yaml_presets


class OptionGroup:
    """Base class for an option group decorator. Requires the option group
    subclass to have a property called `decorator`."""

    @property
    @abstractmethod
    def decorators(self) -> List[Callable]:
        pass

    def __call__(self, f: Callable) -> Callable:
        for decorator in reversed(self.decorators):
            f = decorator(f)
        return f


cmsd_manager_name_option = partial(
    click.option,
    "--cmsd-manager-name",
    help="""The domain name of the cmsd manager node(s).
Will be used to create a domain name like "name-i.name", where "name" is the
name specified here and i is an int 0 < i < --cmsd-manager-count.
""",
)


cmsd_manager_count_option = partial(
    click.option,
    "--cmsd-manager-count",
    help="The number of cmsd manager node(s).",
    type=int,
)


czar_connection_option = partial(
    click.option,
    "--czar-connection",
    help="The czar db connection in format mysql://user:pass@host:port/database.",
)


worker_connection_option = partial(
    click.option,
    "--worker-connection",
    "worker_connections",
    help="The worker db connection(s) in format mysql://user:pass@host:port/database.",
    multiple=True,
)


db_qserv_user_option = partial(
    click.option,
    "--db-qserv-user",
    default="qsmaster",
    help="The user to use for mysql database.",
)


debug_option = partial(
    click.option,
    "--debug",
    "debug_port",
    help="Run gdbserver with the application arguments.",
)


instance_id_option = partial(
    click.option,
    "--instance-id",
    help="The unique identifier of a Qserv instance served by the Replication System.",
    required=True,
)


logLevelChoices = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
log_level_option = partial(
    click.option,
    "--log-level",
    default="INFO",
    type=click.Choice(logLevelChoices, case_sensitive=False),
    callback=lambda ctx, par, val: val.upper(),
    help=f"The logging level. Supported levels are [{'|'.join(logLevelChoices)}]",
)


mysql_monitor_password_option = partial(
    click.option,
    "--mysql-monitor-password",
    help="The password for monitoring applications.",
)


repl_connection_option = partial(
    click.option,
    "--repl-connection",
    help="The connection string for the replication database in format mysql://user:pass@host:port/database",
)


run_option = partial(
    click.option,
    "--run/--no-run",
    is_flag=True,
    default=True,
    help="Run the subcommand that is executed by entrypoint."
    "For --no-run, will print the command and arguments that would have "
    "been run.",
)


vnid_config_option = partial(
    click.option,
    "--vnid-config",
    help="The config parameters used by the qserv cmsd to get the vnid from the specified "
    " source (static string, a file or worker database)."
)


xrootd_manager_option = partial(
    click.option,
    "--xrootd-manager",
    help="The host name of the xrootd manager.",
)


tests_yaml_option = partial(
    click.option,
    "--tests-yaml",
    help="Path to the yaml that contains settings for integration test execution.",
    default="/usr/local/etc/integration_tests.yaml",
    show_default=True,
)


case_option = partial(
    click.option,
    "--case",
    "cases",
    help="Run this/these test cases only. If omitted will run all the cases.",
    multiple=True,
)


load_option = partial(
    click.option,
    "--load/--no-load",
    help=(
        "Force qserv_testdata to be loaded or not loaded into qserv and the reference database. "
        "Will handle --unload first. "
        "By default, if --unload is passed will not load databases, "
        "otherwise will load test databases that are not loaded yet."
    ),
    is_flag=True,
    default=None,
)


unload_option = partial(
    click.option,
    "--unload",
    help="Unload qserv_testdata from qserv and the reference database. Will be handled before --load.",
    is_flag=True,
)


reload_option = partial(
    click.option,
    "--reload",
    help="Remove and reload testdata. Same as passing --unload --load.",
    is_flag=True,
)


run_tests_option = partial(
    click.option,
    "--run-tests/--no-run-tests",
    help="Run or skip test execution. Defaults to --run-tests.",
    is_flag=True,
    default=True,
)


compare_results_option = partial(
    click.option,
    "--compare-results/--no-compare-results",
    help="Run or skip query output comparison. Defaults to --compare-results",
    is_flag=True,
    default=True,
)


db_uri_option = partial(
    click.option,
    "--db-uri",
    help="The URI to the database."
)


db_admin_uri_option = partial(
    click.option,
    "--db-admin-uri",
    help="The admin URI to the database."
)


options_file_option = partial(
    click.option,
    "--options-file", "-@",
    expose_value=False,  # This option should not be forwarded
    help="""Path to a YAML file containing overrides of command line options.
The YAML should be organized as a hierarchy with subcommand names at the top
level, options names and values (as key-value pairs) for that subcommand
below. The option name should NOT include prefix dashes.""",
    callback=yaml_presets,
)


cmd_option = partial(
    click.option,
    "--cmd",
    help="""The command template (in jinja2 format) that will be used to call
    the command that this function executes.""",
    show_default=True,
)


targs_option = partial(
    click.option,
    "--targs",
    help="""Key=value pairs to be applied to templates. Value may be a comma-separated list.""",
    multiple=True,
    callback=process_targs,
)


targs_file_option = partial(
    click.option,
    "--targs-file",
    help="""Path to a yaml file that contains key-value pairs to apply to templates.""",
)


class targs_options(OptionGroup):  # noqa: N801

    @property
    def decorators(self) -> List[Callable]:
        return [
            targs_option(),
            targs_file_option(),
        ]


log_cfg_file_option = partial(
    click.option,
    "--log-cfg-file",
    help="Path to the log4cxx config file.",
    default="/config-etc/log/log.cnf",
    show_default=True,
)


repl_auth_key_option = partial(
    click.option,
    "--repl-auth-key",
    help="The authorizaiton key for the replication-ingest system."
)


repl_admin_auth_key_option = partial(
    click.option,
    "--repl-admin-auth-key",
    help="The admin authorizaiton key for the replication-ingest system."
)
