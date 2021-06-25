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


import click
from functools import partial

from .utils import OptionDecorator, split_kv


cmsd_manager_option = partial(
    click.option,
    "--cmsd-manager",
    help="The host name of the cmsd manager."
)


connection_option = partial(
    click.option,
    "--connection",
    help="Connection string in format user:pass@host:port/database.",
)


db_qserv_user_option = partial(
    click.option,
    "--db-qserv-user",
    default="qsmaster",
    help="The user to use for mysql database.",
)


db_scheme_option = partial(
    click.option,
    "--db-scheme",
    help="The scheme part of the connection string.",
    default="mysql+mysqlconnector",
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
log_level_option = OptionDecorator(
    "--log-level",
    default="INFO",
    type=click.Choice(logLevelChoices, case_sensitive=False),
    callback=lambda ctx, par, val: val.upper(),
    help=f"The logging level. Supported levels are [{'|'.join(logLevelChoices)}]",
)


mysql_monitor_password_option = partial(
    click.option,
    "--mysql-monitor-password",
    help="The password for monitoring applications."
)


mysql_user_qserv_option = partial(
    click.option,
    "--mysql-user-qserv",
    help="The qserv db user."
)


repl_connection_option = partial(
    click.option,
    "--repl-connection",
    help="The connection string for the replication database."
)


repl_ctrl_domain_name_option = partial(
    click.option,
    "--repl-ctl-dn",
    help="The fully qualified domain name of the replication controller."
)


repl_ctrl_port_option = partial(
    click.option,
    "--repl-ctl-port",
    help="The port that the replication controller is listening on."
)


run_option = partial(
    click.option,
    "--run/--no-run",
    is_flag=True,
    default=True,
    help="Run the subcommand that is executed by entrypoint.."
         "For --no-run, will print the command and arguments that would have "
         "been run.")


vnid_option = partial(
    click.option,
    "--vnid",
    help="The virtual network identifier for this component.",
)


xrd_port_option = partial(
    click.option,
    "--xrd-port",
    help="The port for the xrd.port xrootd configuration file.",
    type=int,
)


xrootd_manager_option = partial(
    click.option,
    "--xrootd-manager",
    help="The host name of the xrootd manager."
)
