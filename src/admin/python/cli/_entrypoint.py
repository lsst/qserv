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

from click.exceptions import ClickException

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
from . import script, _integration_test
from .utils import split_kv
from ..template import save_template_cfg


_log = logging.getLogger(__name__)


@click.group()
@log_level_option()
def entrypoint(log_level):
    logging.basicConfig(level=log_level)


@entrypoint.command()
def load_simple(**kwargs):
    "Load a small test dataset into qserv."
    _load.load_simple(**kwargs)


@entrypoint.command()
@click.option(
    "--tests-yaml",
    help="Path to the yaml that contains settings for integration test execution.",
    default="/usr/local/etc/integration_tests.yaml",
    show_default=True,
)
@click.option(
    "--case", "cases",
    help="Run this/these test cases only. If omitted will run all the cases.",
    multiple=True,
)
@click.option(
    "--pull",
    help="Pull a new copy of qserv_testdata. Will remove the old copy if it exists.",
    is_flag=True,
)
@click.option(
    "--load",
    help="Load qserv_testdata into qserv and the reference database. Will run after unload if both are passed.",
    is_flag=True,
)
@click.option(
    "--unload",
    help="Unload qserv_testdata from qserv and the reference database. Will run before load if both are passed.",
    is_flag=True,
)
@click.option(
    "--reload",
    help="Remove and reload testdata. Same as passing '--unload --load'.",
    is_flag=True,
)
@click.option(
    "--run-tests/--no-run-tests",
    help="Run or skip test execution. Defaults to '--run-tests'.",
    is_flag=True,
    default=True,
)
@click.option(
    "--compare-results/--no-compare-results",
    help="Run or skip query output comparison. Defaults to '--compare-results'",
    is_flag=True,
    default=True,
)
def integration_test(**kwargs):
    """Run integration tests using ingested test data.

    TESTS_YAML is the yaml file paths that contains connection information & describes tests to load and run.
    """
    success = _integration_test.run_integration_tests(**kwargs)
    if success == False:
        raise ClickException("Tests failed.")
    if success == True:
        _log.info("Tests passed.")
    if success == None:
        _log.info("Did not compare test output.")


@entrypoint.command()
@click.argument("DATABASE", nargs=1)
@click.option(
    "--table", "table_file",
    help="The json file that contains the table configuration or a "
         "folder of json files named `<TableName>.json`.",
)
@click.option(
    "--chunks", "chunks_folder",
    help="If the --table is a single .json file then --chunks must "
         "indicate the folder that contains the chunk info json and "
         "the chunk files. If --table is a folder of .json files "
         "then --chunks must be a folder of folders whose names "
         "match the name of the `<TableName>.json files that contain "
         "the chunk info json and chunk files."
)
def ingest_table(**kwargs):
    """Ingest table data prepared by the qserv partitioner.

    DATABASE is the path to the databse json file.
    """
    _load.ingest_table(**kwargs)


@entrypoint.command()
@click.argument("DATABASE", nargs=-1)
@click.option(
    "--admin",
    help="Use the admin insetad of user auth key.",
    is_flag=True
)
def delete_database(**kwargs):
    """Remove a database. !!DOES NOT VERIFY!!

    This deletes a named database from a qserv instance.
    Does not verify or ask the user to confirm

    IN THE FUTURE we should add a credential check.

    DATABASE is the name of the database to remove."
    _load.delete_database(**kwargs)
    """
    _load.delete_database(**kwargs)


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
