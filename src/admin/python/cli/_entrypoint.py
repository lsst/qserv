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

"""Launcher for Qserv components."""

import click
import os
import subprocess

from lsst.qserv.schema import smig
from ..template import save_template_cfg


smig_dir = "/usr/local/qserv/smig"
admin_smig_dir = "admin/schema"
qmeta_smig_dir = "qmeta/schema"


def smigAdmin(connection):
    """Apply admin schema migration scripts to a database.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database.
    """
    smig(
        verbose=False,
        do_migrate=True,
        check=False,
        final=None,
        scripts=os.path.join(smig_dir, admin_smig_dir),
        connection=connection,
        config_file=None, # could use this instead of connection string
        config_section=None, # goes with config_file
        module="admin",
    )


def smigCzar(connection):
    smigAdmin(connection)
    # smigQmeta(connection)


def enterCzar(connection):
    smigCzar(connection)


@click.group()
def entrypoint():
    pass

def split_kv(ctx, param, values):
    """Split muliple groups of comma-separated key=value pairs into a dict.

    Parameters
    ----------
    context : `click.Context` or `None`
        The current execution context. Unused, but Click always passes it to
        callbacks.
    param : `click.core.Option` or `None`
        The parameter being handled. Unused, but Click always passes it to
        callbacks.
    values : `list` [`str`]
        Each item in the list should be a string in the form "key=value" or
        "key=value,key=value,..."
    """
    if not values:
        return
    pairs = ",".join(values).split(",")
    return dict(val.split("=") for val in pairs)


@entrypoint.command()
@click.option("--connection",
              help="Connection string in format user:pass@host:port/database.")
@click.option("--scheme",
              help="The scheme part of the connection string.",
              default="mysql+mysqlconnector")
@click.option("-t", "--template",
              help="Key-value pairs to insert into templated files.",
              metavar="key=value,...",
              multiple=True,
              callback=split_kv)
def czar(scheme, connection, template):
    save_template_cfg(template)
    connection = f"{scheme}://{connection}"
    enterCzar(connection)


def enterWorker():
    launchXrootd(worker_node_name)
    launchCmsd(czar_node_name)


def enterMariadb():
    launchMaraidb()
    launchMysqld()
    # launchWatcher?
    # launchWmgr?

