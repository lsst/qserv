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


import click

from ..script import launch


def printFormatList(list):
    return ", ".join(list[:-2] + [" and ".join(list[-2:])])


@click.group()
def qserv():
    pass


@qserv.command()
def start_mariadb():
    """Launch a mariadb-scisql container."""
    containerId = launch.start_mariadb()
    click.echo(f"Launched maraidb-scisql container {containerId}.")


@qserv.command()
@click.argument("container_name", nargs=-1)
def stop_mariadb(container_name):
    """Stop a mariadb container.

    CONTAINER_NAME is the name of the mariadb-scisql container name to stop."

    If only one mariadb-scisql container is running then the container name can
    be discovered automatically. If more than one is running then the name must
    be provided.
    """
    stoppedContainers = launch.stop_mariadb(container_name)
    click.echo(f"Stopped {printFormatList(stoppedContainers)}.")


@qserv.command()
def list_mariadb():
    """Get the names of the running mariadb containers."""
    names = printFormatList(launch.list_mariadb())
    click.echo(names or "No running mariadb-scisql containers.")


@qserv.command()
def start_czar():
    launch.start_czar(dbHost="localhost", dbPort="3306")


@qserv.command()
@click.option(
    "--root-volume",
    default="qserv-build-root",
    help="The name of the volume to mount at /root",
)
def start_qserv_build_container(**kwargs):
    print(launch.start_qserv_build_container(**kwargs))
