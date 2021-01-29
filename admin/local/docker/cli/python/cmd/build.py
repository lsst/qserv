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
import subprocess


@click.command()
@click.option("--cmake", help="Run the cmake build step.", is_flag=True)
@click.option("--make", help="Run the make build step.", is_flag=True)
@click.option("--container", help="Build the docker image.", is_flag=True)
@click.option("--qserv-run", help="Build the qserv-run container (from within the qserv-build container)",
              is_flag=True)
def build(cmake, make, container, qserv_run):
    """Build qserv.

    If any of --cmake, --make, or --container are passed then only those that
    are passed will be run. If none are passed then all will be run.
    """
    if qserv_run:
        build_qserv_run()
        return

    if not cmake and not make and not container:
        cmake = True
        make = True
        container = True

    if cmake:
        result = subprocess.run(
            ["cmake", ".."],
            cwd="/root/qserv/build"
        )
        result.check_returncode()

    if make:
        result = subprocess.run(
            ["make", "install", "-j4"],
            cwd="/root/qserv/build"
        )
        result.check_returncode()

    if container:
        result = subprocess.run(
            ["docker", "build", "--tag=qserv", "-f", "../../admin/tools/docker/run/Dockerfile", "."],
            cwd="/root/qserv/build/install"
        )
        result.check_returncode()


def build_qserv_run():
    """Build the qserv-run container from within the qserv-build container."""
    # TODO add an option to run from outside the qserv build container?
    result = subprocess.run(
        ["docker", "build", "--target=qserv-run", "--tag=qserv-run", "."],
        cwd="/root/qserv/admin/tools/docker/base"
    )
    result.check_returncode()
