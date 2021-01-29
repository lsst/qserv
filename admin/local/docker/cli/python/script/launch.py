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


import os
import subprocess
import time


FILE_DIR = os.path.dirname(__file__)


mysqlRootPwd = "CHANGEME"


def decodeDockerOutput(output):
    output = output.decode("utf-8").strip()
    output = output.split("\n")
    return [o for o in output if bool(o)]


def getContainerHealthStatus(containerName):
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}",
            containerName,
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    return decodeDockerOutput(result.stdout)[0]


def getContainerName(containerId):
    """Use the container id to get the container name.

    Parameters
    ----------
    containerId : `str`
        The id of the container.

    Returns
    -------
    name : `str`
        The name of the container.
    """
    # Get the name of the container using the container id:
    result = subprocess.run(
        ["docker", "inspect", containerId, "--format", "{{.Name}}"],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
    return result.stdout.decode("utf-8").strip().lstrip("/")


def start_mariadb():
    """Launch a mariadb-scisql container.

    Returns
    -------
    name : `str`
        The name of the mariadb container that was started.
    """
    cnfPath = os.path.abspath(
        os.path.join(FILE_DIR, "../../../../../../src/admin/templates/mariadb/etc/my.cnf")
    )
    result = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "-e",
            f"MYSQL_ROOT_PASSWORD={mysqlRootPwd}",
            "--mount",
            f"type=bind,source={cnfPath},target=/etc/mysql/my.cnf,readonly",
            "--net=host",
            "mariadb-scisql",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        result.check_returncode()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(result.stdout + result.stderr) from e

    containerId = result.stdout.decode("utf-8").strip()
    containerName = getContainerName(containerId)

    for i in range(20):
        result = subprocess.run(
            [
                "docker",
                "exec",
                containerName,
                "mysqladmin",
                "ping",
                "-pCHANGEME",
                "--silent",
            ],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        if result.returncode == 0:
            if i > 0:
                print()
            break
        time.sleep(1)
        if i == 0:
            print("Waiting for mysql to start")
        else:
            print(".")
            # print(".", end="")  black is having trouble with this needs investigation.

    return containerName


def stop_mariadb(container_names):
    """Stop a running mariadb-scisql container.

    If only one container using that image is running then the contaner can be
    identified automatically. If more than one is running then a name must be
    provided.

    Parameters
    ----------
    container_name : `str` or `None`
        The name of the container to stop, or `None` if the container should be
        discovered automatically.

    Returns
    -------
    names : `list` [`str`]
        The names of the containers that were stopped.
    """
    if not container_names:
        container_names = list_mariadb()
        if not container_names:
            raise RuntimeError(f"Could not find a mariadb-scisql container to stop.")
    result = subprocess.run(
        ["docker", "stop"] + container_names,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
    return container_names


def list_mariadb():
    """Get the names of running mariadb-scisql containers.

    Returns
    -------
    names : `list` [`str`]
        The names of the running mariadb-scisql containers.
    """
    result = subprocess.run(
        [
            "docker",
            "container",
            "ls",
            "-f",
            "ancestor=mariadb-scisql",
            "--format",
            "{{.Names}}",
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
    return decodeDockerOutput(result.stdout)


def start_czar(dbHost, dbPort):
    """Start a container running the qserv Czar.

    Parameters
    ----------
    dbHost : `str`
        The host ip address or name of the mariadb instance hosting Czar
        databases.
    dbPort : `str`
        The port number of the mariadb instance hosting Czar databases.
    """
    result = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--net=host",
            "qserv",
            "entrypoint",
            "czar",
            "--connection",
            # don't include scheme ("mysql+mysqlconnector://") in connection string here. TODO: WHY?
            f"root:{mysqlRootPwd}@{dbHost}:{dbPort}",
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result.check_returncode()


def start_qserv_build_container(root_volume):
    """Start a container running the qserv build container.

    Parameters
    ----------
    root_volume : `str`
        The name of the volume to mount at ``/root``.

    Returns
    -------
    containerName : `str`
        The name of the container that was started.
    """

    result = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--init",
            "-v",
            f"{root_volume}:/root",
            "-v",
            "/var/run/docker.sock:/var/run/docker.sock",
            "qserv-build",
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
    containerId = result.stdout.decode("utf-8").strip()
    return getContainerName(containerId)
