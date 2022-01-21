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

from collections import namedtuple
import getpass
import grp
import logging
import os
import pwd
import subprocess
import time
from urllib.parse import urlparse
import yaml
from typing import Dict, List, Optional, Sequence

from .opt import (
    dashboard_port_ev,
    mariadb_image_ev,
    qserv_image_ev,
    dh_user_ev,
    dh_token_ev,
)
from . import images


# The location where the lite-build and run-base images should be built from:
base_image_build_subdir = "admin/tools/docker/base"
user_build_image_subdir = "admin/tools/docker/build-user"
run_image_build_subdir = "admin/tools/docker/run"
mariadb_image_subdir = "admin/tools/docker/mariadb"
mypy_cfg_file = "src/admin/python/mypy.ini"


_log = logging.getLogger(__name__)


def do_pull_image(image_name: str, dh_user: Optional[str], dh_token: Optional[str], dry: bool) -> bool:
    """Attempt to pull an image. If valid dockerhub credentials are provided
    check the registry for the image first. If they are not provided then just
    try to pull the image and accept any failure as 'image does not exist'.

    Parameters
    ----------
    image_name : `str`
        The name of the image to pull.
    dh_user : `Optional[str]`
        The name of the dockerhub user, or None.
    dh_token : `Optional[str]`
        The dockerhub user's token, or None.
    dry : `bool`
        If True do not run the command; print what would have been run.

    Returns
    -------
    pulled : `bool`
        True if the images was pulled, else False.
    """
    did_pull = False
    if (dh_user and dh_token and images.dh_image_exists(image_name, dh_user, dh_token)) or (
        not dh_user and not dh_token
    ):
        did_pull = images.dh_pull_image(image_name, dry)
    _log.debug("%s %s", "Pulled" if did_pull else "Could not pull", image_name)
    return did_pull


def root_mount(qserv_root: str, qserv_build_root: str, user: str) -> str:
    """Get the value for the `docker run --mount` option.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv folder.
    qserv_build_root : `str`
        The location inside the container to mount the qserv folder.
    user : `str`
        If `qserv_build_root` has a formattable string with a `user` field, this
        value will be substituted into that field.

    Returns
    -------
    bind : `str`
        The value for the `--bind` option.
    """
    qserv_build_root = qserv_build_root.format(user=user)
    return f"src={qserv_root},dst={qserv_build_root},type=bind"


def add_network_option(args: List[str], project: str) -> None:
    """If project is not None, add a network option to a list of subprocess
    arguments for running a docker process.

    Parameters
    ----------
    args : `list` [ `str` ]
        The list of arguments to append the network option to.
    project : `str`
        The project name that is used to derive a network name, follows
        docker-compose conventions, so if the project name is "foo" then the
        network name will be "foo_default".
    """
    # this is the network name given to compose clusters when docker-compose is run with the -p option:
    args.append(f"--network={project}_default")


def build_dir(qserv_root: str) -> str:
    """Get the build directory in the build container.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv sources (may be on the host machine or in a build
        container).

    Returns
    -------
    build_dir : `str`
        The path to the build folder.
    """
    return os.path.join(qserv_root, "build")


def submodules_initalized(qserv_root: str) -> bool:
    """Perform a very simple check to see if "git submodule update --init" has been
    run yet.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv sources (may be on the host machine or in a build
        container).

    Returns
    -------
    initialized : `bool`
        True if the submodules appear to have been initialized already.
    """
    # This is a file that does not exist in a fresh pull of qserv, and will be
    # populated when "git submodule update --init" is run.
    f = "extern/sphgeom/CMakeLists.txt"
    return os.path.exists(os.path.join(qserv_root, f))


def do_update_submodules(
    qserv_root: str, qserv_build_root: str, build_image: str, user: str, dry: bool
) -> None:
    """Run 'git update submodules'.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    qserv_build_root : `str`
        The path to mount the qserv source folder inside the build container.
    build_image : `str`
        The name of the build image to use to run cmake.
    user : `str`
        The name of the user to run the build container as.
    dry : `bool`
        If True do not run the command; print what would have been run.
    """
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
        "-u",
        user,
        "--mount",
        root_mount(qserv_root, qserv_build_root, user),
        "-w",
        qserv_build_root.format(user=user),
        build_image,
        "git",
        "submodule",
        "update",
        "--init",
    ]
    if dry:
        print(" ".join(args))
        return
    _log.debug('Running "%s"', " ".join(args))
    subprocess.run(args, check=True)


def cmake(qserv_root: str, qserv_build_root: str, build_image: str, user: str, dry: bool) -> None:
    """Run cmake on qserv.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    qserv_build_root : `str`
        The path to mount the qserv source folder inside the build container.
    build_image : `str`
        The name of the build image to use to run cmake.
    user : `str`
        The name of the user to run the build container as.
    dry : `bool`
        If True do not run the command; print what would have been run.
    """
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
        "-u",
        user,
        "--mount",
        root_mount(qserv_root, qserv_build_root, user),
        "-w",
        build_dir(qserv_build_root.format(user=user)),
        build_image,
        "cmake",
        "..",
    ]
    if dry:
        print(" ".join(args))
        return
    _log.debug('Running "%s"', " ".join(args))
    subprocess.run(args, check=True)


def make(
    qserv_root: str,
    qserv_build_root: str,
    unit_test: bool,
    build_image: str,
    user: str,
    dry: bool,
    jobs: Optional[int],
) -> None:
    """Make qserv (but do not build the qserv image).

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    qserv_build_root : `str`
        The path to mount the qserv source folder inside the build container.
    unit_test : `bool`
        True if unit tests should be run.
    build_image : `str`
        The name of the image to use to run make.
    user : `str`
        The name of the user to run the build container as.
    dry : `bool`
        If True do not run the command; print what would have been run.
    jobs : `int` or `None`
        The number of make recipes to run at once (same as the make -j option).
    """
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
        "-u",
        user,
        "--mount",
        root_mount(qserv_root, qserv_build_root, user),
        "-w",
        build_dir(qserv_build_root.format(user=user)),
        build_image,
        "make",
    ]
    if jobs:
        args.append(f"-j{jobs}")
    args.append("install")
    if unit_test:
        args.append("test")
    if jobs:
        args.append(f"ARGS=-j{jobs}")
    if dry:
        print(" ".join(args))
        return
    _log.debug('Running "%s"', " ".join(args))
    subprocess.run(args, check=True)


def mypy(
    qserv_root: str,
    qserv_build_root: str,
    build_image: str,
    user: str,
    dry: bool,
) -> None:
    """Run mypy on python files.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    qserv_build_root : `str`
        The path to mount the qserv source folder inside the build container.
    build_image : `str`
        The name of the image to use to run make.
    user : `str`
        The name of the user to run the build container as.
    dry : `bool`
        If True do not run the command; print what would have been run.
    """
    # qserv_py_modules is relative to the build folder inside the build container:
    qserv_py_modules = "install/python/lsst/qserv"
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
        "-u",
        user,
        "--mount",
        root_mount(qserv_root, qserv_build_root, user),
        "-w",
        build_dir(qserv_build_root.format(user=user)),
        build_image,
        "mypy",
        qserv_py_modules,
        "--exclude",
        "testing",
    ]
    mypy_ini_file = os.path.join(qserv_build_root.format(user=user), mypy_cfg_file)
    args.extend(["--config-file", mypy_ini_file])
    if dry:
        print(" ".join(args))
        return
    _log.debug('Running "%s"', " ".join(args))
    print("Running mypy on all qserv python modules except 'testing'...")
    subprocess.run(args, check=True)


def build(
    qserv_root: str,
    qserv_build_root: str,
    unit_test: bool,
    dry: bool,
    jobs: Optional[int],
    run_cmake: bool,
    run_make: bool,
    run_mypy: bool,
    user_build_image: str,
    qserv_image: str,
    run_base_image: str,
    do_build_image: bool,
    push_image: bool,
    pull_image: bool,
    update_submodules: bool,
    user: str,
) -> None:
    """Build qserv and a new lite-qserv image.

    Parameters
    ----------
    qserv_root
    qserv_build_root
    unit_test
    dry
    jobs
        Same as the arguments to `make`
    run_cmake : `bool` or None
        True if cmake should be run, False if not, or None if cmake should be
        run if it has not been run before, determened by the absence/presence
        of the build direcetory.
    run_make : `bool`
        True if `make` should be called.
    run_mypy : `bool`
        True if `mypy` should be run.
    user_build_image : `str`
        The name of the lite-build image to use to run cmake and make.
    qserv_image : `str`
        The name of the image to create.
    run_base_image : `str`
        The name of the lite-run-base image to use as a base for the lite-run image.
    do_build_image : `bool`
        True if a qserv run image should be created.
    push_image : `bool`
        True if the lite-qserv image should be pushed to dockerhub.
    pull_image: `bool`
        Pull the lite-qserv image from dockerhub if it exists there.
    update_submodules : `bool`
        True if "git update submodules" should be run, False if Not, or None
        if it should be run if it has not been run before, determined by the
        absence/presense of a file populated by running it.
    user : `str`
        The name of the user to run the build container as.
    """
    if pull_image and do_pull_image(qserv_image, dh_user_ev.val(), dh_token_ev.val(), dry):
        return

    if update_submodules is True or (update_submodules is None and not submodules_initalized(qserv_root)):
        do_update_submodules(qserv_root, qserv_build_root, user_build_image, user, dry)

    if run_cmake is True or (run_cmake is None and not os.path.exists(build_dir(qserv_root))):
        # Make sure the build dir exists because it will be the working dir of a
        # future `docker run` command, and if it does not exist it will be
        # created but the owner will be root, and we need the owner to be the
        # same as `user`.
        os.makedirs("build", exist_ok=True)
        cmake(qserv_root, qserv_build_root, user_build_image, user, dry)

    if run_make:
        make(qserv_root, qserv_build_root, unit_test, user_build_image, user, dry, jobs)

    if run_mypy:
        mypy(qserv_root, qserv_build_root, user_build_image, user, dry)

    if not do_build_image:
        return

    images.build_image(
        image_name=qserv_image,
        run_dir=os.path.join(qserv_root, "build", "install"),
        dockerfile=os.path.join(qserv_root, run_image_build_subdir, "Dockerfile"),
        options=["--build-arg", f"QSERV_RUN_BASE={run_base_image}"] if run_base_image else None,
        target=None,
        dry=dry,
    )

    if push_image:
        images.dh_push_image(qserv_image, dry)


def build_build_image(
    build_image: str, qserv_root: str, dry: bool, push_image: bool, pull_image: bool
) -> None:
    """Build the build image.

    Parameters
    ----------
    build_image : `str`
        The name of the build image to make.
    qserv_root : `str`
        The path to the qserv source folder.
    dry : `bool`
        If True do not run the command; print what would have been run.
    push_image : `bool`
        True if the lite-qserv image should be pushed to dockerhub.
    pull_image: `bool`
        Pull the lite-qserv image from dockerhub if it exists there.
    """
    if pull_image and do_pull_image(build_image, dh_user_ev.val(), dh_token_ev.val(), dry):
        return
    images.build_image(
        image_name=build_image,
        target="lite-build",
        run_dir=os.path.join(qserv_root, base_image_build_subdir),
        dry=dry,
    )
    if push_image:
        images.dh_push_image(build_image, dry)


def build_user_build_image(
    qserv_root: str, build_image: str, user_build_image: str, group: str, dry: bool
) -> None:
    """Build the user-build image."""
    user_info = pwd.getpwnam(getpass.getuser())
    args = [
        "docker",
        "build",
        "--build-arg",
        f"QSERV_BUILD_BASE={build_image}",
        "--build-arg",
        f"USER={user_info.pw_name}",
        "--build-arg",
        f"UID={user_info.pw_uid}",
        "--build-arg",
        f"GROUP={group or grp.getgrgid(user_info.pw_gid).gr_name}",
        "--build-arg",
        f"GID={user_info.pw_gid}",
        f"--tag={user_build_image}",
        ".",
    ]
    run_dir = os.path.join(qserv_root, user_build_image_subdir)
    if dry:
        print(f"cd {run_dir}; {' '.join(args)}; cd -")
    else:
        _log.debug('Running "%s" from directory %s', " ".join(args), run_dir)
        subprocess.run(args, cwd=run_dir, check=True)


def build_run_base_image(
    run_base_image: str, qserv_root: str, dry: bool, push_image: bool, pull_image: bool
) -> None:
    """Build the qserv lite-run-base image."""
    if pull_image and do_pull_image(run_base_image, dh_user_ev.val(), dh_token_ev.val(), dry):
        return
    images.build_image(
        image_name=run_base_image,
        target="lite-run-base",
        run_dir=os.path.join(qserv_root, base_image_build_subdir),
        dry=dry,
    )
    if push_image:
        images.dh_push_image(run_base_image, dry)


def build_mariadb_image(
    mariadb_image: str, qserv_root: str, dry: bool, push_image: bool, pull_image: bool
) -> None:
    """Build the mariadb image."""
    if pull_image and do_pull_image(mariadb_image, dh_user_ev.val(), dh_token_ev.val(), dry):
        return
    images.build_image(
        image_name=mariadb_image,
        run_dir=os.path.join(qserv_root, mariadb_image_subdir),
        target=None,
        dry=dry,
    )
    if push_image:
        images.dh_push_image(mariadb_image, dry)


def bind_args(qserv_root: str, bind_names: List[str]) -> List[str]:
    """Get the options for `docker run` to bind locations in qserv run container to locations in the
    built products.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    bind_names : List[str]
        The name(s) of the locations to be bound, or "all"

    Returns
    -------
    options : List[str]
        The flags and options to pass to `docker run`
    """
    SrcDst = namedtuple("SrcDst", "src dst")
    src_dst = dict(
        python=SrcDst(os.path.join(qserv_root, "build", "install", "python"), "/usr/local/python/"),
        bin=SrcDst(os.path.join(qserv_root, "build", "install", "bin"), "/usr/local/bin/"),
        lib64=SrcDst(os.path.join(qserv_root, "build", "install", "lib64"), "/usr/local/lib64/"),
        lua=SrcDst(os.path.join(qserv_root, "build", "install", "lua"), "/usr/local/lua/"),
        qserv=SrcDst(os.path.join(qserv_root, "build", "install", "qserv"), "/usr/local/qserv/"),
        etc=SrcDst(os.path.join(qserv_root, "build", "install", "etc"), "/usr/local/etc/"),
    )
    flag = "--mount"
    val = "src={src},dst={dst},type=bind"
    locations = (
        src_dst.values()
        if "all" in bind_names
        else [sd for name, sd in src_dst.items() if name in bind_names]
    )
    return [t.format(src=loc.src, dst=loc.dst) for loc in locations for t in (flag, val)]


def run_dev(
    qserv_root: str,
    test_container: str,
    qserv_image: str,
    bind: List[str],
    project: str,
    dry: bool,
) -> str:
    """Launch a lite-qserv container for iterative developement testing.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    test_container : `str`
        The name to give the container.
    qserv_image : `str`
        The name of the image to run.
    bind : List[`str`]
        Any of ["all", "python", "bin", "lib64", "lua", "qserv", "etc"].
        If provided, selected build artifact directories will be bound into
        their install locations in the container. If "all" is provided then all
        the locations will be bound. Allows for local iterative build & test
        without having to rebuild the docker image.
    dry : `bool`
        If True do not run the command; print what would have been run.
    project : `str`
        The name used for qserv instance customizations.

    Returns
    -------
    container_name : `str`
        The name of the container that was launched (or if dry == True the name
        of the contianer that would have been launched).
    """
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
        "--name",
        test_container,
        "-it",
    ]
    if bind:
        args.extend(bind_args(qserv_root=qserv_root, bind_names=bind))
    add_network_option(args, project)
    args.extend([qserv_image, "/bin/bash"])
    if dry:
        print(" ".join(args))
    else:
        _log.debug('Running "%s"', " ".join(args))
        subprocess.run(args, check=True)
    return test_container


def run_build(
    qserv_root: str,
    build_container_name: str,
    qserv_build_root: str,
    user_build_image: str,
    user: str,
    dry: bool,
) -> None:
    """Same as qserv_cli.run_build"""
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
        "-it",
        "--name",
        build_container_name,
        "-u",
        user,
        "--mount",
        root_mount(qserv_root, qserv_build_root, user),
        "-w",
        build_dir(qserv_build_root.format(user=user)),
        user_build_image,
        "/bin/bash",
    ]
    if dry:
        print(" ".join(args))
    else:
        _log.debug('Running "%s"', " ".join(args))
        subprocess.run(args, check=True)


def run_debug(
    container_name: str,
    image: str,
    project: str,
    dry: bool,
) -> None:
    """Same as qserv_cli.run_debug"""
    args = [
        "docker",
        "run",
        "-it",
        "--rm",
        f"--pid=container:{container_name}",
        "--cap-add",
        "sys_admin",
        "--cap-add",
        "sys_ptrace",
    ]
    add_network_option(args, project)
    args.extend(
        [
            image,
            "/bin/bash",
        ]
    )

    if dry:
        print(" ".join(args))
    else:
        _log.debug('Running "%s"', " ".join(args))
        subprocess.run(args, check=True)


def itest_ref(
    qserv_root: str,
    itest_file: str,
    itest_volume: str,
    project: str,
    mariadb_image: str,
    dry: bool,
) -> str:
    """Launch the reference database used by integration tests.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    itest_file : `str` or `None`
        The path to the yaml file that contains integration test execution data.
    itest_volume : `str`
        The name of the volume that holds integration test data. Also used to
        derive other database volume names.
    project : `str`
        The name used for qserv instance customizations.
    mariadb_image : `str`
        The name of the database image to run.
    dry : `bool`
        If True do not run the command; print what would have been run.

    Returns
    -------
    container_name : `str`
        The name of the container that was launched (or if dry == True the name
        of the contianer that would have been launched).
    """
    with open(itest_file) as f:
        tests_data = yaml.safe_load(f.read())
    ref_db = urlparse(tests_data["reference-db-uri"])
    hostname = str(ref_db.hostname)
    cnf_src = os.path.join(qserv_root, "src/admin/templates/integration-test/etc/my.cnf")

    args = [
        "docker",
        "run",
        "--init",
        "-d",
        "--name",
        hostname,
        "--expose",
        str(ref_db.port),
        "--mount",
        f"src={itest_volume},dst=/qserv/data,type=volume",
        "--mount",
        f"src={cnf_src},dst=/etc/mysql/my.cnf,type=bind",
        "--mount",
        f"src={itest_volume}_lib,dst=/var/lib/mysql,type=volume",
        "-e",
        "MYSQL_ROOT_PASSWORD=CHANGEME",
    ]
    add_network_option(args, project)
    args.extend(
        [
            mariadb_image,
            "--port",
            str(ref_db.port),
        ]
    )
    if dry:
        print(" ".join(args))
        return hostname
    _log.debug(f"Running {' '.join(args)}")
    result = subprocess.run(args)
    result.check_returncode()
    return hostname


def stop_itest_ref(container_name: str, dry: bool) -> None:
    """Stop the integration test reference database.

    Parameters
    ----------
    container_name : `str`
        The name of the container running the integration test reference
        database.
    dry : `bool`
        If True do not run the command; print what would have been run.
    """
    args = ["docker", "rm", "-f", container_name]
    if dry:
        print(" ".join(args))
        return
    _log.debug(f"Running {' '.join(args)}")
    result = subprocess.run(args)
    result.check_returncode()


def integration_test(
    qserv_root: str,
    itest_container: str,
    itest_volume: str,
    qserv_image: str,
    bind: List[str],
    itest_file: str,
    dry: bool,
    project: str,
    pull: Optional[bool],
    unload: bool,
    load: Optional[bool],
    reload: bool,
    cases: List[str],
    run_tests: bool,
    tests_yaml: str,
    compare_results: bool,
    wait: int,
    remove: bool,
) -> int:
    """Run integration tests.

    Parameters
    ----------
    qserv_root : `str`
        The path to the qserv source folder.
    itest_container : `str`
        The name to give the container.
    itest_volume : `str`
        The name of the volume used to host integration test data.
    qserv_image : `str`
        The name of the image to run.
    bind : `List[str]`
        One of ["all", "python", "bin", "lib64", "lua", "qserv", "etc"].
        If provided, selected build artifact directories will be bound into
        their install locations in the container. If "all" is provided then all
        the locations will be bound. Allows for local iterative build & test
        without having to rebuild the docker image.
    itest_file : `str`
        The path to the yaml file that contains integration test execution data.
    dry : `bool`
        If True do not run the command; print what would have been run.
    project : `str`
        The name used for qserv instance customizations.
    pull : Optional[bool]
        True forces pull of a new copy of qserv_testdata, False prohibits it.
        None will pull if testdata has not yet been pulled. Will remove the old
        copy if it exists. Will be handled before `load` or `unload.
    unload : bool
        If True, unload qserv_testdata from qserv and the reference database.
    load : Optional[bool]
        Force qserv_testdata to be loaded (if True) or not loaded (if False)
        into qserv and the reference database. Will handle `unload` first. If
        `load==None` and `unload` is passed will not load databases, otherwise
        will load test databases that are not loaded yet.
    reload : bool
        Remove and reload test data. Same as passing `unload=True` and `load=True`.
    cases : List[str]
        Run this/these test cases only. If list is empty list will run all the cases.
    run_tests : bool
        If False will skip test execution.
    tests_yaml : str
        Path to the yaml that contains settings for integration test execution.
    compare_results : bool
        If False will skip comparing test results.
    wait : `int`
        How many seconds to wait before launching the integration test container.
    remove : `bool`
        True if the containers should be removed after executing tests.

    Returns
    -------
    returncode : `int`
        The returncode of "entrypoint integration-test".
    """
    if wait:
        _log.info(f"Waiting {wait} seconds for qserv to stabilize.")
        time.sleep(wait)
        _log.info("Continuing.")

    args = [
        "docker",
        "run",
        "--init",
        "--name",
        itest_container,
        "--mount",
        f"src={itest_file},dst=/usr/local/etc/integration_tests.yaml,type=bind",
        "--mount",
        f"src={itest_volume},dst=/qserv/data,type=volume",
    ]
    if remove:
        args.append("--rm")
    if bind:
        args.extend(bind_args(qserv_root=qserv_root, bind_names=bind))
    add_network_option(args, project)
    args.extend([qserv_image, "entrypoint", "--log-level", "DEBUG", "integration-test"])
    for opt, var in (
        ("--unload", unload),
        ("--reload", reload),
        ("--run-tests", run_tests),
        ("--compare-results", compare_results),
    ):
        if var:
            args.append(opt)

    def add_flag_if(val: Optional[bool], true_flag: str, false_flag: str, args: List[str]) -> None:
        """Add a do-or-do-not flag to `args` if `val` is `True` or `False`, do
        not add if `val` is `None`."""
        if val == True:
            args.append(true_flag)
        elif val == False:
            args.append(false_flag)

    add_flag_if(pull, "--pull", "--no-pull", args)
    add_flag_if(load, "--load", "--no-load", args)

    if tests_yaml:
        args.extend(["--tests-yaml", tests_yaml])
    for case in cases:
        args.extend(["--case", case])
    if dry:
        print(" ".join(args))
        return 0
    _log.debug(f"Running {' '.join(args)}")
    result = subprocess.run(args)
    return result.returncode


def itest(
    qserv_root: str,
    mariadb_image: str,
    itest_container: str,
    itest_volume: str,
    qserv_image: str,
    bind: List[str],
    itest_file: str,
    dry: bool,
    project: str,
    pull: Optional[bool],
    unload: bool,
    load: Optional[bool],
    reload: bool,
    cases: List[str],
    run_tests: bool,
    tests_yaml: str,
    compare_results: bool,
    wait: int,
    remove: bool,
) -> int:
    """Run integration tests.

    Parameters
    ----------
    Similar to `integration_test`

    Returns
    -------
    returncode : `int`
        The returncode of "entrypoint integration-test".
    """
    ref_db_container_name = itest_ref(qserv_root, itest_file, itest_volume, project, mariadb_image, dry)
    try:
        returncode = integration_test(
            qserv_root,
            itest_container,
            itest_volume,
            qserv_image,
            bind,
            itest_file,
            dry,
            project,
            pull,
            unload,
            load,
            reload,
            cases,
            run_tests,
            tests_yaml,
            compare_results,
            wait,
            remove,
        )
    finally:
        if remove:
            stop_itest_ref(ref_db_container_name, dry)
    return returncode


def itest_rm(itest_volume: str, dry: bool) -> None:
    """Remove integration test volumes.

    Parameters
    ----------
    itest_volume : `str`
        The name of the volume used for integration tests.
    dry : `bool`
        If True do not run the command; print what would have been run.
    """
    args = ["docker", "volume", "rm", itest_volume, f"{itest_volume}_lib"]
    if dry:
        print(" ".join(args))
        return
    _log.debug(f"Running {' '.join(args)}")
    result = subprocess.run(args)
    result.check_returncode()


def update_schema(
    czar_connection: str,
    worker_connections: List[str],
    repl_connection: str,
    qserv_image: str,
    project: str,
    dry: bool,
) -> str:
    """Update schema on qserv nodes.

    Parameters
    ----------
    czar_connection : `str`
        The czar db connection in format user:pass@host:port/database
    worker_connections : `list` [ `str` ]
        The worker db connections in format user:pass@host:port/database
    repl_connection : `str`
        The replication db connection in format user:pass@host:port/database
    qserv_image : `str`
        The name of the qserv image to use.
    project : `str`
        The project name that is used to derive a network name.
    dry : `bool`
        If True do not run the command; print what would have been run.

    Returns
    -------
    stderr : `str`
        If there is an error, returns stderr, otherwise returns an empty string.
    """
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
    ]
    add_network_option(args, project)
    args.extend(
        [
            qserv_image,
            "entrypoint",
            "--log-level",
            "DEBUG",
            "smig-update",
        ]
    )
    if czar_connection:
        args.extend(["--czar-connection", czar_connection])
    if worker_connections:
        for connection in worker_connections:
            args.extend(["--worker-connection", connection])
    if repl_connection:
        args.extend(["--repl-connection", repl_connection])
    if dry:
        print(" ".join(args))
        return ""
    _log.debug('Running "%s"', " ".join(args))
    result = subprocess.run(args, encoding="utf-8", errors="replace")
    return result.stderr


def get_env(vals: Dict[str, str]) -> Dict[str, str]:
    """Get a dict of the current environment variables with additional values.

    Parameters
    ----------
    vals : `dict` [`str` : `str`]
        The additional environment variables to add.
    """
    env = os.environ.copy()
    populated_vals = {k: v for k, v in vals.items() if v is not None}
    env.update(populated_vals)
    return env


def up(
    yaml_file: str,
    dry: bool,
    project: str,
    qserv_image: str,
    mariadb_image: str,
    dashboard_port: Optional[int],
) -> None:
    """Send docker-compose up and down commands.

    Parameters
    ----------
    yaml_file : `str`
        Path to the yaml file that describes the compose cluster.
    dry : `bool`
        If True do not run the command; print what would have been run.
    project : `str`
        The name used for qserv instance customizations.
    qserv_image : `str`
        The name of the qserv image to use.
    mariadb_image : `str`
        The name of the mariadb image to use.
    dashboard_port : `int` or `None`
        The host port to use for the qserv dashboard.
    """
    args = ["docker-compose", "-f", yaml_file]
    if project:
        args.extend(["-p", project])
    args.extend(["up", "-d"])
    env_override = {
        qserv_image_ev.env_var: qserv_image,
        mariadb_image_ev.env_var: mariadb_image,
    }
    if dashboard_port:
        env_override[dashboard_port_ev.env_var] = str(dashboard_port)
    if dry:
        env_str = " ".join([f"{k}={v}" for k, v in env_override.items()])
        print(f"{env_str} {' '.join(args)}")
    else:
        env = get_env(env_override)
        _log.debug("Running %s with environment overrides %s", " ".join(args), env_override)
        subprocess.run(args, env=env, check=True)


def down(
    yaml_file: str,
    volume: str,
    dry: bool,
    project: str,
    qserv_image: str,
    mariadb_image: str,
) -> None:
    """Send docker-compose up and down commands.

    Parameters
    ----------
    yaml_file : `str`
        Path to the yaml file that describes the compose cluster.
    volume : `bool`
        Pass the -v flag to docker-compose, to remove cluster volumes.
    dry : `bool`
        If True do not run the command; print what would have been run.
    project : `str`
        The name used for qserv instance customizations.
    qserv_image : `str`
        The name of the qserv being used.
    mariadb_image : `str`
        The name of the qserv being used.
    """
    args = [
        "docker-compose",
        "-f",
        yaml_file,
    ]
    if project:
        args.extend(["-p", project])
    args.append("down")
    if volume:
        args.append("-v")
    if dry:
        print(" ".join(args))
        return
    _log.debug('Running "%s"', " ".join(args))
    env = get_env(
        {
            qserv_image_ev.env_var: qserv_image,
            mariadb_image_ev.env_var: mariadb_image,
        }
    )
    subprocess.run(args, env=env, check=True)


def entrypoint_help(
    command: str,
    qserv_image: str,
    dry: bool,
) -> None:
    """Print the entrypoint CLI help output.

    Parameters
    ----------
    command : Sequence[str]
        The commands to get help for.
    qserv_image : `str`
        The name of the image to run.
    dry : `bool`
        If True do not run the command; print what would have been run.
    """
    args = [
        "docker",
        "run",
        "--init",
        "--rm",
        qserv_image,
        "entrypoint",
    ]
    if command:
        args.append(command)
    args.append("--help")
    if dry:
        print(" ".join(args))
    else:
        _log.debug('Running "%s"', " ".join(args))
        subprocess.run(args, check=True)
