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

"""This is the command-line `qserv` command. If it's findable on the PATH then
there will be a CLI command called qserv. Use `qserv --help` to for more
information.
"""

import click
from functools import partial
import getpass
import logging
import os
from typing import Callable, List, Optional, Union


from images import get_tag


_log = logging.getLogger(__name__)


class EV:
    """Base class for classes that use environment variables to assign values
    and defaults to options.

    Parameters
    ----------
    env_var : `str`
        The environment variable.
    default : `str` or `None`
        The default value if the environment variable is not defined.
    private : `bool`
        True if the value should not be printed or logged.
    """

    def __init__(self, env_var: str, default: Optional[str], private: bool):
        self.env_var = env_var
        self.default = default
        self.private = private

    def val(self) -> Optional[str]:
        """Get the value of the environment variable or a default value.

        Returns
        -------
        value : `str`
            The value.
        """
        return os.getenv(self.env_var, self.default)

    @property
    def var_val(self) -> Optional[str]:
        """Get the value of the environment variable or return None if the var
        is not defined."""
        return os.getenv(self.env_var, None)


class EnvVal(EV):

    """Defines an environment varaible and defaults that may be used by options.
    Parameters
    ----------
    Inherited from `EV`
    description : `str`
        Description of what the variable is for, used for help output.
    """

    def __init__(self, env_var: str, description: str, private: bool = False):
        super().__init__(env_var, default=None, private=private)
        self.description = description

    @property
    def used_for(self) -> str:
        """Get the description of what the EnvVal is used for, for help output."""
        return self.description

    def val(self, default: Optional[str] = None) -> Optional[str]:
        """Get the value.

        Parameters
        ----------
        default : `str`, optional
            The value to return if the environment variable does not exist, by
            default None

        Returns
        -------
        value : `str` or `None`
            The value of the environment variable or the passed-in default
            value.
        """
        return super().var_val or default


class FlagEnvVal(EV):
    """Associates CLI flags that can also be set by environment variables
    and optionally their default value.

    Parameters
    ----------
    Inherited from `EV`
    opt : `str`, Optional
        The option flag.
    """

    def __init__(self, opt: str, env_var: str, default: Optional[str]):
        super().__init__(env_var, default, private=False)
        self.opt = opt

    @property
    def used_for(self) -> str:
        """Get the description of what the EnvVal is used for, for help output."""
        return self.opt

    def help(self, preamble: str) -> str:
        """Get the help text for this item, including boilerplate about how the
        value can be set.

        Parameters
        ----------
        preamble : `str`
            The text that describes the item to put before the boilerplate help
            output.

        Returns
        -------
        help_text : `str`
            The complete help text for this item.
        """
        msg = preamble
        msg += (
            f" Can be set with environment variable {click.style(self.env_var, bold=True)}; current value is "
        )
        var = os.getenv(self.env_var, None)
        if var is not None:
            msg += click.style(var, fg="green", bold=True)
            msg += f". (Default value is {click.style(self.default, fg='blue', bold=True)})."
        else:
            msg += click.style("not set", fg="red", bold=True)
            msg += f". Will use default value: {click.style(self.default, fg='green', bold=True)})."
        return msg


base_dockerfile = "admin/tools/docker/base/Dockerfile"
user_dockerfile = "admin/tools/docker/build-user/Dockerfile"
run_base_dockerfile = "admin/tools/docker/base/Dockerfile"
mariadb_dockerfile = "admin/tools/docker/mariadb/Dockerfile"


def tagged_image_name(image_name: str, dockerfiles: Optional[List[str]]) -> Optional[str]:
    """Get the image name to use, tagged with a tag indicating the changelist
    when the dockerfiles were most recently changed.

    Parameters
    ----------
    image_name : `str`
        The image base name (without tag).
    dockerfiles : `List` [ `str` ] or `None`
        If provided, the list of dockerfiles to consider when generating the
        tag. If `None`, get the current git tag.

    Returns
    -------
    image_name : `str`
        The image name + tag
    """
    qserv_root = qserv_root_ev.val()
    if qserv_root is None:
        return None
    return f"{image_name}:{image_tag_ev.val(get_tag(dockerfiles, qserv_root))}"


qserv_root_ev = FlagEnvVal(
    "--qserv-root",
    "QSERV_ROOT",
    os.path.abspath(os.path.join(__file__, "../../../../")),
)
image_tag_ev = EnvVal(env_var="QSERV_IMAGE_TAG", description="the tag of all qserv image names")
qserv_image_ev = FlagEnvVal(
    "--qserv-image",
    "QSERV_IMAGE",
    tagged_image_name("qserv/lite-qserv", None),
)
run_base_image_ev = FlagEnvVal(
    "--run-base-image",
    "QSERV_RUN_BASE_IMAGE",
    tagged_image_name("qserv/lite-run-base", [run_base_dockerfile]),
)
mariadb_image_ev = FlagEnvVal(
    "--mariadb-image",
    "QSERV_MARIADB_IMAGE",
    tagged_image_name("qserv/lite-mariadb", [mariadb_dockerfile]),
)
build_image_ev = FlagEnvVal(
    "--build-image",
    "QSERV_BUILD_IMAGE",
    tagged_image_name("qserv/lite-build", [base_dockerfile]),
)
user_build_image_ev = FlagEnvVal(
    "--user-build-image",
    "QSERV_USER_BUILD_IMAGE",
    tagged_image_name(f"qserv/lite-build-{getpass.getuser()}", [base_dockerfile, user_dockerfile]),
)
# qserv root default is derived by the relative path to the qserv folder from
# the locaiton of this file.
qserv_build_root_ev = FlagEnvVal("--qserv-build-root", "QSERV_BUILD_ROOT", "/home/{user}/code/qserv")
project_ev = FlagEnvVal("--project", "QSERV_PROJECT", getpass.getuser())
dashboard_port_ev = FlagEnvVal("--dashboard-port", "QSERV_DASHBOARD_PORT", None)
dh_user_ev = EnvVal("QSERV_DH_USER", "CI only; the dockerhub user for pushing and pulling images")
dh_token_ev = EnvVal(
    "QSERV_DH_TOKEN",
    "CI only; the dockerhub user token for pushing and pulling images",
    private=True,
)


class OptDefault:

    """Associates CLI flags with a default value that can be derived using a
    FlagEnvVal environment variable.

    Parameters
    ----------
    opt : `list` [`str`]
        The flag and variable name arguments to click.option
    default : `str` or `None`
        The default value of the value if no environment variable is
        defined, or `None` if there is no default value.
    ev : `FlagEnvVal`
        The `FlagEnvVal` instance that defines the associated environment variable.
    val : `(str) -> `str`
        A callable function that takes the currently defined value of an
        associated `FlagEnvVal` and returns a value derived from it. Will not be
        called if the environment variable is not set.
    """

    def __init__(
        self,
        opt: List[str],
        default: Optional[str],
        ev: FlagEnvVal,
        val: Callable[[str], str],
    ):
        self.opt = opt
        self.default = default
        self.ev = ev
        self._val = val

    def val(self) -> Optional[str]:
        """Get the value for this default: if the environment variable is
        defined, returns the value derived from that environment variable, or a
        default value (which may be `None`).

        Returns
        -------
        value : `str` or `None`
            The value to use.
        """
        ev_val = self.ev.val()
        if ev_val is None:
            return self.default
        return self._val(ev_val)

    def help(self, preamble: str) -> str:
        """Get the help text for this item, including boilerplate about how the
        value can be set.

        Parameters
        ----------
        preamble : `str`
            The text that describes the item to put before the boilerplate help
            output.

        Returns
        -------
        help_text : `str`
            The complete help text for this item.
        """
        msg = preamble
        if self.default is None:
            msg += " No default value."
        else:
            msg += f" Default is {click.style(self.default, fg='blue', bold=True)}."
        msg += f" Can derive value from from {click.style(self.ev.env_var, bold=True)}."
        msg += f" Current value is {click.style(self.val(), fg='green', bold=True)}."
        return msg


itest_default = OptDefault(
    opt=["--itest-file"],
    default=None,
    ev=qserv_root_ev,
    val=lambda ev_val: os.path.join(ev_val, "src/admin/etc/integration_tests.yaml"),
)
itest_container_default = OptDefault(
    opt=["--itest-container"],
    default="itest",
    ev=project_ev,
    val=lambda ev_val: f"{ev_val}_itest",
)
itest_volume_default = OptDefault(
    opt=["--itest-volume"],
    default="itest_volume",
    ev=project_ev,
    val=lambda ev_val: f"{ev_val}_itest_volume",
)
test_container_default = OptDefault(
    opt=["--test-container"],
    default="test",
    ev=project_ev,
    val=lambda ev_val: f"{ev_val}_test",
)
compose_file_default = OptDefault(
    opt=["--file", "yaml_file"],
    default=None,
    ev=qserv_root_ev,
    val=lambda ev_val: os.path.join(ev_val, "admin/local/docker/compose/docker-compose.yml"),
)
build_container_default = OptDefault(
    opt=["--build-container-name"],
    default=f"build_container",
    ev=project_ev,
    val=lambda ev_val: f"{ev_val}_build",
)


class FlagEnvVals:
    """Container for all the FlagEnvVal instances.

    Parameters
    ----------
    evs : `list` [ `FlagEnvVal` ]
        The `FlagEnvVal` instances used by the qserv command.
    """

    def __init__(self, evs: List[Union[FlagEnvVal, EnvVal]]):
        self.evs = evs

    def describe(self) -> str:
        """Get the description of all the FlagEnvVal instances, incluing current and
        default values, for command line display.
        """
        ret = ""
        indent = "  "
        for ev in self.evs:
            ret += indent
            ret += f"{click.style(ev.env_var, bold=True)} "
            ret += f"({click.style('***' if ev.private else ev.var_val) if ev.var_val else click.style('Not defined', fg='red')}) "
            ret += f"for {click.style(ev.used_for)} "
            ret += f"(default: {click.style(ev.default, fg='blue')}) "
            ret += f"is {click.style('***' if ev.private else ev.val(), fg='green', bold=True) if ev.val() else click.style('None', fg='red')}."
            ret += "\n"
        return ret


class DefaultValues:

    """Container for the OptDefault instances.

    Parameters
    ----------
    defaults : `list` [ `OptDefault` ]
        The `OptDefault` instances used by the qserv command.
    """

    def __init__(self, defaults: List[OptDefault]):
        self.defaults = defaults

    def describe(self) -> str:
        """Get the description of all the OptDefault instances, incluing the
        related environment variable and default values, for command line
        display.
        """
        ret = ""
        for default in self.defaults:
            val = default.val()
            ret += f" {default.opt[0]} using {click.style(default.ev.env_var, bold=True)} "
            if val is None:
                ret += f"({click.style('not defined', fg='red')}), "
            else:
                ret += f"is {click.style(default.val(), fg='green', bold=True)} "
            ret += f"(default: {click.style(default.default, fg='blue', bold=True)})"
            ret += "\n"
        return ret


qserv_env_vals = FlagEnvVals(
    [
        image_tag_ev,
        qserv_image_ev,
        build_image_ev,
        user_build_image_ev,
        run_base_image_ev,
        mariadb_image_ev,
        qserv_root_ev,
        qserv_build_root_ev,
        project_ev,
        dashboard_port_ev,
        dh_user_ev,
        dh_token_ev,
    ]
)


qserv_default_vals = DefaultValues(
    [
        itest_default,
        compose_file_default,
        # itest_container_default, # need to fix these (nonsensical when QSERV_PROJECT is not set)
        # test_container_default
    ]
)


bind_choices = ["all", "python", "bin", "lib64", "lua", "qserv", "etc"]


qserv_image_option = partial(
    click.option,
    qserv_image_ev.opt,
    help=qserv_image_ev.help("The name of the qserv image."),
    default=qserv_image_ev.val(),
)


build_image_option = partial(
    click.option,
    build_image_ev.opt,
    help=build_image_ev.help("The name of the qserv build image."),
    default=build_image_ev.val(),
)

user_build_image_option = partial(
    click.option,
    user_build_image_ev.opt,
    help=user_build_image_ev.help("The name of the qserv user build image."),
    default=user_build_image_ev.val(),
)


user_option = partial(
    click.option,
    "--user",
    help="The user name to use when running the build container. "
    f"Default will use current user: {click.style(getpass.getuser(), fg='green', bold=True)}",
    default=getpass.getuser(),
)


push_image_option = partial(
    click.option,
    "--push-image",
    help="Push the image to dockerhub if it does not exist. Requires login to dockerhub first.",
    is_flag=True,
)


pull_image_option = partial(
    click.option,
    "--pull-image",
    help="Pull the image from dockerhub if it exists.",
    is_flag=True,
)


run_base_image_option = partial(
    click.option,
    run_base_image_ev.opt,
    help=run_base_image_ev.help("The name of the run base image."),
    default=run_base_image_ev.val(),
)


mariadb_image_option = partial(
    click.option,
    mariadb_image_ev.opt,
    help=mariadb_image_ev.help("The name of the mariadb image."),
    default=mariadb_image_ev.val(),
)


qserv_root_option = partial(
    click.option,
    qserv_root_ev.opt,
    help=qserv_root_ev.help(
        "Location of the qserv sources folder outside of the build container. "
        "Default location is relative to the qserv command file."
    ),
    envvar=qserv_root_ev.env_var,
    default=qserv_root_ev.default,
    required=True,
)


qserv_build_root_option = partial(
    click.option,
    qserv_build_root_ev.opt,
    help=qserv_build_root_ev.help("Location of the qserv sources folder inside the build container."),
    default=qserv_build_root_ev.default,
)


dashboard_port_option = partial(
    click.option,
    dashboard_port_ev.opt,
    help=dashboard_port_ev.help("The host port to use for the qserv dashboard."),
    default=dashboard_port_ev.val(),
)


unit_test_option = partial(
    click.option,
    "--unit-test/--no-unit-test",
    help="Run unit tests. Default: --unit-test",
    is_flag=True,
    default=True,
)


dry_option = partial(
    click.option,
    "-d",
    "--dry",
    help="Do not run the command, instead print the action to the command line.",
    is_flag=True,
)


jobs_option = partial(
    click.option,
    "-j",
    "--jobs",
    help="Same as the gnu make -j option; the number of make recipies to execute at once.",
    type=int,
)


project_option = partial(
    click.option,
    project_ev.opt,
    help=project_ev.help("The project name for the qserv docker-compose instance."),
    default=project_ev.val(),
    callback=lambda ctx, par, val: None if val == "None" else val,
)


compose_file_option = partial(
    click.option,
    *compose_file_default.opt,
    help=compose_file_default.help("The location of the yaml file that describes the compose cluster."),
    default=compose_file_default.val(),
    required=True,
)


itest_container_name_option = partial(
    click.option,
    *itest_container_default.opt,
    help=itest_container_default.help("The name to give the integration test container."),
    default=itest_container_default.val(),
    required=True,
)


itest_file_option = partial(
    click.option,
    *itest_default.opt,
    help=itest_default.help("Path to an yaml file that describes how to run the integration tests."),
    default=itest_default.val(),
    required=True,
)


itest_volume_option = partial(
    click.option,
    *itest_volume_default.opt,
    help=itest_volume_default.help("The name of the volume used to hold integration test data."),
    default=itest_volume_default.val(),
    required=True,
)


test_container_name_option = partial(
    click.option,
    *test_container_default.opt,
    help=test_container_default.help("The name to give the test container."),
    default=test_container_default.val(),
    required=True,
)


bind_option = partial(
    click.option,
    "-b",
    "--bind",
    help="Build artifact location(s) to be bind mounted into the container. "
    "Allows for local iterative build & test without having to rebuild the docker image. "
    "'all' includes all locations.",
    multiple=True,
    type=click.Choice(bind_choices),
)


cmake_option = partial(
    click.option,
    "--cmake/--no-cmake",
    "run_cmake",
    help="Force cmake to run or not run, before running make. By default runs cmake if the build folder does not exist yet.",
    is_flag=True,
    default=None,
)


make_option = partial(
    click.option,
    "--make/--no-make",
    "run_make",
    help="Run make before creating the image. Default: --make",
    default=True,
    is_flag=True,
)


do_build_image_option = partial(
    click.option,
    "--do-build-image/--no-build-image",
    help="Build the run image after running cmake and make. Default: --do-build-image",
    default=True,
    is_flag=True,
)


build_container_name_option = partial(
    click.option,
    *build_container_default.opt,
    help=build_container_default.help("The name to give the build container."),
    default=build_container_default.val(),
)


remove_option = partial(
    click.option,
    "--remove/--no-remove",
    help="Remove docker container(s) after execution.",
    default=True,
)
