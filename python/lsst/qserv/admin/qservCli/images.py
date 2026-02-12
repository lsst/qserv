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


"""Utilities for working with docker images and registries."""

import logging
import subprocess
from copy import copy

from . import subproc

_log = logging.getLogger(__name__)


def get_description(dockerfiles: list[str] | None, cwd: str) -> str:
    """Get the git description of the commit that contains the most recent
    change to any of the given dockerfiles, or of the commit of the most
    recent tag if it is more recent than the dockerfile changes.

    Parameters
    ----------
    dockerfiles : `list` [`str`] or `None`
        If provided, a list of Dockerfiles to check. If none, describe the
        current git sha, with the ``--dirty`` option (so the tag will be marked
        dirty if there is currently uncommitted changes).
    cwd : `str`
        The directory to run the command from, all dockerfile paths must be
        relative to this directory.

    Returns
    -------
    tag : `str`
        The tag of the commit.
    """
    # todo handle if one of the dockerfiles is dirty
    if dockerfiles is not None:
        shas = [get_last_change(fname, cwd) for fname in dockerfiles]
        shas.append(last_git_tag(cwd))
        sha: str | None = get_most_recent(shas, cwd)
    else:
        sha = None
    tag = describe(sha, cwd)
    return tag


def last_git_tag(cwd: str) -> str:
    """Get the sha of the most recent git tag.

    Parameters
    ----------
    cwd : str
        The directory to run the command from, must be in the
        git repository to get the tag from.

    Returns
    -------
    SHA : str
        The sha of the most recent git tag.

    Raises
    ------
    RuntimeError
        If the most recent tag can not be gotten, or if the
        SHA of that tag can not be gotten from git.
    """
    args = ["git", "describe", "--abbrev=0"]
    _log.debug('Running "%s"', " ".join(args))
    res = subproc.run(
        args,
        capture_stdout=True,
        cwd=cwd,
        errmsg=f"Failed to get most recent tag from repo at {cwd}.",
    )
    tag = res.stdout.decode().strip()
    args = ["git", "rev-list", "-n", "1", tag]
    _log.debug('Running "%s"', " ".join(args))
    res = subproc.run(
        args,
        capture_stdout=True,
        cwd=cwd,
        errmsg=f"Failed to get SHA for tag {tag}.",
    )
    return res.stdout.decode().strip()


def describe(sha: str | None, cwd: str) -> str:
    """Get the description of the change from `git describe`.

    Parameters
    ----------
    sha : `str` or `None`
        The sha to describe, or None if the current state of git should be
        described, with the ``--dirty`` option.
    cwd : `str`
        The directory to execute the git command in.

    Returns
    -------
    description : `str`
        The result of `git describe`

    Raises
    ------
    RuntimeError
        If the git command fails.
    """
    abbrev = 9
    if sha:
        args = ["git", "describe", "--always", f"--abbrev={abbrev}", sha]
    else:
        args = ["git", "describe", "--always", "--dirty", f"--abbrev={abbrev}"]
    _log.debug('Running "%s"', " ".join(args))
    res = subproc.run(
        args,
        cwd=cwd,
        capture_stdout=True,
        errmsg=f"Failed to get git description of sha {sha}.",
    )
    description = res.stdout.decode().strip()
    if sha:
        _log.debug("The description of %s is %s", sha, description)
    else:
        _log.debug("The description of the current state of git is %s", description)
    return description


def get_last_change(fname: str, cwd: str) -> str:
    """Get the sha of the most recent change to a file.

    Parameters
    ----------
    fname : `str`
        The path to the file, relative to cwd.
    cwd : `str`
        The directory to execute the git command in.

    Returns
    -------
    sha : `str`
        The sha of the commit that contains the most recent change.
    """
    args = ["git", "log", "--pretty=format:%H", "--max-count=1", fname]
    _log.debug('Running "%s"', " ".join(args))
    res = subproc.run(
        args,
        cwd=cwd,
        capture_stdout=True,
        errmsg=f"Failed to get git sha of most recent change to {fname}.",
    )
    sha = res.stdout.decode().strip()
    _log.debug("The most recent change to %s was in %s", fname, sha)
    return sha


def git_log(a: str, b: str, cwd: str) -> list[str]:
    """Get the commits between two shas.

    Parameters
    ----------
    a : `str`
        A git sha.
    b : `str`
        A git sha.
    cwd : `str`
        The directory to execute the git command in.

    Returns
    -------
    log : `list` [`str`]
        A list of commits rechable from b but not from a.

    Raises
    ------
    RuntimeError
        If there is an error running `git log a..b`.
    """
    args = ["git", "log", "--pretty=format:%H", f"{a}..{b}"]
    _log.debug('Running "%s"', " ".join(args))
    res = subproc.run(
        args,
        cwd=cwd,
        capture_stdout=True,
        errmsg=f"Failed to get git log of shas {a}..{b}.",
    )
    return res.stdout.decode().strip().split()


def get_most_recent(shas: list[str], cwd: str) -> str:
    """Get the most recent sha of a given list of shas.

    Parameters
    ----------
    shas : `list` [`str`]
        A list of git shas, the newest sha will be returned.
    cwd : `str`
        The directory to execute the git command in.

    Returns
    -------
    newest : `str`
        The newest sha from the list of shas.
    """
    shas_copy = copy(shas)
    newest = shas_copy.pop()
    while shas_copy:
        other = shas_copy.pop()
        if other == newest:
            continue
        history = git_log(newest, other, cwd)
        if history:
            newest = other
        else:
            history = git_log(other, newest, cwd)
            if not history:
                raise RuntimeError(f"Could not establish a relationship between shas {newest} and {other}.")
    _log.debug("The newest sha out of %s is %s", shas, newest)
    return newest


def image_exists(image_name: str) -> bool:
    """Determine if a given tag exists in its associated registry.

    Parameters
    ----------
    image_name : `str`
        The name+tag of the image, e.g. "ghcr.io/qserv-build-base:2021.9.2-rc1-24-gda31230

    Returns
    -------
    exists : `bool`
        True if the image exists in the associated registry.
    """
    args = ["crane", "manifest", image_name]
    _log.debug('Running "%s"', " ".join(args))
    result = subprocess.run(args, capture_output=True)
    return result.returncode == 0


def pull_image(image_name: str, dry: bool) -> bool:
    """Pull an image from registry.

    Parameters
    ----------
    image_name : `str`
        The name+tag of the image to pull.
    dry : `bool`
        If True do not run the command; print what would have been run.

    Returns
    -------
    pulled : `bool`
        `True` if the image was pulled, `False` if it was not pulled.
    """
    args = ["docker", "pull", image_name]
    if dry:
        print(" ".join(args))
        return True  # if `dry`, emulate successful state.
    _log.debug('Running "%s"', " ".join(args))
    result = subprocess.run(args)
    return result.returncode == 0


def push_image(image_name: str, dry: bool) -> None:
    """Push an image to registry.

    Parameters
    ----------
    image_name : `str`
        The name+tag of the image.
    dry : `bool`
        If True do not run the command; print what would have been run.
    """
    args = ["docker", "push", image_name]
    if dry:
        print(" ".join(args))
        return
    _log.debug('Running "%s"', " ".join(args))
    result = subprocess.run(args)
    result.check_returncode()


def build_image(
    image_name: str,
    target: str | None,
    run_dir: str,
    dry: bool,
    options: list[str] | None = None,
    dockerfile: str | None = None,
) -> None:
    """Build a qserv image.

    Parameters
    ----------
    image_name : `str`
        The name+tag of the image.
    target : `str` or `None
        Names the target to build if the dockerfile has multiple targets.
    run_dir : `str`
        The path to the directory that contains the docker build context.
    dry : `bool`
        If True do not run the command; print what would have been run.
    options : `list` [ `str` ] or `None`
        A list of option flags & values for the `docker build` command.
    dockerfile : `str`
        The path to the dockerfile.
    """
    args = ["docker", "build"]
    if target:
        args.append(f"--target={target}")
    args.append(f"--tag={image_name}")
    if options:
        args.extend(options)
    if dockerfile:
        args.extend(["-f", dockerfile])
    args.append(".")
    if dry:
        print(f"cd {run_dir}; {' '.join(args)}; cd -")
    else:
        _log.debug('Running "%s" from directory %s', " ".join(args), run_dir)
        subproc.run(args, cwd=run_dir)
