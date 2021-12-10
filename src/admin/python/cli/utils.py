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

"""Utilities for CLI functions in qserv.
"""

import click
import logging
from typing import cast, Any, Dict, List, Tuple, Union
import yaml


_log = logging.getLogger(__name__)


def split_kv(
    ctx: click.Context, param: click.core.Option, values: List[str]
) -> Dict[str, str]:
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

    Returns
    -------
    items : `Dict` [`str`, `str`]
        The separated key-value pairs.
    """
    if not values:
        return {}
    # combine all the (possibly comma-separated) values into one comma-separated
    # string, and then split on comma:
    pairs = ",".join(values).split(",")
    # verify each pair has exactly one equal sign
    for pair in pairs:
        if pair.count("=") != 1:
            raise RuntimeError("Each key-value pair must be separated by '='.")
    # split each pair on the equal sign:
    split_pairs = (cast(Tuple[str, str], pair.split("=")) for pair in pairs)
    # and finally, make a dict:
    return dict(split_pairs)


def yaml_presets(ctx: click.Context, param: str, value: str) -> None:
    """Update a click context with defaults from a yaml file.

    Parameters
    ----------
    ctx : click.Context
        The click context to update.
    param : str
        The name of the parameter.
    value : str
        The value of the parameter.
    """
    ctx.default_map = ctx.default_map or {}
    cmd_name = ctx.info_name
    if value:
        try:
            overrides = _read_yaml_presets(value, cmd_name)
        except Exception as e:
            raise click.BadOptionUsage(param.name, f"Error reading overrides file: {e}", ctx)
        # Override the defaults for this subcommand
        ctx.default_map.update(overrides)


def _read_yaml_presets(file, cmd_name) -> Dict[str, Union[str, int, bool]]:
    """Read file command line overrides from YAML config file.

    Parameters
    ----------
    file : `str`
        Path to override YAML file containing the command line overrides.
        They should be grouped by command name. The option name should
        NOT include prefix dashes.
    cmd_name : `str`
        The subcommand name that is being modified.

    Returns
    -------
    overrides : `dict` of [str, Union[`str`, `int`, `bool`]]
        The relevant command line options read from the override file.
    """
    _log.debug("Reading command line overrides for subcommand %s from URI %s", cmd_name, file)
    with open(file) as f:
        presets = yaml.safe_load(f.read())
    return presets.get(cmd_name, dict())
