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
from typing import cast, Dict, List, Tuple


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
