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
from functools import partial


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


class OptionDecorator:
    """Wraps the click.option decorator to enable shared options to be declared
    and allows inspection of the shared option.
    """

    def __init__(self, *param_decls, **kwargs):
        self.partialOpt = partial(click.option, *param_decls, **kwargs)
        opt = click.Option(param_decls, **kwargs)
        self._name = opt.name
        self._opts = opt.opts

    def name(self):
        """Get the name that will be passed to the command function for this
        option."""
        return self._name

    def opts(self):
        """Get the flags that will be used for this option on the command
        line."""
        return self._opts

    @property
    def help(self):
        """Get the help text for this option. Returns an empty string if no
        help was defined."""
        return self.partialOpt.keywords.get("help", "")

    def __call__(self, *args, **kwargs):
        return self.partialOpt(*args, **kwargs)
