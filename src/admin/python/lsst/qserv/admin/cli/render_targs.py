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


"""This module contains functions for rendering jinja template
values in values passed into the `entrypoint` command line."""


from copy import copy
import jinja2

from .utils import Targs


class UnresolvableTemplate(RuntimeError):
    """Exception class used by `render` when a template value can not be
    resolved."""
    pass


def render_targs(targs: Targs) -> Targs:
    """Go through a dict whose values may contain jinja templates that are
    other keys in the dict, and resolve the values.

    Will raise if any template value(s) can not be resolved. Causes include:
    * the key is not present
    * there is a circular reference where 2 or more keys refer to eachother.

    Parameters
    ----------
    targs :
        The dict to resolve.

    Returns
    -------
    resolved_targs :
        The dict, resolved.

    Raises
    ------
    UnresolvableTemplate
        If a value in the template can not be resolved.
    """
    rendered = copy(targs)
    while True:
        changed = False
        for k, v in rendered.items():
            if not isinstance(v, str):
                continue
            if "{{" in v:
                t = jinja2.Template(v, undefined=jinja2.StrictUndefined)
                try:
                    rendered[k] = t.render(rendered)
                    changed = True
                except jinja2.exceptions.UndefinedError as e:
                    raise UnresolvableTemplate(f"Missing template value: {str(e)}")
        if not changed:
            break
    if any([isinstance(v, str) and "{{" in v for v in rendered.values()]):
        raise UnresolvableTemplate(
           "Could not resolve inputs: "
           f"{', '.join([f'{k}={targs[k]}' for k, v in rendered.items() if '{{' in v])}"
        )
    return rendered
