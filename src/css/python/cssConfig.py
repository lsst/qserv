"""
Module containing Python-specific methods for CSS configuration.

"""

# --------------------------------
#  Imports of standard modules --
# --------------------------------
from typing import Mapping

# ---------------------------------
#  Imports of base class module --
# ---------------------------------
import logging

# -----------------------------
# Imports for other modules --
# -----------------------------
from sqlalchemy.engine.url import make_url

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# ------------------------
# Exported definitions --
# ------------------------


def configFromUrl(url: str) -> Mapping[str, str]:
    """
    This method builds configuration object (dict) from URL string.

    URL string has sqlalchemy-supported format:
    - mysql://user:passwd@host:port/database
    - sqlite:///file.path
    - mem:///file.path
    - other options may be possible in future

    @raises ValueError: if URL parsing fails
    """
    try:
        _url = make_url(url)
    except Exception as exc:
        logging.error('Failed to parse connection URL: %s %s)', url, exc)
        raise ValueError('Failed to parse connection URL: ' + url)

    cssConfig = {'technology': _url.drivername}
    if _url.drivername == 'mysql':
        if _url.host:
            cssConfig['hostname'] = _url.host
        if _url.port:
            cssConfig['port'] = str(_url.port)
        if _url.username:
            cssConfig['username'] = _url.username
        if _url.password:
            cssConfig['password'] = _url.password
        if _url.database:
            cssConfig['database'] = _url.database
        if _url.query and 'unix_socket' in _url.query:
            cssConfig['socket'] = _url.query['unix_socket']
    elif _url.drivername == 'mem':
        cssConfig['data'] = _url.query.get('data', '')
        cssConfig['file'] = _url.database or ''
    else:
        raise ValueError('Technology %s is not supported (yet)' % _url.drivername)

    return cssConfig
