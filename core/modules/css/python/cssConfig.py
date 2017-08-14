"""
Module containing Python-specific methods for CSS configuration.

"""

# --------------------------------
#  Imports of standard modules --
# --------------------------------

# ---------------------------------
#  Imports of base class module --
# ---------------------------------
from builtins import str
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


def configFromUrl(url):
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
        url = make_url(url)
    except Exception as exc:
        logging.error('Failed to parse connection URL: %s %s)', url, exc)
        raise ValueError('Failed to parse connection URL: ' + url)

    cssConfig = {'technology': url.drivername}
    if url.drivername == 'mysql':
        if url.host:
            cssConfig['hostname'] = url.host
        if url.port:
            cssConfig['port'] = str(url.port)
        if url.username:
            cssConfig['username'] = url.username
        if url.password:
            cssConfig['password'] = url.password
        if url.database:
            cssConfig['database'] = url.database
        if url.query and 'unix_socket' in url.query:
            cssConfig['socket'] = url.query['unix_socket']
    else:
        raise ValueError('Technology %s is not supported (yet)' % url.drivername)

    return cssConfig
