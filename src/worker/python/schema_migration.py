"""Module defining methods used in schema migration of Worker database.
"""

__all__ = ["make_migration_manager"]

import backoff
from functools import partial
import logging
import mysql.connector
import os

from lsst.db import utils
from lsst.qserv.admin.backoff import on_smig_backoff
from lsst.qserv.schema import SchemaMigMgr, Uninitialized


_log = logging.getLogger(__name__)


class WorkerMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for Worker metadata database.
    """

    # scripts are located in qmeta/ sub-dir
    def __init__(self, name, connection, scripts_dir):
        super().__init__(scripts_dir, connection)

    def current_version(self):
        """Returns current schema version.

        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """
        # Currently the worker metadata is reloaded every time the worker
        # boots up, so just say "Uninitialized".
        return Uninitialized


def make_migration_manager(name, connection, scripts_dir):
    """Factory method for admin schema migration manager

    This method is needed to support dynamic loading in `qserv-smig` script.

    Parameters
    ----------
    name : `str`
        Module name, e.g. "admin"
    connection : dbapi connection
        Database connection instance.
    scripts_dir : `str`
        Path where migration scripts are located, this is system-level directory,
        per-module scripts are usually located in sub-directories.
    """
    return WorkerMigrationManager(name, connection, scripts_dir)
