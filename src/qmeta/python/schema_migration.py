"""Module defining methods used in schema migration of QMeta database.
"""

__all__ = ["make_migration_manager"]

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging
import os

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.qserv.schema import SchemaMigMgr
from lsst.db import utils

#----------------------------------
# Local non-exported definitions --
#----------------------------------

_log = logging.getLogger(__name__)

#------------------------
# Exported definitions --
#------------------------


class QMetaMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for QMeta database.
    """

    # scripts are located in qmeta/ sub-dir
    def __init__(self, name, engine, scripts_dir):
        SchemaMigMgr.__init__(self, scripts_dir)
        self.engine = engine

    def current_version(self):
        """Returns current schema version.

        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """

        # Initial QMeta implementation did not have version number stored at all,
        # and we call this version 0. Since version=1 version number is stored in
        # QMetadata table with key="version"
        if not utils.tableExists(self.engine, "QMetadata"):
            _log.debug("QMetadata missing: version=0")
            return 0
        else:
            query = "SELECT value FROM QMetadata WHERE metakey = 'version'"
            result = self.engine.execute(query)
            row = result.first()
            if row:
                _log.debug("found version in database: %s", row[0])
                return int(row[0])

    def apply_migrations(self, migrations):
        """Apply migrations.

        Parameters
        ----------
        migrations : `list` [``Migrations``]
            Migrations to apply, in order.

        Returns
        -------
        version : `int`
            The current version number after applying migrations.
        """
        # apply all scripts
        for migration in migrations:
            _log.info("--- Executing migration script %s", migration.name)
            utils.loadSqlScript(self.engine, migration.filepath)
            _log.info("+++ Script %s completed successfully", migration.name)

        # make sure that current version is updated in database
        query = "UPDATE QMetadata SET value = {} WHERE metakey = 'version'".format(final)
        self.engine.execute(query)

        # read it back and compare with expected
        current = self.current_version()
        if current != to_version:
            raise RuntimeError("failed to update version number in database to {}, "
                                "current version is now {}".format(to_version, current))
        return current


def make_migration_manager(name, engine, scripts_dir):
    """Factory method for QMeta schema migration manager

    This method is needed to support dynamic loading in `qserv-smig` script.

    Parameters
    ----------
    name : `str`
        Module name, e.g. "qmeta"
    engine : object
        Sqlalchemy engine instance.
    scripts_dir : `str`
        Path where migration scripts are located, this is system-level directory,
        per-module scripts are usually located in sub-directories.
    """
    return QMetaMigrationManager(name, engine, scripts_dir)
