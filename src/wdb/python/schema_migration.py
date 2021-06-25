"""Module defining methods used in schema migration of qservw_worker database.
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


class WdbMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for qservw_worker database.
    """

    def __init__(self, name, engine, scripts_dir):

        SchemaMigMgr.__init__(self)

        self.engine = engine

        # scripts are located in qmeta/ sub-dir
        scripts_dir = os.path.join(scripts_dir, name)

        # find all migration scripts, add full script path to each
        scripts = self.find_scripts(scripts_dir, self.script_match)
        self.scripts = [(v0, v1, fname, os.path.join(scripts_dir, fname))
                        for v0, v1, fname in scripts]

    def current_version(self):
        """Returns current schema version.
        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """

        # Initial qservw_worker implementation did not have version number stored at all,
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

    def migrate(self, version=None, do_migrate=False):
        """Perform schema migration from current version to given version.
        Parameters
        ----------
        version : int or None
            If None then migrate to latest known version, otherwise only
            migrate to given version.
        do_migrate : bool
            If True performa migration, otherwise only print steps that
            should be performed
        Returns
        -------
        None if no migrations were performed or the version number at
        which migration has stopped.
        Raises
        ------
        Exception is raised for any migration errors. The state of the
        database is not guaranteed to be consistent after exception.
        """

        # checks for requested version
        if version is not None:
            current = self.current_version()
            if current >= version:
                _log.debug("current version (%s) is already same or newer as requested (%s)",
                           current, version)
                return None
            final_versions = [s[1] for s in self.scripts]
            if version not in final_versions:
                raise ValueError("No known migration scripts for requested version ({})".format(version))

        # apply all migration scripts in a loop
        result = None
        current = self.current_version()
        while True:

            if version is not None and current >= version:
                _log.debug("current version (%s) is now same or newer as requested (%s)",
                           current, version)
                break

            # find all migrations for current version
            scripts = [s for s in self.scripts if s[0] == current]
            if not scripts:
                _log.debug("no migration scripts found for current version (%s)", current)
                break

            # if there are more than one final version use the latest or requested one
            final_versions = set([s[1] for s in scripts])
            final = max(final_versions)
            if version is not None:
                if version in final_versions:
                    final = version

            # only use scripts for that final version
            scripts = sorted([s[2:4] for s in scripts if s[1] == final])

            # apply all scripts
            for script, path in scripts:
                _log.info("--- Executing migration script %s", script)
                if do_migrate:
                    utils.loadSqlScript(self.engine, path)
                    _log.info("+++ Script %s completed successfully", script)

            # make sure that current version is updated in database
            if do_migrate:
                query = "UPDATE QMetadata SET value = {} WHERE metakey = 'version'".format(final)
                self.engine.execute(query)

                # read it back and compare with expected
                current = self.current_version()
                if current != final:
                    raise RuntimeError("failed to update version number in database to {}, "
                                       "current version is now {}".format(final, current))
            else:
                # pretend that we migrated to this new version
                current = final

            result = final

        return result


def make_migration_manager(name, engine, scripts_dir):
    """Factory method for qservw_worker schema migration manager
    This method is needed to support dynamic loading in `qserv-smig` script.
    Parameters
    ----------
    name : `str`
        Module name, e.g. "wdb"
    engine : object
        Sqlalchemy engine instance.
    scripts_dir : `str`
        Path where migration scripts are located, this is system-level directory,
        per-module scripts are usually located in sub-directories.
    """
    return WdbMigrationManager(name, engine, scripts_dir)
