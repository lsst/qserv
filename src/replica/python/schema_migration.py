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

"""Module defining methods used in schema migration of qservReplica database.
"""

__all__ = ["make_migration_manager"]

from contextlib import closing
import logging

from lsst.qserv.schema import SchemaMigMgr, Uninitialized


_log = logging.getLogger(__name__)

replicaDb = "qservReplica"

repl_schema_version = "repl_schema_version"


class MasterReplicationMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for the master replication
    controller database.
    """

    def __init__(self, name, connection, scripts_dir, set_initial_configuration):
        self.set_initial_configuartion = set_initial_configuration
        super().__init__(scripts_dir, connection)

    def current_version(self):
        """Returns current schema version.

        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """
        # If the database does not exist then the version is `Uninitialized`.
        if not self.databaseExists(replicaDb):
            return Uninitialized

        self.connection.database = replicaDb
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("SELECT value FROM ReplicaMetadata WHERE metakey = 'version'")
            result = cursor.fetchone()
        if not result:
            return Uninitialized
        return int(result[0])

    def _set_version(self, version):
        """Set the version number stored in ReplicaMetadata."""
        # make sure that current version is updated in database
        self.connection.database = replicaDb
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(f"UPDATE ReplicaMetadata SET value = {version} WHERE metakey = 'version'")
        _log.info(f"Set replica schema version to {version}.")
        self.connection.commit()

        # read it back and compare with expected
        current = self.current_version()
        if current != version:
            raise RuntimeError(
                f"Failed to update version number in database to {version}, current version is now {current}")

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
        current_version = self.current_version()
        to_version = super().apply_migrations(migrations)
        if current_version == Uninitialized and self.set_initial_configuartion:
            self.set_initial_configuartion()
        self._set_version(to_version)
        return to_version


def make_migration_manager(name, connection, scripts_dir, set_initial_configuration=None):
    """Factory method for master replication controller schema migration
    manager

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
    set_initial_configuration : function or `None`
        A function to be called to set the initial configuration of the repl database.
        Will only be called if the schema is migrated from None to version 1. Optional.
    """
    return MasterReplicationMigrationManager(name, connection, scripts_dir, set_initial_configuration)
