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
from sqlalchemy.engine.url import make_url
from typing import Callable, List, Optional

from lsst.qserv.schema import (
    Migration,
    SchemaMigMgr,
    Version,
    Uninitialized,
)


_log = logging.getLogger(__name__)


repl_schema_version = "repl_schema_version"


class MasterReplicationMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for the master replication
    controller database.

    Parameters
    ----------
    Same as make_migration_manager
    """

    def __init__(
        self,
        name: str,
        connection: str,
        scripts_dir: str,
        set_initial_configuration: Optional[Callable[[], None]],
        repl_connection: Optional[str],
    ):
        self.repl_connection = repl_connection
        self.set_initial_configuartion = set_initial_configuration
        super().__init__(scripts_dir, connection)
        if not self.database:
            raise RuntimeError(
                "The name of the replication database must be provided in the connection URI."
            )

    def current_version(self) -> Version:
        """Returns current schema version.

        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """
        # If the database does not exist then the version is `Uninitialized`.
        if not self.databaseExists(self.database):
            return Version(Uninitialized)

        self.connection.database = self.database
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("SELECT value FROM QMetadata WHERE metakey = 'version'")
            result = cursor.fetchone()
        if not result:
            return Version(Uninitialized)
        return Version(int(result[0]))

    def _set_version(self, version: int) -> None:
        """Set the version number stored in QMetadata."""
        # make sure that current version is updated in database
        self.connection.database = self.database
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(f"UPDATE QMetadata SET value = {version} WHERE metakey = 'version'")
            warnings = cursor.fetchwarnings()
            if warnings:
                _log.warn("Warnings were issued when updating version to %s", version)
        _log.info(f"Set replica schema version to {version}.")
        self.connection.commit()

        # read it back and compare with expected
        current = self.current_version()
        if current != version:
            raise RuntimeError(
                f"Failed to update version number in database to {version}, current version is now {current}")

    def _create_database(self) -> None:
        """Create the replication controller database.
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(f"CREATE DATABASE {self.database};")
            warnings = cursor.fetchwarnings()
            if warnings:
                _log.warn("Warnings were creating database %s", self.database)
        _log.info(f"Created database {self.database}.")
        self.connection.commit()

    def _create_users(self) -> None:
        """Create the users for the replication controller database.
        """
        if not self.repl_connection:
            raise RuntimeError(
                "A non-admin replication database connection uri must be provided to initialize the "
                "replication database."
            )
        user = make_url(self.repl_connection).username
        if not user:
            raise RuntimeError(
                "To initialize the replication database, the non-admin connection uri must contain a user "
                "name."
            )
        for stmt in [
            f"CREATE USER IF NOT EXISTS {user}@localhost;",
            f"CREATE USER IF NOT EXISTS {user}@'%';",
            f"GRANT ALL ON qservReplica.* TO  {user}@localhost;",
            f"GRANT ALL ON qservReplica.* TO  {user}@'%';",
            f"FLUSH PRIVILEGES;",
        ]:
            with closing(self.connection.cursor()) as cursor:
                _log.info(f"executing: {stmt}")
                cursor.execute(stmt)
                warnings = cursor.fetchwarnings()
                if warnings:
                    _log.warn("Warnings were issued when creating user %s", {user})
        self.connection.commit()

    def apply_migrations(self, migrations: List[Migration]) -> Version:
        """Apply migrations.

        Parameters
        ----------
        migrations : `list` [``Migrations``]
            Migrations to apply, in order.

        Returns
        -------
        version : `Version`
            The current version number after applying migrations.
        """
        current_version = self.current_version()
        # The replica migrate-from-None scripts do not create the database or
        # the non-admin user, so if we are initializing the replica database
        # schema, first create them.
        if current_version == Uninitialized:
            self._create_database()
            self._create_users()
        self.connection.database = self.database
        to_version = super().apply_migrations(migrations)
        if current_version == Uninitialized and self.set_initial_configuartion:
            self.set_initial_configuartion()
        self._set_version(to_version)
        return Version(to_version)


def make_migration_manager(
    name: str,
    connection: str,
    scripts_dir: str,
    set_initial_configuration: Optional[Callable[[], None]] = None,
    repl_connection: Optional[str] = None,
) -> SchemaMigMgr:
    """Factory method for master replication controller schema migration
    manager

    This method is needed to support dynamic loading in `qserv-smig` script.

    Parameters
    ----------
    name : `str`
        Module name, e.g. "admin"
    connection : `str`
        The uri to the module database.
    scripts_dir : `str`
        Path where migration scripts are located, this is system-level directory,
        per-module scripts are usually located in sub-directories.
    set_initial_configuration : function, optional.
        A function to be called to set the initial configuration of the repl database.
        Will only be called if the schema is migrated from None to version 1. Optional.
    repl_connection : `str`
        Database connection string for the non-admin user.
    """
    return MasterReplicationMigrationManager(
        name,
        connection,
        scripts_dir,
        set_initial_configuration,
        repl_connection,
    )
