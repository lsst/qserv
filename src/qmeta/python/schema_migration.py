"""Module defining methods used in schema migration of QMeta database.
"""

__all__ = ["make_migration_manager"]

import backoff
import logging
import mysql.connector
from typing import Sequence

from lsst.qserv.admin.qserv_backoff import max_backoff_sec, on_backoff
from lsst.qserv.schema import Migration, SchemaMigMgr, Uninitialized, Version


_log = logging.getLogger(__name__)
qservMetaDb = "qservMeta"


class QMetaMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for QMeta database.
    """

    # scripts are located in qmeta/ sub-dir
    def __init__(self, name: str, connection: str, scripts_dir: str):
        super().__init__(scripts_dir, connection)

    def current_version(self) -> Version:
        """Returns current schema version.

        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """

        # If the `qservMeta` database does not exist then QMeta is
        # Uninitialized.
        cursor = self.connection.cursor()
        cursor.execute("SHOW DATABASES")
        if ('qservMeta',) not in cursor.fetchall():
            return Version(Uninitialized)

        # Initial QMeta implementation did not have version number stored at all,
        # and we call this version 0. Since version=1 version number is stored in
        # QMetadata table with key="version"
        cursor.execute("select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = 'QMetadata';")
        if not cursor.fetchone():
            return 0

        self.connection.database = qservMetaDb
        cursor = self.connection.cursor()
        cursor.execute("SELECT value FROM QMetadata WHERE metakey = 'version'")
        result = cursor.fetchone()
        if not result:
            return Uninitialized
        return Version(int(result[0]))

    @backoff.on_exception(
        exception=(mysql.connector.errors.OperationalError, mysql.connector.errors.ProgrammingError),
        wait_gen=backoff.expo,
        on_backoff=on_backoff(log=_log),
        max_time=max_backoff_sec,
    )
    def _set_version(self, version: int) -> None:
        """Set the version number stored in QMetadata."""
        # make sure that current version is updated in database
        query = f"UPDATE QMetadata SET value = {version} WHERE metakey = 'version'"
        self.connection.database = qservMetaDb
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()

        # read it back and compare with expected
        current = self.current_version()
        if current != version:
            raise RuntimeError(
                f"Failed to update version number in database to {version}, current version is now {current}")

    def apply_migrations(self, migrations: Sequence[Migration]) -> Version:
        """Apply migrations.

        Parameters
        ----------
        migrations : `list` [``Migration``]
            Migrations to apply, in order.

        Returns
        -------
        version : `Version`
            The current version after applying migrations.
        """
        version = super().apply_migrations(migrations)
        self._set_version(version)
        return Version(version)


def make_migration_manager(name: str, connection: str, scripts_dir: str) -> SchemaMigMgr:
    """Factory method for admin schema migration manager

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
    """
    return QMetaMigrationManager(name, connection, scripts_dir)
