"""Module defining methods used in schema migration admin module.
"""

__all__ = ["make_migration_manager"]


import jinja2
import logging
import yaml
import sqlalchemy

from lsst.qserv.schema import SchemaMigMgr, Uninitialized
from .template import apply_template_cfg, get_template_cfg
from lsst.db import utils


_log = logging.getLogger(__name__)


class QMetaMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for QMeta database.
    """

    # scripts are located in qmeta/ sub-dir
    def __init__(self, name, connection, scripts_dir):
        SchemaMigMgr.__init__(self, scripts_dir)
        self.connection = connection

    def current_version(self):
        """Returns current schema version.

        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """
        stmt = apply_template_cfg(
            "SELECT COUNT(*) FROM mysql.user WHERE user='{{ qserv_user }}' AND host='{{ host }}'"
        )
        cursor = self.connection.cursor()
        cursor.execute(stmt)
        result = cursor.fetchall()
        try:
            # result is expected to be a list in a tuple, e.g. [(0,)]
            count = result[0][0]
        except:
            raise RuntimeError(f"Could not extract version from query result: {result}.")
        return Uninitialized if count == 0 else 0

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

        cursor = self.connection.cursor()

        for migration in migrations:
            _log.info("--- Executing migration script %s", migration.name)
            stmt = None
            if migration.filepath.endswith(".jinja"):
                env = jinja2.Environment(loader=jinja2.FileSystemLoader(migration.dirname),
                                         autoescape=jinja2.select_autoescape("sql"))
                stmt = env.get_template(migration.name).render(get_template_cfg())
            else:
                with open(migration.filepath, "r") as f:
                    stmt = f.read()
            cursor.execute(stmt, multi=True)
            _log.info("+++ Script %s completed successfully", migration.name)

        if not self.current_version() == migrations[-1]:
            raise RuntimeError(f"Failed to update admin schema to version {migrations[-1]}.")

        return migration.to_version


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
    return QMetaMigrationManager(name, connection, scripts_dir)
