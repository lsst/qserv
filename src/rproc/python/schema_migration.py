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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


"""Module defining methods used in schema migration of rproc database.
"""

__all__ = ["make_migration_manager"]

import logging

from lsst.qserv.schema import SchemaMigMgr, Uninitialized


_log = logging.getLogger(__name__)
qservResultDb = "qservResult"


class RprocMigrationManager(SchemaMigMgr):
    """Class implementing schema migration for rproc database.
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

        # If the result database does not exist then rproc is Uninitialized.
        if not self.databaseExists(qservResultDb):
            return Uninitialized

        # rproc does not have multiple versions yet, so if the database exists
        # then assume version 0.
        return 0


def make_migration_manager(name, connection, scripts_dir):
    """Factory method for schema migration manager

    This method is needed to support dynamic loading in `qserv-smig` script.

    Parameters
    ----------
    name : `str`
        Module name, e.g. "admin"
    connection : dbapi connection
        TODO fix all cases, this is the url string I think?
    scripts_dir : `str`
        Path where migration scripts are located, this is system-level directory,
        per-module scripts are usually located in sub-directories.
    """
    return RprocMigrationManager(name, connection, scripts_dir)
