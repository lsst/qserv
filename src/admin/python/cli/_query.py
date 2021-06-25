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

from contextlib import closing
import logging
import mysql.connector


_log = logging.getLogger(__name__)


def query(statement, host, port):
    with closing(mysql.connector.connect(user="qsmaster", host=host, port=port)) as connection:
        cursor = connection.cursor()
        cursor.execute(statement)
        print(cursor.column_names)
        print(cursor.fetchall())

# TODO needs output formatting similar to astropy.table
#    width = max(cursor.column_names)
