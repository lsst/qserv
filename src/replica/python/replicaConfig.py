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


import backoff
from contextlib import closing
import mysql.connector
import logging
from urllib.parse import urlparse

from lsst.qserv.admin.backoff import qserv_backoff, on_backoff


replicaDb = "qservReplica"

_log = logging.getLogger(__name__)
database_error_connection_refused_code = 2003


@qserv_backoff(
    exception=mysql.connector.errors.DatabaseError,
    on_backoff=on_backoff(log=_log),
    # Do give up, unless error is that the connection was refused (assume db is starting up)
    giveup=lambda e: e.errno != database_error_connection_refused_code,
)
def applyConfiguration(connection: str, sql: str) -> None:
    """Apply configuration sql to the replication controller database."""
    c = urlparse(connection)
    with closing(
        mysql.connector.connect(
            user=c.username,
            password=c.password,
            host=c.hostname,
            port=c.port,
        )
    ) as cnx:
        cnx.database = replicaDb
        with closing(cnx.cursor()) as cursor:
            for result in cursor.execute(sql, multi=True):
                pass
        cnx.commit()
    _log.debug(f"Applied configuration: \n{sql}")
