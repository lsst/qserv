"""Mock for MySQL DBAPI.
"""

import logging
import re


_LOG = logging.getLogger(__name__)

_num_re = re.compile("\d+")

class MockCursor:
    """Mock cursor class.

    For our test we only need couple of methods.
    """
    def __init__(self):
        self._query = None

    def execute(self, query):
        _LOG.info("executing query: %s", query)
        self._query = query

    def fetchall(self):
        # some parameterization, look at the query, if it contains any number
        # then use that number for the count of rows returned
        nrows = 2
        if self._query:
            match = _num_re.search(self._query)
            if match:
                nrows = int(match[0])
        rows = [(i, f"row{i}") for i in range(nrows)]
        return rows


class MockConnection:

    def cursor(self):
        return MockCursor()


def connect(*args, **kwargs):
    """Can take any parameters so it can be used as replacement for
    MySQLdb.connect() method.
    """
    return MockConnection()
