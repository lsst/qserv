"""Mock for MySQL DBAPI."""

import logging
import re
import time
from collections import namedtuple
from typing import Any, Self

_LOG = logging.getLogger(__name__)

_num_re = re.compile(r"\d+")


ColDescriptor = namedtuple(
    "ColDescriptor", "name type_code display_size internal_size precision scale null_ok"
)


class MockCursor:
    """Mock cursor class.

    For our test we only need couple of methods.
    """

    arraysize = 1
    _query: str | None
    n_rows: int
    rows: list[tuple[int, str]]

    def __init__(self: Self) -> None:
        self._query = None
        self.n_rows = 0
        self.rows = []

    def execute(self: Self, query: str) -> None:
        _LOG.debug("executing query: %s", query)
        self._query = query
        self.n_rows = 2
        if self._query:
            # some parameterization, look at the query, if it contains any
            # number then use that number for the count of rows returned
            match = _num_re.search(self._query)
            if match:
                self.n_rows = min(int(match[0]), 10000)
        self.rows = [(i, f"row{i}") for i in range(self.n_rows)]
        # spend at least few milliseconds in query
        time.sleep(0.01)

    def fetchall(self: Self) -> list[tuple[int, str]]:
        rows = self.rows
        self.rows = []
        return rows

    def fetchmany(self: Self, arraysize: int | None = None) -> list[tuple[int, str]]:
        if arraysize is None:
            arraysize = self.arraysize
        rows = self.rows[:arraysize]
        self.rows = self.rows[arraysize:]
        return rows

    @property
    def rowcount(self: Self) -> int:
        return self.n_rows

    @property
    def description(self: Self) -> list[ColDescriptor]:
        # some randome codes
        return [
            ColDescriptor(
                name="ID", type_code=1, display_size=10, internal_size=4, precision=0, scale=1, null_ok=False
            ),
            ColDescriptor(
                name="name",
                type_code=15,
                display_size=32,
                internal_size=8,
                precision=0,
                scale=1,
                null_ok=True,
            ),
        ]


class MockConnection:
    def cursor(self: Self) -> MockCursor:
        return MockCursor()


def connect(*args: Any, **kwargs: Any) -> MockConnection:
    """Can take any parameters so it can be used as replacement for
    MySQLdb.connect() method.
    """
    return MockConnection()
