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

import csv
import json
import logging
import math
import os
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Collection, Generator, Sequence
from filecmp import dircmp
from typing import Any, TextIO
from urllib.parse import urljoin, urlparse

import requests
import requests.auth
import urllib3
from requests_toolbelt.multipart.encoder import MultipartEncoder

from .replication_interface import repl_api_version

_log = logging.getLogger(__name__)


default_async_timeout = 600  # seconds

query_mode_mysql = "mysql"
query_mode_qserv_attached = "qserv_attached"
query_mode_qserv_detached = "qserv_detached"


class FrontEndError(Exception):
    """
    A custom exception class for errors reported by the REST API of the HTTP-based
    frontend of Qserv. Inherits from Python's built-in Exception class.
    """

    def __init__(self, context: str, error: str):
        """
        Initializes the FrontEndError.

        Parameters
        ----------
        context : str
            A descriptive error context.
        error : str
            A detailed error message.
        """
        super().__init__("FrontEndError")
        self.message = f"{context}, error:{error}"

    def __str__(self) -> str:
        """
        Returns a string representation of the exception.
        """
        return self.message


class ITestQuery:
    """Represents a query to execute, with utilities for loading the query from a file.

    Parameters
    ----------
    query_file : `str`
        A file that contains a query to execute, may contain tags to be replaced
        and pragmas used during execution.
    output_dir : `str`
        The location where generated artifacts can be written and read for
        comparison.
    modes : `list` [`str`] or `None`
        Any/all of "sync", "async", "mysql". Sync and async run the tests in
        qserv, mysql runs the tests in the mysql reference database. Optional,
        defaults to ["sync", "async", "mysql"]
    """

    def __init__(self, query_file: str, output_dir: str, modes: list[str] | None = None) -> None:
        self.query_file = query_file
        with open(self.query_file) as f:
            self.query_t = f.read()
        pragmas = self._get_pragmas(self.query_t)
        self.output_dir = output_dir
        self.modes = ["sync", "async", "mysql"] if modes is None else modes
        self.column_names = "noheader" not in pragmas
        self.no_async = "no_async" in pragmas
        if self.no_async:
            # no_async was not supported by the old test runner (benchmark.py in
            # qserv_testdata), and there is currently only one test with the
            # no_async pragma and it's marked FIXME:
            # case04/queries/0020_showColumnsFromDeepForcedSource.FIXME
            # When this is fixed, if it must not run asynchronously then we must
            # add this feature.
            raise NotImplementedError("no_async is not handled by the integration test runner.")
        self.async_timeout = int(pragmas.get("async_timeout", None) or default_async_timeout)
        self.sort_result = "sortresult" in pragmas
        self.out_file_t = os.path.join(
            self.output_dir,
            "{mode}",
            os.path.basename(self.query_file).replace(".sql", ".txt"),
        )
        _log.debug(
            "ITestQuery query_file:%s, out_file:%s, modes:%s, "
            "column_names:%s, no_async:%s, "
            "async_timeout:%s, sort_result:%s",
            query_file,
            self.out_file_t,
            modes,
            self.column_names,
            self.no_async,
            self.async_timeout,
            self.sort_result,
        )

    def _run(self, args: list[str], outfile: str | None = None) -> str:
        """Run a prepared command.
        Checks the return code & raises if it is not 0.

        Parameters
        ----------
        args : `list` [`str`]
            The parameters to subprocess.run
        outfile : `str` or `None`
            If `None`, will return stdout. If not `None` will write stdout to a
            file at the provided location and return an empty string.

        Returns
        -------
        output : `str`
            An empty string or the stdout generated by the subprocess.
        """

        def _do(stdout: int | TextIO) -> str:
            result = subprocess.run(
                args,
                stdout=stdout,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode != 0:
                _log.error(
                    'run command "%s" failed. stdout:%s, stderr:%s',
                    " ".join(args),
                    result.stdout,
                    result.stderr,
                )
            result.check_returncode()
            return result.stdout

        if outfile is None:
            return _do(subprocess.PIPE)
        else:
            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            with open(outfile, "w") as f:
                _do(f)
                if self.sort_result:
                    self._sort(outfile)
            return ""

    def _sort(self, file: str) -> None:
        """Sort the lines in a result file.

        Parameters
        ----------
        file : `str`
            The path to a file whose lines will be sorted.
        """
        with open(file, "r+b") as f:
            sorted_lines = sorted(f.readlines())
            f.seek(0)
            f.writelines(sorted_lines)

    def run_detached(self, connection: str, qserv: bool, database: str) -> None:
        """Run the query on the db at connection using SUBMIT, and then fetch
        results when they are available.

        Parameters
        ----------
        connection : `str`
            URI to the database to run the query on.
        qserv : `bool`
            True if the query is being run on a qserv instance.
        database : `str`
            The name of the database to use.
        """
        query = self._render_query(self.query_t, qserv, database)
        parsed = urlparse(connection)
        args = [
            "mysql",
            "--host",
            f"{parsed.hostname}",
            f"--port={parsed.port}",
            f"--user={parsed.username}",
            f"--password={parsed.password}",
            "--batch",
            "--binary-as-hex",
            "--skip-column-names",
            "--database",
            database,
            "-e",
            f"SUBMIT {query}",
        ]
        data = self._run(args)
        try:
            query_id = int(data.split()[0])
            _log.debug("SQLCmd.execute query ID = %s", query_id)
        except Exception as e:
            raise RuntimeError(f"Failed to read query ID from SUBMIT: {data}") from e
            # wait until query completes
        args = [
            "mysql",
            "--host",
            f"{parsed.hostname}",
            f"--port={parsed.port}",
            f"--user={parsed.username}",
            f"--password={parsed.password}",
            "--batch",
            "--binary-as-hex",
            "--skip-column-names",
            "-e",
            f"SELECT STATUS FROM INFORMATION_SCHEMA.QUERIES WHERE ID = {query_id}",
        ]
        _log.debug("SQLCmd.execute waiting for query to complete")
        end_time = time.time() + self.async_timeout
        while time.time() < end_time:
            status = self._run(args).strip()
            _log.debug("SQLCmd.execute query status = %s", status)
            if status == "COMPLETED":
                break
        else:
            raise RuntimeError("Timeout while waiting for detached query")

        args = [
            "mysql",
            "--host",
            f"{parsed.hostname}",
            f"--port={parsed.port}",
            f"--user={parsed.username}",
            f"--password={parsed.password}",
            "--batch",
            "--binary-as-hex",
            "-e",
            f"SELECT * from qserv_result({query_id})",
        ]
        if not self.column_names:
            args.insert(1, "--skip-column-names")
        self._run(args, self.out_file_t.format(mode=query_mode_qserv_detached))

        _log.debug("SQLCmd.execute deleting result tables of query ID = %s", query_id)
        args = [
            "mysql",
            "--host",
            f"{parsed.hostname}",
            f"--port={parsed.port}",
            f"--user={parsed.username}",
            f"--password={parsed.password}",
            "--batch",
            "--binary-as-hex",
            "-e",
            f"CALL qserv_result_delete({query_id})",
        ]
        self._run(args)

    def run_attached(self, connection: str, qserv: bool, database: str) -> None:
        """Run the query on the db at the given connection
        attached/synchronously - do not SUBMIT and wait for result.

        Parameters
        ----------
        connection : `str`
            URI to the database to run the query on.
        qserv : `bool`
            True if running on a qserv instance, False if running on a mysql instance.
        database : `str`
            The name of the database to run in.
        """
        query = self._render_query(self.query_t, qserv, database)
        parsed = urlparse(connection)
        _log.debug("connection:%s parsed:%s", connection, str(parsed))
        _log.debug("run_attached qserv:%s", qserv)
        args = [
            "mysql",
            "--host",
            f"{parsed.hostname}",
            f"--port={parsed.port}",
            f"--user={parsed.username}",
            f"--password={parsed.password}",
            "--batch",
            "--binary-as-hex",
            "--database",
            database,
            "-e",
            query,
        ]
        if not self.column_names:
            args.insert(1, "--skip-column-names")
        self._run(
            args,
            self.out_file_t.format(mode=query_mode_qserv_attached if qserv else query_mode_mysql),
        )

    @staticmethod
    def _get_pragmas(query: str) -> dict[str, str | None]:
        """Get the pragmas from the contents of a query file.

        Parameters
        ----------
        query : str
            The contents of a query file, including pragmas for query execution.

        Returns
        -------
        pragmas : `dict` [`str`, `str`]
            The pramas (keys) and their values (values)
        """
        pragmas: dict[str, str | None] = {}
        for line in query.split("\n"):
            if not line.startswith("--"):
                continue
            # check for pragma, format is:
            #    '-- pragma keyval [keyval...]'
            #    where keyval is 'key=value' or 'key'
            words = line.split()
            if len(words) > 1 and words[1] == "pragma":
                for keyval in words[2:]:
                    kv = keyval.split("=", 1)
                    pragmas[kv[0]] = kv[1] if len(kv) > 1 else None
        return pragmas

    def _render_query(self, query_t: str, with_qserv: bool, database: str) -> str:
        """Filters the contents of a qserv integration test SQL query file based
        on qserv/mysql mode.

        Qserv integration tests can be annotated to affect query processing:
        "-- withQserv" will be run only on qserv (not when using the reference database)
        "-- noQserv" will be run only on the reference database (not when using qserv)
        "--" (standard sql comments) will be removed.
        "DBTAG_A" (without the quotes) will be replaced with the current database name.

        Parameters
        ----------
        query_t : `str`
            The text of a qserv integration test query file.
        with_qserv : bool
            If `True` then prepare query for qserv, otherwise for mysql.
        database: `str`
            The database name to replace tag DBTAG_A with; "database" (without
            the quotes).

        Returns
        -------
        query : `str`
            The query text.
        """
        qtext = []
        for line in query_t.split("\n"):
            # squeeze/strip spaces
            line = line.strip()
            line = re.sub(" +", " ", line)
            if not line:
                # skip empty lines
                pass
            elif with_qserv and line.startswith("-- withQserv"):
                # strip the "-- withQserv" text
                qtext.append(line[13:])
            elif line.endswith("-- noQserv"):
                if with_qserv:
                    # skip this line
                    pass
                else:
                    # strip the "-- noQserv" text
                    qtext.append(line[:-10])
            elif not line.startswith("--"):
                # append all non-commented lines
                qtext.append(line)
        query = " ".join(qtext)
        if database:
            query = query.format(DBTAG_A=f"{database}.")
        return query


class ITestQueryHttp:
    """Represents a query to execute, with utilities for loading the query from a file.

    Parameters
    ----------
    query_file : `str`
        A file that contains a query to execute, may contain tags to be replaced
        and pragmas used during execution.
    output_dir : `str`
        The location where generated artifacts can be written and read for
        comparison.
    modes : `list` [`str`] or `None`
        Any/all of "sync", "async", "mysql". Sync and async run the tests in
        qserv, mysql runs the tests in the mysql reference database. Optional,
        defaults to ["sync", "async", "mysql"]
    """

    def __init__(self, query_file: str, output_dir: str, modes: list[str] | None = None) -> None:
        self.query_file = query_file
        with open(self.query_file) as f:
            self.query_t = f.read()
        pragmas = self._get_pragmas(self.query_t)
        self.output_dir = output_dir
        self.modes = ["sync", "async", "mysql"] if modes is None else modes
        self.column_names = "noheader" not in pragmas
        self.no_async = "no_async" in pragmas
        if self.no_async:
            # no_async was not supported by the old test runner (benchmark.py in
            # qserv_testdata), and there is currently only one test with the
            # no_async pragma and it's marked FIXME:
            # case04/queries/0020_showColumnsFromDeepForcedSource.FIXME
            # When this is fixed, if it must not run asynchronously then we must
            # add this feature.
            raise NotImplementedError("no_async is not handled by the integration test runner.")
        self.async_timeout = int(pragmas.get("async_timeout", None) or default_async_timeout)
        self.sort_result = "sortresult" in pragmas
        self.out_file_t = os.path.join(
            self.output_dir,
            "{mode}",
            os.path.basename(self.query_file).replace(".sql", ".txt"),
        )
        _log.debug(
            "ITestQueryHttp query_file:%s, out_file:%s, modes:%s, "
            "column_names:%s, no_async:%s, "
            "async_timeout:%s, sort_result:%s",
            query_file,
            self.out_file_t,
            modes,
            self.column_names,
            self.no_async,
            self.async_timeout,
            self.sort_result,
        )

    def _run(self, args: list[str], outfile: str | None = None) -> str:
        """Run a prepared command.
        Checks the return code & raises if it is not 0.

        Parameters
        ----------
        args : `list` [`str`]
            The parameters to subprocess.run
        outfile : `str` or `None`
            If `None`, will return stdout. If not `None` will write stdout to a
            file at the provided location and return an empty string.

        Returns
        -------
        output : `str`
            An empty string or the stdout generated by the subprocess.
        """

        def _do(stdout: int | TextIO) -> str:
            result = subprocess.run(
                args,
                stdout=stdout,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                errors="replace",
            )
            if result.returncode != 0:
                _log.error(
                    'run command "%s" failed. stdout:%s, stderr:%s',
                    " ".join(args),
                    result.stdout,
                    result.stderr,
                )
            result.check_returncode()
            return result.stdout

        if outfile is None:
            return _do(subprocess.PIPE)
        else:
            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            with open(outfile, "w") as f:
                _do(f)
                if self.sort_result:
                    self._sort(outfile)
            return ""

    def _sort(self, file: str) -> None:
        """Sort the lines in a result file.

        Parameters
        ----------
        file : `str`
            The path to a file whose lines will be sorted.
        """
        with open(file, "r+b") as f:
            sorted_lines = sorted(f.readlines())
            f.seek(0)
            f.writelines(sorted_lines)

    def run_attached(self, connection: str, database: str) -> None:
        """Run the query on the db at the given connection
        attached/synchronously - do not SUBMIT and wait for result.

        Parameters
        ----------
        connection : `str`
            URI to the database to run the query on.
        database : `str`
            The name of the database to run in.
        """
        qserv = False
        query = self._render_query(self.query_t, qserv, database)
        parsed = urlparse(connection)
        _log.debug("run_attached qserv:%s", qserv)
        args = [
            "mysql",
            "--host",
            f"{parsed.hostname}",
            f"--port={parsed.port}",
            f"--user={parsed.username}",
            f"--password={parsed.password}",
            "--batch",
            "--binary-as-hex",
            "--database",
            database,
            "-e",
            query,
        ]
        if not self.column_names:
            args.insert(1, "--skip-column-names")
        self._run(
            args,
            self.out_file_t.format(mode=query_mode_qserv_attached if qserv else query_mode_mysql),
        )

    def run_attached_http(self, connection: str, user: str, password: str, database: str) -> None:
        """Run the query via the HTTP frontend at the given connection
        attached/synchronously - do not SUBMIT and wait for result.

        Parameters
        ----------
        connection : `str`
            URI to the HTTP frontend to run the query on.
        user : `str`
            The user to use to connect to the HTTP frontend.
        password : `str`
            The password to use to connect to the HTTP frontend.
        database : `str`
            The name of the database to run in.
        """
        qserv = True
        query = self._render_query(self.query_t, qserv, database)
        _log.debug("run_attached_http qserv:%s", qserv)

        # Submit the query, check and analyze the completion status
        svc = str(urljoin(connection, f"/query?version={repl_api_version}"))
        req = requests.post(
            svc,
            json={"query": query, "database": database, "binary_encoding": "hex"},
            verify=False,
            auth=(requests.auth.HTTPBasicAuth(user, password)),
        )
        req.raise_for_status()
        res = req.json()
        if res["success"] == 0:
            raise RuntimeError(
                f"Failed to execute the attached query: {query}, server serror: {res['error']}"
            )
        self._write_result(self.out_file_t.format(mode=query_mode_qserv_attached), res)

    def run_detached_http(self, connection: str, user: str, password: str, database: str) -> None:
        """Run the query via the HTTP frontend at connection using SUBMIT, and then fetch
        results when they are available.

        Parameters
        ----------
        connection : `str`
            URI to the HTTP frontend to run the query on.
        user : `str`
            The user to use to connect to the HTTP frontend.
        password : `str`
            The password to use to connect to the HTTP frontend.
        database : `str`
            The name of the database to run in.
        """
        qserv = True
        query = self._render_query(self.query_t, qserv, database)
        _log.debug("run_detached_http qserv:%s", qserv)

        # Submit the query via the async service, check and analyze the completion status
        svc = str(urljoin(connection, f"/query-async?version={repl_api_version}"))
        req = requests.post(
            svc,
            json={"query": query, "database": database},
            verify=False,
            auth=(requests.auth.HTTPBasicAuth(user, password)),
        )
        req.raise_for_status()
        res = req.json()
        if res["success"] == 0:
            raise RuntimeError(
                f"Failed to execute the detached query: {query}, server serror: {res['error']}"
            )
        query_id = res["queryId"]

        # Wait for the completion of the query by periodically checking the query status
        _log.debug(f"SQLCmd.execute waiting for query {query_id} to complete")
        end_time = time.time() + self.async_timeout
        while time.time() < end_time:
            # Submit a request to check a status of the query
            svc = str(urljoin(connection, f"/query-async/status/{query_id}?version={repl_api_version}"))
            req = requests.get(svc, verify=False, auth=(requests.auth.HTTPBasicAuth(user, password)))
            req.raise_for_status()
            res = req.json()
            if res["success"] == 0:
                raise RuntimeError(
                    f"Failed to check a status of the detached query: {query_id}, server serror: "
                    f"{res['error']}"
                )
            status = res["status"]["status"]
            _log.debug("SQLCmd.execute query status = %s", status)
            if status == "COMPLETED":
                break
            elif status == "EXECUTING":
                # The query is still running, wait a bit and try again
                time.sleep(0.1)
            else:
                raise RuntimeError(
                    f"Detached query {query_id} failed, status: {status}, error: {res['status']['error']}"
                )
        else:
            raise RuntimeError(f"Timeout while waiting for detached query {query_id}")

        # Make another request to pull the result set
        _log.debug("SQLCmd.execute pulling result set of query ID = %s", query_id)
        svc = str(
            urljoin(
                connection, f"/query-async/result/{query_id}?version={repl_api_version}&binary_encoding=hex"
            )
        )
        req = requests.get(svc, verify=False, auth=(requests.auth.HTTPBasicAuth(user, password)))
        req.raise_for_status()
        res = req.json()
        if res["success"] == 0:
            raise RuntimeError(
                f"Failed to retrieve a result set of the detached query: {query_id}, server serror: "
                f"{res['error']}"
            )
        self._write_result(self.out_file_t.format(mode=query_mode_qserv_detached), res)

        _log.debug("SQLCmd.execute deleting result tables of query ID = %s", query_id)
        svc = str(urljoin(connection, f"/query-async/result/{query_id}?version={repl_api_version}"))
        req = requests.delete(svc, verify=False, auth=(requests.auth.HTTPBasicAuth(user, password)))
        req.raise_for_status()
        res = req.json()
        if res["success"] == 0:
            raise RuntimeError(
                f"Failed to delete the result set of the detached query: {query_id}, "
                f"server serror: {res['error']}"
            )

    def _write_result(self, outfile: str, res: Any) -> None:
        _log.debug("_write_result out_file:%s", outfile)
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        with open(outfile, mode="w", encoding="utf-8") as f:
            if self.column_names and len(res["rows"]) != 0:
                f.write("\t".join(coldef["column"] for coldef in res["schema"]))
                f.write("\n")
            for row in res["rows"]:
                line: str = ""
                for col_idx in range(len(row)):
                    if col_idx != 0:
                        line = line + "\t"
                    # This prefix needs to be added to make the binary string compatible with
                    # the output [produced by MySQL and Qserv.
                    if res["schema"][col_idx]["is_binary"]:
                        line = line + "0x"
                    line = line + row[col_idx]
                f.write(line)
                f.write("\n")
        if self.sort_result:
            self._sort(outfile)

    @staticmethod
    def _get_pragmas(query: str) -> dict[str, str | None]:
        """Get the pragmas from the contents of a query file.

        Parameters
        ----------
        query : str
            The contents of a query file, including pragmas for query execution.

        Returns
        -------
        pragmas : `dict` [`str`, `str`]
            The pramas (keys) and their values (values)
        """
        pragmas: dict[str, str | None] = {}
        for line in query.split("\n"):
            if not line.startswith("--"):
                continue
            # check for pragma, format is:
            #    '-- pragma keyval [keyval...]'
            #    where keyval is 'key=value' or 'key'
            words = line.split()
            if len(words) > 1 and words[1] == "pragma":
                for keyval in words[2:]:
                    kv = keyval.split("=", 1)
                    pragmas[kv[0]] = kv[1] if len(kv) > 1 else None
        return pragmas

    def _render_query(self, query_t: str, with_qserv: bool, database: str) -> str:
        """Filters the contents of a qserv integration test SQL query file based
        on qserv/mysql mode.

        Qserv integration tests can be annotated to affect query processing:
        "-- withQserv" will be run only on qserv (not when using the reference database)
        "-- noQserv" will be run only on the reference database (not when using qserv)
        "--" (standard sql comments) will be removed.
        "DBTAG_A" (without the quotes) will be replaced with the current database name.

        Parameters
        ----------
        query_t : `str`
            The text of a qserv integration test query file.
        with_qserv : bool
            If `True` then prepare query for qserv, otherwise for mysql.
        database: `str`
            The database name to replace tag DBTAG_A with; "database" (without
            the quotes).

        Returns
        -------
        query : `str`
            The query text.
        """
        qtext = []
        for line in query_t.split("\n"):
            # squeeze/strip spaces
            line = line.strip()
            line = re.sub(" +", " ", line)
            if not line:
                # skip empty lines
                pass
            elif with_qserv and line.startswith("-- withQserv"):
                # strip the "-- withQserv" text
                qtext.append(line[13:])
            elif line.endswith("-- noQserv"):
                if with_qserv:
                    # skip this line
                    pass
                else:
                    # strip the "-- noQserv" text
                    qtext.append(line[:-10])
            elif not line.startswith("--"):
                # append all non-commented lines
                qtext.append(line)
        query = " ".join(qtext)
        if database:
            query = query.format(DBTAG_A=f"{database}.")
        return query


class ITestCase:
    def __init__(
        self,
        case_id: str,
        sourcedir: str,
        outdir: str,
        mysql_connection: str,
        qserv_connection: str,
        skip_numbers: list[str] | None,
    ):
        self.case_id = case_id
        self.queries_dir = os.path.join(sourcedir, "queries")
        self.outdir = outdir
        self.mysql_connection = mysql_connection
        self.qserv_connection = qserv_connection
        self.skip_numbers = skip_numbers or []
        with open(os.path.join(sourcedir, "data/ingest/database.json")) as f:
            self.db_name = json.load(f)["database"]
        if os.path.exists(self.outdir):
            _log.warn("Test output directory (%s) exists, removing it.", self.outdir)
            shutil.rmtree(self.outdir)
        _log.debug(
            "ITestCase queries_dir:%s, outdir=%s, db_name:%s, skip_numbers:%s",
            self.queries_dir,
            self.outdir,
            self.db_name,
            skip_numbers,
        )

    def run(self) -> None:
        """Run the test queries in a test case.

        Test files will be skipped if their file name contains the text "FIXME",
        or if the query number is listed in the integration test yaml's
        skip_numbers for this test case.
        """
        _log.info("Running %s", self.case_id)
        for query_file in os.listdir(self.queries_dir):
            if not query_file.endswith(".sql"):
                _log.info("Skipping query group or query %s", os.path.basename(query_file))
                continue
            if query_file.split("_")[0] in self.skip_numbers:
                _log.info("Skipping 'skip_number' query %s", os.path.basename(query_file))
                continue
            _log.info("Running query %s", os.path.basename(query_file))
            query = ITestQuery(os.path.join(self.queries_dir, query_file), self.outdir)
            query.run_attached(self.mysql_connection, qserv=False, database=self.db_name)
            query.run_attached(self.qserv_connection, qserv=True, database=self.db_name)
            query.run_detached(self.qserv_connection, qserv=True, database=self.db_name)


class ITestCaseHttp:
    def __init__(
        self,
        case_id: str,
        sourcedir: str,
        outdir: str,
        mysql_connection: str,
        http_connection: str,
        user: str,
        password: str,
        skip_numbers: list[str] | None,
    ):
        self.case_id = case_id
        self.queries_dir = os.path.join(sourcedir, "queries")
        self.outdir = outdir
        self.mysql_connection = mysql_connection
        self.http_connection = http_connection
        self.user = user
        self.password = password
        self.skip_numbers = skip_numbers or []
        with open(os.path.join(sourcedir, "data/ingest/database.json")) as f:
            self.db_name = json.load(f)["database"]
        if os.path.exists(self.outdir):
            _log.warn("Test output directory (%s) exists, removing it.", self.outdir)
            shutil.rmtree(self.outdir)
        _log.debug(
            "ITestCaseHttp queries_dir:%s, outdir=%s, db_name:%s, skip_numbers:%s",
            self.queries_dir,
            self.outdir,
            self.db_name,
            skip_numbers,
        )
        # Supress the warning about the self-signed certificate
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def run(self) -> None:
        """Run the test queries in a test case.

        Test files will be skipped if their file name contains the text "FIXME",
        or if the query number is listed in the integration test yaml's
        skip_numbers for this test case.
        """
        _log.info("Running %s", self.case_id)
        for query_file in os.listdir(self.queries_dir):
            if not query_file.endswith(".sql"):
                _log.info("Skipping query group or query %s", os.path.basename(query_file))
                continue
            if query_file.split("_")[0] in self.skip_numbers:
                _log.info("Skipping 'skip_number' query %s", os.path.basename(query_file))
                continue
            _log.info("Running query %s", os.path.basename(query_file))
            query = ITestQueryHttp(os.path.join(self.queries_dir, query_file), self.outdir)
            query.run_attached(self.mysql_connection, database=self.db_name)
            query.run_attached_http(self.http_connection, self.user, self.password, database=self.db_name)
            query.run_detached_http(self.http_connection, self.user, self.password, database=self.db_name)


class ITestCaseResult:
    def __init__(self, case_name: str):
        self.case_name = case_name
        self.diffs: dict[str, list[str]] = {}

    def add_diffs(self, mode: str, diffs: list[str]) -> None:
        """Add a difference in test outputs.

        A diff indicates a test failure.

        Parameters
        ----------
        mode : str
            The name of the mode the test ran in, one of `query_mode_mysql`,
            `query_mode_qserv_attached`, `query_mode_qserv_detached`
        diffs : List[str]
            The list of files that are not the same.
        """
        d = self.diffs.get(mode, [])
        d.extend(diffs)
        self.diffs[mode] = d

    @property
    def passed(self) -> bool:
        """Indicates if the test case passed.

        The test passed if no diffs were generated."""
        return not self.diffs

    def __str__(self) -> str:
        if self.passed:
            ret = f"Test case {self.case_name} passed."
        else:
            ret = f"Test case {self.case_name} FAILED, failing queries: "
            ret += ", ".join(f"{mode}: [{', '.join(queries)}]" for mode, queries in self.diffs.items())
        return ret


class ITestResults:
    def __init__(
        self,
        test_case_results: list[ITestCaseResult],
        ran_tests: bool,
        compared_results: bool,
    ):
        self.test_case_results = test_case_results
        self.ran_tests = ran_tests
        self.compared_results = compared_results

    @property
    def passed(self) -> bool:
        """Check if all test results passed.

        Returns
        -------
        passed : `bool`
            True if tests passed.
        """
        return all([result.passed for result in self.test_case_results])

    def __str__(self) -> str:
        ret = f"{'Ran' if self.ran_tests else 'Did not run'} tests."
        if self.compared_results:
            if self.test_case_results:
                ret += f"\nTests {'passed' if self.passed else 'failed'}."
            else:
                ret += "\nThere were no results to compare."
        for result in self.test_case_results:
            ret += f"\n{result!s}"
        return ret


def run_queries(
    source: str,
    output: str,
    mysql: str,
    qserv: str,
    run_cases: list[str] | None,
    test_cases_data: list[dict[str, Any]],
) -> None:
    """Run queries.

    Parameters
    ----------
    source : `str`
        The folder containing integration test "cases". Each folder in the dir
        should be named "caseNN" and contain a folder called "queries". All the
        files ending in .sql in the queries folder will be run in qserv (sync
        and async) and in mysql.
    output : `str`
        The folder where output datasets can be written.
    mysql : `str`
        The uri to use to connect to the mysql reference database.
    qserv : `str`
        The uri to use to connect to qserv.
    run_cases : `list` [`str`] or `None`
        The test cases to run, or `None` if all cases should be run.
    test_cases_data : `dict`
        Dict of test cases that contain data about the cases to run.
    """

    def get_cases() -> Generator[ITestCase, None, None]:
        """Generator for test cases."""
        # There is a dependency here that the case names in the load.yaml file
        # match the case names (folder names) for each test case. Maybe that's a
        # good thing even, but right now it's not formal, and might want to be
        # formalized.

        for case_data in test_cases_data:
            if not run_cases or case_data["id"] in run_cases:
                yield ITestCase(
                    case_data["id"],
                    os.path.join(source, case_data["id"]),
                    os.path.join(output, case_data["id"]),
                    mysql,
                    qserv,
                    case_data.get("skip_numbers", None),
                )

    for case in get_cases():
        case.run()


def run_queries_http(
    source: str,
    output: str,
    mysql: str,
    http: str,
    user: str,
    password: str,
    run_cases: list[str] | None,
    test_cases_data: list[dict[str, Any]],
) -> None:
    """Run queries.

    Parameters
    ----------
    source : `str`
        The folder containing integration test "cases". Each folder in the dir
        should be named "caseNN" and contain a folder called "queries". All the
        files ending in .sql in the queries folder will be run in qserv (sync
        and async) and in mysql.
    output : `str`
        The folder where output datasets can be written.
    mysql : `str`
        The uri to use to connect to the mysql reference database.
    http : `str`
        The uri to use to connect to the HTPP frontend.
    user : `str`
        The user to use to connect to the HTTP frontend.
    password : `str`
        The password to use to connect to the HTTP frontend.
    run_cases : `list` [`str`] or `None`
        The test cases to run, or `None` if all cases should be run.
    test_cases_data : `dict`
        Dict of test cases that contain data about the cases to run.
    """

    def get_cases() -> Generator[ITestCaseHttp, None, None]:
        """Generator for test cases."""
        # There is a dependency here that the case names in the load.yaml file
        # match the case names (folder names) for each test case. Maybe that's a
        # good thing even, but right now it's not formal, and might want to be
        # formalized.

        for case_data in test_cases_data:
            if not run_cases or case_data["id"] in run_cases:
                yield ITestCaseHttp(
                    case_data["id"],
                    os.path.join(source, case_data["id"]),
                    os.path.join(output, case_data["id"]),
                    mysql,
                    http,
                    user,
                    password,
                    case_data.get("skip_numbers", None),
                )

    for case in get_cases():
        case.run()


def compare_query_results(run_cases: list[str], outputs_dir: str) -> list[ITestCaseResult]:
    """Compare results from runs with different modes.

    Parameters
    ----------
    run_cases : `list` [`str`] or `None`
        The test cases to run, or `None` if all cases should be run.
    outputs_dir : `str`
        The folder where output datasets can be written.

    Returns
    -------
    results : `list` [ `ITestCaseResult` ]
        A list of one result object per test case.
    """
    results = []
    cases = list(run_cases or os.listdir(outputs_dir))
    cases.sort()
    for case in cases:
        _log.debug("Comparing %s", case)
        if not os.path.exists(os.path.join(outputs_dir, case)):
            _log.warn("There are no query results to compare for %s", case)
            continue
        comparisons = (
            (query_mode_mysql, query_mode_qserv_attached),
            (query_mode_mysql, query_mode_qserv_detached),
        )
        result = ITestCaseResult(case)
        for reference, testdir in comparisons:
            r = os.path.join(outputs_dir, case, reference)
            t = os.path.join(outputs_dir, case, testdir)
            cmp = dircmp(r, t)
            diffs = cmp.left_only + cmp.right_only + cmp.diff_files
            if not diffs:
                _log.debug("%s and %s results are identical.", reference, testdir)
            else:
                _log.debug(
                    "%s and %s differ for %s queries: %s",
                    reference,
                    testdir,
                    len(diffs),
                    diffs,
                )
                result.add_diffs(testdir, diffs)
        results.append(result)

    for result in results:
        _log.info(str(result))

    return results


def run_http_ingest(
    http_frontend_uri: str,
    user: str,
    password: str,
    keep_results: bool,
) -> bool:
    """Test ingesting user tables into Qserv and querying the tables.

    Parameters
    ----------
    http_frontend_uri : `str`
        The uri to use to connect to the HTPP frontend.
    user : `str`
        The user to use to connect to the HTTP frontend.
    password : `str`
        The password to use to connect to the HTTP frontend.
    keep_results : `bool`
        If `True` then keep the results of the test, otherwise delete them.
    """

    # Schema, indexes and rows to ingest into the fully-replicated tables.
    schema = [
        {"name": "id", "type": "INT"},
        {"name": "val", "type": "VARCHAR(32)"},
        {"name": "active", "type": "BOOL"},
    ]
    indexes = [
        {
            "index": "idx_id",
            "spec": "UNIQUE",
            "comment": "The unique primary key index.",
            "columns": [{"column": "id", "length": 0, "ascending": 1}],
        },
        {
            "index": "idx_val",
            "spec": "DEFAULT",
            "comment": "The non-unique index on the string values.",
            "columns": [{"column": "val", "length": 32, "ascending": 1}],
        },
    ]
    rows = [
        ["1", "Andy", "1"],
        ["2", "Bob", "0"],
        ["3", "Charlie", "1"],
    ]

    # The database and table names have special symbols "-" "and "$" in the names.
    # These names require quoting in MySQL queries. And they also require special mapping
    # to the underlying file system. See MySQL documentation for more details:
    # https://dev.mysql.com/doc/refman/8.4/en/identifiers.html
    # https://dev.mysql.com/doc/refman/8.4/en/identifier-mapping.html
    # This is test for an ability of Qserv and Replication/Ingest system to properly handle
    # this scenario..
    database = "user_test-db"
    table_json = "json-table"
    table_json_utf8 = "json-table-utf8"
    table_csv = "csv$table"
    table_csv_utf8 = "csv$table-utf8"
    timeout = 30
    charset = "utf8mb4"
    collation = "utf8mb4_uca1400_ai_ci"

    _log.debug("Testing user database: %s", database)

    # Supress the warning about the self-signed certificate
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Run the cleanup step to ensure no such database (and tables in it) exists after
    # prior attempts to run the test.
    try:
        _http_delete_database(http_frontend_uri, user, password, database)
    except Exception as e:
        _log.warning("Failed to delete user database: %s, error: %s", database, e)

    # Create the table and ingest data using the JSON option. Then query the table.
    try:
        _http_ingest_data_json(http_frontend_uri, user, password, database, table_json, schema, indexes, rows)
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s", table_json, database, e
        )
        return False
    try:
        query = f"SELECT `id`,`val`,`active` FROM `{table_json}` ORDER BY `id` ASC"
        _http_query_table(http_frontend_uri, user, password, database, table_json, query, rows)
    except Exception as e:
        _log.error("Failed to query table: %s of user database: %s, error: ", table_json, database, e)
        return False

    # Create the table and ingest data using the JSON option. Then query the table.
    try:
        _http_ingest_data_json(
            http_frontend_uri,
            user,
            password,
            database,
            table_json_utf8,
            schema,
            indexes,
            rows,
            charset,
            collation,
        )
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s",
            table_json_utf8,
            database,
            e,
        )
        return False
    try:
        query = f"SELECT `id`,`val`,`active` FROM `{table_json_utf8}` ORDER BY `id` ASC"
        _http_query_table(http_frontend_uri, user, password, database, table_json_utf8, query, rows)
    except Exception as e:
        _log.error("Failed to query table: %s of user database: %s, error: ", table_json_utf8, database, e)
        return False

    # Create the table and ingest data using the CSV option. Then query the table.
    try:
        _http_ingest_data_csv(
            http_frontend_uri, user, password, database, table_csv, schema, indexes, rows, timeout
        )
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s", table_csv, database, e
        )
        return False
    try:
        query = f"SELECT `id`,`val`,`active` FROM `{table_csv}` ORDER BY `id` ASC"
        _http_query_table(http_frontend_uri, user, password, database, table_csv, query, rows)
    except Exception as e:
        _log.error("Failed to query table: %s of user database: %s, error: ", table_csv, database, e)

    # Create the table and ingest data using the CSV option. Then query the table.
    try:
        _http_ingest_data_csv(
            http_frontend_uri,
            user,
            password,
            database,
            table_csv_utf8,
            schema,
            indexes,
            rows,
            timeout,
            charset,
            collation,
        )
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s",
            table_csv_utf8,
            database,
            e,
        )
        return False
    try:
        query = f"SELECT `id`,`val`,`active` FROM `{table_csv_utf8}` ORDER BY `id` ASC"
        _http_query_table(http_frontend_uri, user, password, database, table_csv_utf8, query, rows)
    except Exception as e:
        _log.error("Failed to query table: %s of user database: %s, error: ", table_csv_utf8, database, e)

    # Cleanup the tables and the database in two separate steps unless the user
    # requested to keep the results.
    if not keep_results:
        for table in [table_json, table_json_utf8, table_csv, table_csv_utf8]:
            try:
                _http_delete_table(http_frontend_uri, user, password, database, table)
            except Exception as e:
                _log.error("Failed to delete table: %s from user database: %s, error: %s", table, database, e)
                return False
        try:
            _http_delete_database(http_frontend_uri, user, password, database)
        except Exception as e:
            _log.error("Failed to delete user database: %s, error: %s", database, e)
            return False

    # Create the database and table whose names deliberately violate the constraint of the REST API
    # to ensure that the error is properly reported. The API limits the combined length
    # of the database and table names to 56 characters. Here we attempt to create
    # such a combination.
    database = "user_test_db_012345678901234567890"  # 30 characters
    table_json = "user_table_0123456789012345"  # 27 characters
    try:
        _http_ingest_data_json(http_frontend_uri, user, password, database, table_json, schema, indexes, rows)
    except FrontEndError as e:
        _log.debug(
            "The attempt to ingest data into table: %s of user database: %s failed as expected, error: %s",
            table_json,
            database,
            e,
        )
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s",
            table_json,
            database,
            e,
        )
        return False
    else:
        _log.error(
            "The attempt to ingest data into table: %s of user database: %s did not fail as expected",
            table_json,
            database,
        )
        return False

    # Create the director table in a separate database and ingest data using the CSV option.
    # Then query the table.
    #
    # IMPORTANT: The director table must be created in a separate database because of
    # a known problem with the current implementation of Qserv's CSS. The CSS does not
    # get updated if there is more than one partitioned table in the database and the
    # table ingests are interleaved with querying the previously ingested tables.
    # In this case, the CSS transient cache gets out of sync with the actual state of
    # the database. Normally, this is not a problem for the large-scale production databases
    # because the cache would get reset by simply restarting Qserv czars.
    schema_dir = [
        {"name": "id", "type": "INT"},
        {"name": "ra", "type": "DOUBLE"},
        {"name": "dec", "type": "DOUBLE"},
        {"name": "val", "type": "VARCHAR(32)"},
        {"name": "active", "type": "BOOL"},
    ]
    indexes_dir = indexes
    rows_dir = [
        ["1", "2.99845583493592", "-34.236453785757455", "Andy", "1"],
        ["2", "23.45678901234567", "1.67890123456789", "Bob", "0"],
        ["3", "255.56789012345678", "56.78901234567890", "Charlie", "1"],
    ]
    expected_rows_dir = rows_dir
    database_dir = "user_test-db-dir"
    table_csv_dir = "csv-director-table"
    is_director = True
    is_child = False
    id_col_name = "id"
    longitude_col_name = "ra"
    latitude_col_name = "dec"

    def validate_result(ingested: list[list[Any]], expected: list[list[Any]]) -> bool:
        if len(ingested) != len(expected):
            return False
        for i in range(len(ingested)):
            if len(ingested[i]) != len(expected[i]):
                return False
            # PK
            if ingested[i][0] != expected[i][0]:
                return False
            # ra
            if not math.isclose(float(ingested[i][1]), float(expected[i][1])):
                return False
            # dec
            if not math.isclose(float(ingested[i][2]), float(expected[i][2])):
                return False
            # val
            if ingested[i][3] != expected[i][3]:
                return False
            # active
            if ingested[i][4] != expected[i][4]:
                return False
        return True

    try:
        _http_ingest_data_csv(
            http_frontend_uri, user, password, database_dir, table_csv_dir, schema_dir, indexes_dir, rows_dir, timeout,
            charset, collation, is_director, is_child, id_col_name, longitude_col_name, latitude_col_name
        )
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s",
            table_csv_dir,
            database_dir,
            e,
        )
        return False
    try:
        query = f"SELECT `id`,`ra`,`dec`,`val`,`active` FROM `{table_csv_dir}` ORDER BY `id` ASC"
        _http_query_table(
            http_frontend_uri,
            user,
            password,
            database_dir,
            table_csv_dir,
            query,
            expected_rows_dir,
            validate_result,
        )
    except Exception as e:
        _log.error(
            "Failed to query table: %s of user database: %s, error: %s", table_csv_dir, database_dir, e
        )

    # Cleanup the tables and the database in two separate steps unless the user
    # requested to keep the results.
    if not keep_results:
        try:
            _http_delete_table(http_frontend_uri, user, password, database_dir, table_csv_dir)
        except Exception as e:
            _log.error(
                "Failed to delete table: %s from user database: %s, error: %s", table_csv_dir, database_dir, e
            )
            return False
        try:
            _http_delete_database(http_frontend_uri, user, password, database_dir)
        except Exception as e:
            _log.error("Failed to delete user database: %s, error: %s", database_dir, e)
            return False

    # A similar test w/o specifying the PK. The frontend should assign a hidden PK column "qserv_id"
    # and poulate it automatically with unique values: 1, 2, 3, ...
    schema_dir = [
        {"name": "ra", "type": "DOUBLE"},
        {"name": "dec", "type": "DOUBLE"},
        {"name": "val", "type": "VARCHAR(32)"},
        {"name": "active", "type": "BOOL"},
    ]
    rows_dir = [
        ["2.99845583493592", "-34.236453785757455", "Andy", "1"],
        ["23.45678901234567", "1.67890123456789", "Bob", "0"],
        ["255.56789012345678", "56.78901234567890", "Charlie", "1"],
    ]
    expected_rows_dir = [
        ["1", "2.99845583493592", "-34.236453785757455", "Andy", "1"],
        ["2", "23.45678901234567", "1.67890123456789", "Bob", "0"],
        ["3", "255.56789012345678", "56.78901234567890", "Charlie", "1"],
    ]
    id_col_name = ""
    database_dir = "user_test-db-dir-no-pk"
    table_csv_dir = "csv-director-table-no-pk"
    try:
        _http_ingest_data_csv(
            http_frontend_uri, user, password, database_dir, table_csv_dir, schema_dir, indexes_dir, rows_dir, timeout,
            charset, collation, is_director, is_child, id_col_name, longitude_col_name, latitude_col_name
        )
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s",
            table_csv_dir,
            database_dir,
            e,
        )
        return False
    try:
        # Query the table expecting the PK column "qserv_id" to be added automatically. Expected result
        # of the query is in expected_rows_dir.
        query = f"SELECT `qserv_id`,`ra`,`dec`,`val`,`active` FROM `{table_csv_dir}` ORDER BY `qserv_id` ASC"
        _http_query_table(
            http_frontend_uri,
            user,
            password,
            database_dir,
            table_csv_dir,
            query,
            expected_rows_dir,
            validate_result,
        )
    except Exception as e:
        _log.error(
            "Failed to query table: %s of user database: %s, error: %s", table_csv_dir, database_dir, e
        )

    # Ingest a child table of the previously ingested director. The child table will be placed into a separate database.
    # The table will be partitioned based on values of the FK pointing to the object identifiers in the director table.
    # Then query the child table.
    schema_dir = [
        {"name": "id", "type": "BIGINT UNSIGNED"},
        {"name": "height", "type": "INT"},
    ]
    rows_dir = [
        ["1", "182"],
        ["2", "175"],
        ["3", "190"],
    ]
    expected_rows_dir = [
        ["1", "Andy", "182"],
        ["2", "Bob", "175"],
        ["3", "Charlie", "190"],
    ]
    indexes_child = [
        {
            "index": "idx_id",
            "spec": "DEFAULT",
            "comment": "The non-unique key index on the id column.",
            "columns": [{"column": "id", "length": 0, "ascending": 1}],
        },
    ]
    id_col_name = "id"
    database_child = "user_test-db-child"
    table_csv_child = "csv-child-table"
    is_director = False
    is_child = True
    longitude_col_name = ""
    latitude_col_name = ""
    try:
        _http_ingest_data_csv(
            http_frontend_uri, user, password, database_child, table_csv_child, schema_dir, indexes_child, rows_dir, timeout,
            charset, collation, is_director, is_child, id_col_name, longitude_col_name, latitude_col_name,
            database_dir, table_csv_dir, "qserv_id"
        )
    except Exception as e:
        _log.error(
            "Failed to ingest data into table: %s of user database: %s, error: %s", table_csv_child, database_child, e
        )
        return False
    try:
        # Query the table expecting the PK column "qserv_id" to be added automatically. Expected result
        # of the query is in expected_rows_dir.
        query = f"SELECT c.id,d.val,c.height FROM `{database_dir}`.`{table_csv_dir}` d JOIN `{database_child}`.`{table_csv_child}` c  ON d.qserv_id=c.id ORDER BY c.id ASC"
        _http_query_table(http_frontend_uri, user, password, database_child, table_csv_child, query, expected_rows_dir)
    except Exception as e:
        _log.error("Failed to query table: %s of user database: %s, error: ", table_csv_child, database_child, e)

    # Cleanup both last 2 tables and 2 databases in two separate steps unless the user
    # requested to keep the results.
    #
    # IMPORTANT: The cleanup must be done in the right order: first delete the child table & the child databases,
    #            then the director table, and only after that delete the director database.
    #            Otherwise, the Replication system's worker will crash. In general this is the minor problem
    #            because the large-scale ingest workflows are aware of the right sequence for managing lifecycles
    #            of databases/tables. The life expectancy of the large (referenced) catalogs is normally longer
    #            than that of the smaller, temporary user tables. The problem will be addressed later
    #            in a separate effort.
    if not keep_results:

        # First, delete the dependent table & database.
        try:
            _http_delete_table(http_frontend_uri, user, password, database_child, table_csv_child)
        except Exception as e:
            _log.error("Failed to delete table: %s from user database: %s, error: %s", table_csv_child, database_child, e)
            return False
        try:
            _http_delete_database(http_frontend_uri, user, password, database_child)
        except Exception as e:
            _log.error("Failed to delete user database: %s, error: %s", database_child, e)
            return False

        # Then delete the director table and database.
        try:
            _http_delete_table(http_frontend_uri, user, password, database_dir, table_csv_dir)
        except Exception as e:
            _log.error(
                "Failed to delete table: %s from user database: %s, error: %s", table_csv_dir, database_dir, e
            )
            return False
        try:
            _http_delete_database(http_frontend_uri, user, password, database_dir)
        except Exception as e:
            _log.error("Failed to delete user database: %s, error: %s", database_dir, e)
            return False

    return True


def _http_delete_database(
    http_frontend_uri: str,
    user: str,
    password: str,
    database: str,
) -> None:
    """Delete an existing user database.

    Parameters
    ----------
    http_frontend_uri : `str`
        The uri to use to connect to the HTPP frontend.
    user : `str`
        The user to use to connect to the HTTP frontend.
    password : `str`
        The password to use to connect to the HTTP frontend.
    database : `str`
        The name of the database to delete.
    """
    _log.debug("Deleting user database: %s", database)
    url = str(urljoin(http_frontend_uri, f"/ingest/database/{database}?version={repl_api_version}"))
    req = requests.delete(url, verify=False, auth=(requests.auth.HTTPBasicAuth(user, password)))
    req.raise_for_status()
    res = req.json()
    if res["success"] == 0:
        error = res["error"]
        raise FrontEndError(f"Failed to delete user database: {database}", error)


def _http_delete_table(
    http_frontend_uri: str,
    user: str,
    password: str,
    database: str,
    table: str,
) -> None:
    """Delete an existing table from the user database.

    Parameters
    ----------
    http_frontend_uri : `str`
        The uri to use to connect to the HTPP frontend.
    user : `str`
        The user to use to connect to the HTTP frontend.
    password : `str`
        The password to use to connect to the HTTP frontend.
    database : `str`
        The name of the database where the table is located.
    table : `str`
        The name of the table to delete.
    """
    _log.debug("Deleting table: %s from user database: %s", table, database)
    url = str(urljoin(http_frontend_uri, f"/ingest/table/{database}/{table}?version={repl_api_version}"))
    req = requests.delete(url, verify=False, auth=(requests.auth.HTTPBasicAuth(user, password)))
    req.raise_for_status()
    res = req.json()
    if res["success"] == 0:
        error = res["error"]
        raise FrontEndError(f"Failed to delete table: {table} from user database: {database}", error)


def _http_ingest_data_json(
    http_frontend_uri: str,
    user: str,
    password: str,
    database: str,
    table: str,
    schema: list[dict[str, str]],
    indexes: list[dict[str, Sequence[Collection[str]]]],
    rows: list[list[Any]],
    charset: str | None = None,
    collation: str | None = None,
) -> None:
    """Ingest data into an existing table of the user database.

    Parameters
    ----------
    http_frontend_uri : `str`
        The uri to use to connect to the HTPP frontend.
    user : `str`
        The user to use to connect to the HTTP frontend.
    password : `str`
        The password to use to connect to the HTTP frontend.
    database : `str`
        The name of the database where the table is located.
    table : `str`
        The name of the table where the data will be ingested.
    schema : `list` [`dict` [`str`, `str`]]
        The schema of the table to be created.
    indexes : `list` [`dict` [`str`, `list` [`list` [`str`]]]]
        The indexes of the table to be created.
    rows : `list` [`list` [`Any`]]
        The rows of data to be ingested into the table.
    charset : `str`, optional
        The character set to use for the table. If not provided, the default
        character set will be used.
    collation : `str`, optional
        The collation to use for the table. If not provided, the default
        collation will be used.
    """
    _log.debug("Ingesting JSON data into table: %s of user database: %s", table, database)
    data = {
        "database": database,
        "table": table,
        "schema": schema,
        "indexes": indexes,
        "rows": rows,
    }
    if charset is not None:
        data["charset_name"] = charset
    if collation is not None:
        data["collation_name"] = collation

    url = str(urljoin(http_frontend_uri, f"/ingest/data?version={repl_api_version}"))
    req = requests.post(url, json=data, verify=False, auth=(requests.auth.HTTPBasicAuth(user, password)))
    req.raise_for_status()
    res = req.json()
    if res["success"] == 0:
        error = res["error"]
        raise FrontEndError(
            f"Failed to create and load the table: {table} in user database {database}", error
        )


def _http_ingest_data_csv(
    http_frontend_uri: str,
    user: str,
    password: str,
    database: str,
    table: str,
    schema: list[dict[str, str]],
    indexes: list[dict[str, Sequence[Collection[str]]]],
    rows: list[list[Any]],
    timeout: int,
    charset: str | None = None,
    collation: str | None = None,
    is_director: bool = False,
    is_child: bool = False,
    id_col_name: str | None = None,
    longitude_col_name: str | None = None,
    latitude_col_name: str | None = None,
    ref_director_database: str | None = None,
    ref_director_table: str | None = None,
    ref_director_id_col_name: str | None = None,
) -> None:
    """Create the table and ingest the data into the table.

    Parameters
    ----------
    http_frontend_uri : `str`
        The uri to use to connect to the HTPP frontend.
    user : `str`
        The user to use to connect to the HTTP frontend.
    password : `str`
        The password to use to connect to the HTTP frontend.
    database : `str`
        The name of the database where the table is located.
    table : `str`
        The name of the table where the data will be ingested.
    schema : `list` [`dict` [`str`, `str`]]
        The schema of the table to be created.
    indexes : `list` [`dict` [`str`, `list` [`list` [`str`]]]]
        The indexes of the table to be created.
    rows : `list` [`list` [`Any`]]
        The rows of data to be ingested into the table.
    timeout : `int`
        The timeout for the ingestion operation in seconds.
    charset : `str`, optional
        The character set to use for the table. If not provided, the default
        character set will be used.
    collation : `str`, optional
        The collation to use for the table. If not provided, the default
        collation will be used.
    is_director : `bool`, optional
        If `True` then the table is a director table.
    is_child : `bool`, optional
        If `True` then the table is a child table.
    id_col_name : `str`, optional
        The name of the column to use as the director id column. Required if
        `is_director` is `True`.
    longitude_col_name : `str`, optional
        The name of the column to use as the director longitude column. Required
        if `is_director` is `True`.
    latitude_col_name : `str`, optional
        The name of the column to use as the director latitude column. Required
        if `is_director` is `True`.
    ref_director_database: `str`, optional
        The name of the database where the director table is located. Required if
        `is_child` is `True`.
    ref_director_table: `str`, optional
        The name of the director table. Required if `is_child` is `True`.
    ref_director_id_col_name: `str`, optional
        The name of the column to use as the director id column in the director table. Required if `is_child` is `True`.
    """
    _log.debug("Ingesting CSV data into table: %s of user database: %s", table, database)
    base_dir = "/tmp"
    schema_file = "schema.json"
    schema_file_path = os.path.join(base_dir, schema_file)
    indexes_file = "indexes.json"
    indexes_file_path = os.path.join(base_dir, indexes_file)
    rows_file = "rows.csv"
    rows_file_path = os.path.join(base_dir, rows_file)

    with open(schema_file_path, "w") as f:
        json.dump(schema, f)
    with open(indexes_file_path, "w") as f:
        json.dump(indexes, f)
    with open(rows_file_path, "w") as f:
        csv_writer = csv.writer(f)
        for row in rows:
            csv_writer.writerow(row)

    if is_director:
        encoder = MultipartEncoder(
            fields={
                "database": (None, database),
                "table": (None, table),
                "is_partitioned": (None, "1"),
                "is_director": (None, "1"),
                "is_child": (None, "0"),
                "id_col_name": (None, id_col_name),
                "longitude_col_name": (None, longitude_col_name),
                "latitude_col_name": (None, latitude_col_name),
                "fields_terminated_by": (None, ","),
                "timeout": (None, str(timeout)),
                "schema": (schema_file, open(schema_file_path, "rb"), "application/json"),
                "indexes": (indexes_file, open(indexes_file_path, "rb"), "application/json"),
                "rows": (rows_file, open(rows_file_path, "rb"), "text/csv"),
            }
        )
    elif is_child:
        encoder = MultipartEncoder(
            fields={
                "database": (None, database),
                "table": (None, table),
                "is_partitioned": (None, "1"),
                "is_director": (None, "0"),
                "is_child": (None, "1"),
                "id_col_name": (None, id_col_name),
                "ref_director_database": (None, ref_director_database),
                "ref_director_table": (None, ref_director_table),
                "ref_director_id_col_name": (None, ref_director_id_col_name),
                "fields_terminated_by": (None, ","),
                "timeout": (None, str(timeout)),
                "schema": (schema_file, open(schema_file_path, "rb"), "application/json"),
                "indexes": (indexes_file, open(indexes_file_path, "rb"), "application/json"),
                "rows": (rows_file, open(rows_file_path, "rb"), "text/csv"),
            }
        )
    else:
        encoder = MultipartEncoder(
            fields={
                "database": (None, database),
                "table": (None, table),
                "fields_terminated_by": (None, ","),
                "timeout": (None, str(timeout)),
                "schema": (schema_file, open(schema_file_path, "rb"), "application/json"),
                "indexes": (indexes_file, open(indexes_file_path, "rb"), "application/json"),
                "rows": (rows_file, open(rows_file_path, "rb"), "text/csv"),
            }
        )
    url = str(urljoin(http_frontend_uri, f"/ingest/csv?version={repl_api_version}"))
    req = requests.post(
        url,
        data=encoder,
        headers={"Content-Type": encoder.content_type},
        verify=False,
        auth=(requests.auth.HTTPBasicAuth(user, password)),
    )
    req.raise_for_status()
    res = req.json()
    if res["success"] == 0:
        error = res["error"]
        raise FrontEndError(
            f"Failed to create and load the table: {table} in user database {database}", error
        )


def _http_query_table(
    http_frontend_uri: str,
    user: str,
    password: str,
    database: str,
    table: str,
    query: str,
    expected_rows: list[list[Any]],
    validate_result: Callable[[list[list[Any]], list[list[Any]]], bool] = lambda ingested, expected: (
        ingested == expected
    ),
) -> None:
    """Query an existing table of the user database.

    Parameters
    ----------
    http_frontend_uri : `str`
        The uri to use to connect to the HTPP frontend.
    user : `str`
        The user to use to connect to the HTTP frontend.
    password : `str`
        The password to use to connect to the HTTP frontend.
    database : `str`
        The name of the database where the table is located.
    table : `str`
        The name of the table that will be queried.
    query: `str`
        The query to execute on the table.
    expected_rows : `list` [`list` [`Any`]]
        The expected data in the table
    validate_result : `Callable[[list[list[Any]], list[list[Any]]], bool]`
        A function to validate the received rows against the expected rows.
    """
    _log.debug("Querying table: %s of user database: %s", table, database)
    data = {
        "database": database,
        "query": query,
    }
    url = str(urljoin(http_frontend_uri, f"/query?version={repl_api_version}"))
    req = requests.post(url, json=data, verify=False, auth=(requests.auth.HTTPBasicAuth(user, password)))
    req.raise_for_status()
    res = req.json()
    if res["success"] == 0:
        error = res["error"]
        raise FrontEndError(f"Failed to query the table: {table} in user database: {database}", error)
    ingested_rows = res["rows"]
    if not validate_result(ingested_rows, expected_rows):
        raise RuntimeError(
            f"Query result mismatch for table: {table} in user database: {database}, "
            f"expected: {expected_rows}, got: {ingested_rows}"
        )
