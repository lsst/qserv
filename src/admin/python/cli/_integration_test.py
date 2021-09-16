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
import logging
import mysql.connector

# MySQLInterfaceError can get thrown, we need to catch it.
# It's not exposed as a public python object but *is* used in mysql.connector unit tests.
from _mysql_connector import MySQLInterfaceError
import os
import requests
import shutil
import subprocess
from typing import List, Optional
from urllib.parse import urlparse
import yaml

from .. import itest_load
from ..qserv_backoff import max_backoff_sec, on_backoff
from ..replicationInterface import ReplicationInterface
from ..template import save_template_cfg
from .. import itest


_log = logging.getLogger(__name__)


def _pull_testdata(destination: str, qserv_testdata_repo: str) -> None:
    """Run `git pull` from repo, into the given destination.

    Parameters
    ----------
    destination : `str`
        Absolute path to the location to put the qserv_testdata repository.
    qserv_testdata_repo : `str`
        The url where a tar of the testdata repo can be downloaded using
        `requests.get`.
    """
    # Remove the existing testdata files if they exist
    if os.path.exists(destination):
        _log.warn("Removing existing qserv_testdata files and pulling new ones.")
        shutil.rmtree(destination)
    os.makedirs(destination)
    tar_filename = os.path.join(destination, "master.tar")

    # Get a tarball of the testdata repo
    response = requests.get(qserv_testdata_repo, stream=True)
    if response.status_code != 200:
        raise RuntimeError(f"Failed getting qserv_testdata tarball, status code {response.status_code}")
    with open(tar_filename, mode="wb") as f:
        shutil.copyfileobj(response.raw, f)

    # Extract the tarball
    args = ["tar", "xpvf", "master.tar", "--strip-components", "1"]
    result = subprocess.run(args, cwd=destination, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result.check_returncode()

    # Remove the tarball
    os.remove(tar_filename)


@backoff.on_exception(
    exception=requests.exceptions.ConnectionError,
    wait_gen=backoff.expo,
    on_backoff=on_backoff(log=_log),
    max_time=max_backoff_sec,
)
def wait_for_replication_system(repl_ctrl_uri: str) -> None:
    """Wait for the replication system to be ready, indicated by successfully
    getting a version from it.

    Parameters
    ----------
    repl_ctrl_uri : str
        The uri to the replication controller.
    """
    ReplicationInterface(repl_ctrl_uri).version()


class CzarNotReady(RuntimeError):
    pass


@backoff.on_exception(
    exception=(CzarNotReady, mysql.connector.errors.DatabaseError, MySQLInterfaceError),
    wait_gen=backoff.expo,
    on_backoff=on_backoff(log=_log),
    max_time=max_backoff_sec,
)
def wait_for_czar(czar_db_uri: str) -> None:
    """Wait for the czar to be ready.

    Parameters
    ----------
    czar_db_uri : str
        The uri to the czar database.

    Raises
    ------
    CzarNotReady
        If the czar is not ready. This is handled by the backoff decorator until
        a max wait time is exceeded, and then the excetpion will not be caught,
        and can be handled above or test execution can be aborted.
    """
    parsed = urlparse(czar_db_uri)

    def get_databases() -> List[str]:
        """Get the names of the databases in the czar db."""
        with closing(
            mysql.connector.connect(
                user=parsed.username,
                password=parsed.password,
                host=parsed.hostname,
                port=parsed.port,
            )
        ) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute("show databases;")
                return [row[0] for row in cursor.fetchall()]

    databases = get_databases()
    checkdb = "qservMeta"
    if checkdb in databases:
        return
    raise CzarNotReady(f"{checkdb} not in existing databases: {databases}")


def run_integration_tests(
    pull: Optional[bool],
    unload: bool,
    load: Optional[bool],
    reload: bool,
    cases: List[str],
    run_tests: bool,
    tests_yaml: str,
    compare_results: bool,
    mysqld_user: str,
) -> itest.ITestResults:
    """Top level script to run the integration tests.

    Parameters
    ----------
    pull : `bool` or `None`
        True if a new copy of qserv_testdata should be downloaded. If a local
        copy of qserv_testdata already exists it will be removed first.
    unload : `bool`
        Remove test databases from the databases.
    load : `bool` or `None`
        True if the database should be loaded, False if not. If `None`, and
        unload == True then will not load the database, otherwise if `None` will
        load the database if it is not yet loaded into qserv (assumes the ref
        database matches the qserv database.)
    reload : `bool`
        Remove test databases and re-add them.
    cases : `list` [`str`]
        Run (and load/reload data if those flags are set) these test cases only.
    run_tests : `bool`
        True if the tests should be run. (False can be used to compare
        previously generated test outputs.)
    tests_yaml : `str`
        Path to the yaml file that contains the details about running the
        tests. The files will be merged, higher index files get priority.
    compare_resutls : `bool`
        If True run query output comparison, if False do not.
    mysqld_user : `str`
        The name of the qserv user.

    Returns
    -------
    success : `bool`
        `True` if all query outputs were the same, otherwise `False`.

    Raises
    ------
    RuntimeError
        If qserv_testdata sources have not been downloaded and `pull` was
        `False`.
    """
    save_template_cfg(
        dict(
            mysqld_user_qserv=mysqld_user,
        )
    )

    with open(tests_yaml) as f:
        tests_data = yaml.safe_load(f.read())

    qserv_testdata_dir = tests_data["qserv-testdata-dir"]
    qserv_testdata_repo = tests_data["qserv-testdata-repo"]
    qserv_testdata_test_case_dir = tests_data["qserv-testdata-test-case-dir"]
    testdata_output = tests_data["testdata-output"]

    if pull or (pull is None and not os.path.exists(qserv_testdata_dir)):
        _pull_testdata(qserv_testdata_dir, qserv_testdata_repo)
    elif not os.path.exists(qserv_testdata_dir):
        raise RuntimeError("qserv_testdata sources have not been downloaded. Use --pull to get them.")

    wait_for_czar(tests_data["czar-db-admin-uri"])
    wait_for_replication_system(tests_data["replication-controller-uri"])

    # If unload is True, and load is not specified (default is `None`, means
    # "load if not loaded") then change load to False; do not reload.
    if unload and load is None:
        load = False

    if unload or reload:
        itest_load.remove(
            repl_ctrl_uri=tests_data["replication-controller-uri"],
            ref_db_uri=tests_data["reference-db-uri"],
            test_cases_data=tests_data["test_cases"],
            ref_db_admin=tests_data["reference-db-admin-uri"],
            cases=cases,
        )

    if load != False or reload:
        itest_load.load(
            repl_ctrl_uri=tests_data["replication-controller-uri"],
            ref_db_uri=tests_data["reference-db-uri"],
            test_cases_data=tests_data["test_cases"],
            ref_db_admin=tests_data["reference-db-admin-uri"],
            cases=cases,
            load=load,
        )

    if os.path.exists(testdata_output):
        _log.info(f"Removing previous test outputs from {testdata_output}.")
        shutil.rmtree(testdata_output)

    if run_tests:
        itest.run_queries(
            source=os.path.join(qserv_testdata_dir, qserv_testdata_test_case_dir),
            output=testdata_output,
            mysql=tests_data["reference-db-uri"],
            qserv=tests_data["qserv-uri"],
            run_cases=cases,
            test_cases_data=tests_data["test_cases"],
        )

    if compare_results:
        test_case_results = itest.compareQueryResults(run_cases=cases, outputs_dir=testdata_output)
    else:
        test_case_results = []

    return itest.ITestResults(
        test_case_results=test_case_results,
        ran_tests=run_tests,
        compared_results=compare_results,
    )
