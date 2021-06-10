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


import logging
import os
import requests
import shutil
import subprocess
import yaml

from . import _load
from .. import itest


qserv_testdata_repo = "https://github.com/lsst/qserv_testdata/tarball/master"
qserv_testdata_dir = "/qserv/data/qserv_testdata"
qserv_testdata_test_case_dir = "datasets"
testdata_output = "/qserv/data/integration_test/"


# The test data is loaded according to a yaml file, it's currently installed in
# the lite-qserv container next to the other cli files. We might/probably want
# to find a more formal place to put it, and/or allow the caller to pass it in.
testdata_load_yaml = os.path.join(os.path.dirname(__file__), "load.yaml")

_log = logging.getLogger(__name__)


def _pull_testdata(destination):
    """Run `git pull` from repo, into the given destination.

    Parameters
    ----------
    dest : [type]
        [description]
    """
    # Remove the existing testdat files if they exist
    if os.path.exists(qserv_testdata_dir):
        _log.warn("Removing existing qserv_testdata files and pulling new ones.")
        shutil.rmtree(qserv_testdata_dir)
    os.makedirs(qserv_testdata_dir)
    tar_filename = os.path.join(qserv_testdata_dir, "master.tar")

    # Get a tarball of the testdata repo
    response = requests.get(qserv_testdata_repo, stream=True)
    if response.status_code != 200:
        raise RuntimeError(f"Failed getting qserv_testdata tarball, status code {response.status_code}")
    with open(tar_filename, mode="wb") as f:
        f.write(response.raw.read())

    # Extract the tarball
    args = ["tar", "xpvf", "master.tar", "--strip-components", "1"]
    result = subprocess.run(args, cwd=qserv_testdata_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result.check_returncode()

    # Remove the tarball
    os.remove(tar_filename)


def run_integration_tests(pull, unload, load, reload, cases, run_tests, tests_yaml, compare_results):
    """Top level script to run the integration tests.

    Parameters
    ----------
    pull : `bool`
        True if a new copy of qserv_testdata should be downloaded. If a local
        copy of qserv_testdata already exists it will be removed first.
    load : `bool`
        Load data from the local copy of qserv_testdata into the databases.
    unload : `bool`
        Remove test databases from the databases.
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

    Returns
    -------
    success : `bool`
        `True` if all query outputs were the same, otherwise `False`.

    Raises
    ------
    RuntimeError
        [description]
    """
    if pull:
        if not os.path.exists(os.path.join(qserv_testdata_dir, "qserv_testdata")):
            _pull_testdata(qserv_testdata_dir)
    elif not os.path.exists(qserv_testdata_dir):
        raise RuntimeError("qserv_testdata sources have not been downloaded. Use --pull to get them.")

    with open(tests_yaml) as f:
        tests_data = yaml.safe_load(f.read())

    if unload or reload:
        _load.remove(
            repl_ctrl_uri=tests_data["replication-controller-uri"],
            ref_db_uri=tests_data["reference-db-uri"],
            test_cases_data=tests_data["test_cases"],
            ref_db_admin=tests_data["reference-db-admin-uri"],
            cases=cases,
        )
    if load or reload:
        _load.load(
            repl_ctrl_uri=tests_data["replication-controller-uri"],
            ref_db_uri=tests_data["reference-db-uri"],
            test_cases_data=tests_data["test_cases"],
            ref_db_admin=tests_data["reference-db-admin-uri"],
            cases=cases,
        )


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
        return itest.compareQueryResults(run_cases=cases, outputs_dir=testdata_output)
    return None
