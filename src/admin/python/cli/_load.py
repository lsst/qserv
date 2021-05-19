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

import click
import json
import os
import pathlib
from requests import delete, get, post, put
import subprocess
import sys

headers = {'Content-Type': 'application/json'}

host = "repl-mgr-controller"
port = 25081

def _check(result, url):
    if result["success"] != 1:
        raise RuntimeError(f"Command {url} failed: {result}")


def version():
    url = f"http://{host}:{port}/meta/version"
    res = get(url, headers=headers)
    res = res.json()
    _check(res, url)
    return res['version']


def _post(url, data):
    res = post(url, headers=headers, data=data)
    res = res.json()
    print(f"post\nurl:{url}\ndata:{data}\nresult:{res}")
    _check(res, url)
    return res


def _delete(url, data):
    res = delete(url, headers=headers, data=data)
    res = res.json()
    print(f"delete\nurl:{url}\ndata:{data}\nresult:{res}")
    _check(res, url)


def _put(url, data):
    res = put(url, headers=headers, data=data)
    res = res.json()
    print(f"put\nurl:{url}\ndata:{data}\nresult:{res}")
    _check(res, url)


def ingest_database():
    data = json.dumps(
        {
            "database":"test101",
            "num_stripes":340,
            "num_sub_stripes":3,
            "overlap":0.01667,
            "auto_build_secondary_index":1,
            "local_load_secondary_index": 1,
            "auth_key":""
        }
    )
    _post(url=f"http://{host}:{port}/ingest/database", data=data)


def ingest_table():
    data = json.dumps(
        {
            "database":"test101",
            "table":"Object",
            "is_partitioned":1,
            "chunk_id_key":"chunkId",
            "sub_chunk_id_key":"subChunkId",
            "is_director":1,
            "director_key":"objectId",
            "latitude_key":"dec",
            "longitude_key":"ra",
            "schema":[
            {"name":"objectId","type":"BIGINT NOT NULL"},
            {"name":"ra","type":"DOUBLE NOT NULL"},
            {"name":"dec","type":"DOUBLE NOT NULL"},
            {"name":"property","type":"DOUBLE"},
            {"name":"chunkId","type":"INT UNSIGNED NOT NULL"},
            {"name":"subChunkId","type":"INT UNSIGNED NOT NULL"}
            ],
            "auth_key":""
        }
    )
    _post(url=f"http://{host}:{port}/ingest/table", data=data)


def ingest_chunk(transaction_id):
    res = _post(
        url=f"http://{host}:{port}/ingest/chunk",
        data=json.dumps(dict(transaction_id=transaction_id, chunk=0, auth_key="")),
    )
    return res["location"]["host"], res["location"]["port"]


def ingest_file(transaction_id, worker_host, worker_port):
    chunk_file = os.path.join(pathlib.Path(__file__).parent.absolute(), "chunk_0.txt")
    args = ["qserv-replica-file", "INGEST", "FILE", worker_host, str(worker_port), str(transaction_id), "Object", "P", chunk_file, "--verbose"]
    print(f"ingest file args:{args}")
    res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    res.check_returncode()
    # return [i for i in result.stdout.decode("utf-8").strip().split("\n") if i]
    print(f"ingest file res:{res}")


def start_transaction():
    """Start a transaction and return the transaction id.
    """
    database = "test101"
    res = _post(
        url=f"http://{host}:{port}/ingest/trans",
        data=json.dumps(dict(database=database, auth_key=""))
    )
    return res["databases"][database]["transactions"][0]["id"]


def commit_transaction(transaction_id):
    _put(
        url=f"http://{host}:{port}/ingest/trans/{transaction_id}?abort=0",
        data=json.dumps(dict(auth_key="")),
    )


def finish_ingest_database():
    _put(
        url=f"http://{host}:{port}/ingest/database/test101",
        data=json.dumps(dict(auth_key="")),
    )


def delete_database():
    _delete(
        url=f"http://{host}:{port}/ingest/database/test101?delete_secondary_index=1",
        data=json.dumps(dict(auth_key=""))
    )


def load(delete):
    if delete:
        delete_database()
    else:
        ingest_database()
        ingest_table()
        transaction_id = start_transaction()
        worker_host, worker_port = ingest_chunk(transaction_id)
        ingest_file(transaction_id, worker_host, worker_port)
        commit_transaction(transaction_id)
        finish_ingest_database()
