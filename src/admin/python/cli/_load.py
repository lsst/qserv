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

from collections import namedtuple
from contextlib import closing
import gzip
import json
import logging
import mysql.connector
import os
from pathlib import Path
from requests import delete, get, post, put
import shutil
import subprocess
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
import yaml


chunk_file_t = "chunk_{chunk_id}.txt"
chunk_overlap_file_t = "chunk_{chunk_id}_overlap.txt"

headers = {'Content-Type': 'application/json'}

mysqld_user_qserv = "qsmaster"

qserv_data_dir = "/qserv/data"

chunk_info_file = "chunk_info.json"

_log = logging.getLogger(__name__)


def _check(result, url, check=None):
    """Check the result of an http command and raise a RuntimeError if it was
    not successful.

    Parameters
    ----------
    result : `dict`
        The dict of the result json returned by the ingest system.
    url : `str`
        The url that returned the result.
    check : `func` (result, url) -> bool
        Function that takes the same result and url as this function
        and returns a bool; True if the check completed and False if
        this fucntion shoudl still run its check algorithm.
    """
    checked = check(result, url) if check else False
    if checked:
        return
    if result["success"] != 1:
        raise RuntimeError(f"Command {url} failed: {result}")


def _check_returncode(result):
    """Check the result of a subprocess execution and raise a RuntimeError if it
    was not successful.
    """
    if result.returncode != 0:
        raise RuntimeError(
            f"Subprocess failed ({result.returncode} stdout: {result.stdout} stderr: {result.stderr}"
        )


def _post(url, data):
    res = post(url, headers=headers, data=data)
    res = res.json()
    _check(res, url)
    return res


def _delete(url, data, check):
    res = delete(url, headers=headers, data=data)
    res = res.json()
    _check(res, url, check)
    return res

def _put(url, data):
    res = put(url, headers=headers, data=data)
    res = res.json()
    _check(res, url)
    return res


def _get(url, data):
    res = get(url, headers=headers, data=data)
    res = res.json()
    _check(res, url)
    return res


# TODO needs repl_ctrl_uri
# def version():
#     url = f"http://{host}:{port}/meta/version"
#     res = _get(url, headers=headers)
#     return res['version']


def _ingest_database(database_json, repl_ctrl):
    _log.debug(f"_ingest_database json:{database_json}")
    _post(url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/database", data=database_json)


def _ingest_table_config(table_json, repl_ctrl):
    _log.debug(f"_ingest_table_config:{table_json}")
    _post(url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/table", data=table_json)


def _start_transaction(database, repl_ctrl):
    """Start a transaction and return the transaction id.
    """
    _log.debug(f"_start_transaction database:{database}")
    res = _post(
        url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/trans",
        data=json.dumps(dict(database=database, auth_key=""))
    )
    return res["databases"][database]["transactions"][0]["id"]


def _commit_transaction(transaction_id, repl_ctrl):
    _log.debug(f"_commit_transaction transaction_id:{transaction_id}")
    res = _put(
        url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/trans/{transaction_id}?abort=0",
        data=json.dumps(dict(auth_key="")),
    )


def _ingest_chunk_config(transaction_id, chunk_id, repl_ctrl):
    """Get the location where a given chunk id should be ingested.

    Returns
    -------
    location : `tuple`
        Returns a two-item tuple, the first element is the host name and the
        second name is the port
    repl_ctrl :
        TODO
    """
    res = _post(
        url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/chunk",
        data=json.dumps(dict(transaction_id=transaction_id, chunk=chunk_id, auth_key="")),
    )
    return res["location"]["host"], res["location"]["port"]


def _ingest_chunk_configs(transaction_id, chunk_ids, repl_ctrl):
    """Get the locations where a list of chunk ids should be ingested.

    Parameters
    ----------
    transaction_id : `int`
        The transaction id.
    chunk_ids : `list` [`int`]

    Returns
    -------
    locations : `list` : `ChunkLocation`
        A list of locations, `ChunkLocation` is a namedtuple with the parameters
        `chunk_id`, `host`, `port`.
    chunk_ids :
        TODO
    repl_ctrl :
        The parsed repl_ctrl_uri.
    """
    res = _post(
        url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/chunks",
        data=json.dumps(dict(transaction_id=transaction_id, chunks=chunk_ids, auth_key="")),
    )
    ChunkLocation = namedtuple("ChunkLocation", "chunk_id host port")
    return [ChunkLocation(l["chunk"], l["host"], l["port"]) for l in res["location"]]


def _ingest_regular_table(transaction_id, repl_ctrl):
    """Get the locations where a non-chunk table should be ingested.

    Returns
    -------
    locations : `list` : `RegularLocation`
        A list of locations, `RegularLocation` is a namedtuple with the parameters
        `host`, `port`.
    repl_ctrl:
        TODO
    """
    _log.debug(f"_ingest_regular_table transaction_id:{transaction_id}")
    res = _get(
        url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/regular",
        data=json.dumps(dict(auth_key="", transaction_id=transaction_id))
    )
    RegularLocation = namedtuple("RegularLocation", "host port")
    return [RegularLocation(location["host"], location["port"]) for location in res["locations"]]


def _ingest_data_file(transaction_id, worker_host, worker_port, data_file, table, partitioned=False):
    """Ingest table data from a file.

    Parameters
    ----------
    transaction_id : `int`
        The transaction id.
    worker_host : `str`
        The name of the host ingesting the data.
    worker_port : `int`
        The worker_host port to use.
    data_file : `str`
        The path to the data file to ingest.
    table : `str`
        The name of the database to ingest the data into.
    partitioned : bool, optional
        True if the data is partitioned into chunks, by default False
    """
    args = [
        "qserv-replica-file",
        "INGEST",
        "FILE",
        worker_host,
        str(worker_port),
        str(transaction_id),
        table,
        "P" if partitioned else "R", # app help says P for 'partitioned' and R for 'regular'/non-partitioned.
        data_file,
        "--verbose",
        f"--columns-separator={'TAB' if data_file.endswith('tsv') else 'COMMA'}",
    ]
    _log.debug(f"ingest file args:{args}")
    res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _check_returncode(res)
    # return [i for i in result.stdout.decode("utf-8").strip().split("\n") if i]
    _log.debug(f"ingest file res:{res}")


def _publish_database(database, repl_ctrl):
    _log.debug(f"_publish_database database:{database}")
    _put(
        url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/database/{database}",
        data=json.dumps(dict(auth_key="")),
    )


def delete_database(database, admin, repl_ctrl):
    for db in database:
        data = dict(admin_auth_key="") if admin else dict(auth_key="")
        _log.debug(f"delete_database database:{database}, data:{data}")
        def warn_if_not_exist(res, url):
            if not res["success"]:
                if "no such database" in res["error"]:
                    _log.warn(f"Can not delete database {database}; it does not exist.")
                    return True
            return False
        _delete(
            url=f"http://{repl_ctrl.hostname}:{repl_ctrl.port}/ingest/database/{db}?delete_secondary_index=1",
            data=json.dumps(data),
            check=warn_if_not_exist,
        )


# TODO needs repl_ctrl_uri
def load_simple():
    raise NotImplementedError("Needs to be refactored to use repl_ctl_uri from a yaml or CLI option.")
#     _ingest_database(
#         database_json=json.dumps({
#             "database":"test101",
#             "num_stripes":340,
#             "num_sub_stripes":3,
#             "overlap":0.01667,
#             "auto_build_secondary_index":1,
#             "local_load_secondary_index": 1,
#             "auth_key":""
#         })
#     )
#     _ingest_table_config(
#         table_json=json.dumps({
#             "database":"test101",
#             "table":"Object",
#             "is_partitioned":1,
#             "chunk_id_key":"chunkId",
#             "sub_chunk_id_key":"subChunkId",
#             "is_director":1,
#             "director_key":"objectId",
#             "latitude_key":"dec",
#             "longitude_key":"ra",
#             "schema":[
#                 {"name":"objectId","type":"BIGINT NOT NULL"},
#                 {"name":"ra","type":"DOUBLE NOT NULL"},
#                 {"name":"dec","type":"DOUBLE NOT NULL"},
#                 {"name":"property","type":"DOUBLE"},
#                 {"name":"chunkId","type":"INT UNSIGNED NOT NULL"},
#                 {"name":"subChunkId","type":"INT UNSIGNED NOT NULL"}
#             ],
#             "auth_key":""
#         })
#     )
#     transaction_id = _start_transaction(database="test101")
#     worker_host, worker_port = _ingest_chunk_config(transaction_id, 0)
#     _ingest_data_file(
#         transaction_id,
#         worker_host,
#         worker_port,
#         data_file=os.path.join(Path(__file__).parent.absolute(), "chunk_0.txt"),
#         table="Object",
#         partitioned=True,
#     )
#     _commit_transaction(transaction_id)
#     _publish_database("test101")


def _ingest_chunks_data(transaction_id, table_name, chunks_folder, repl_ctrl):
    """Ingest prepared information about chunks.

    Parameters
    ----------
    transaction_id : `int`
        The transaction id obtained by starting a transaction
    table_name : `str`
        The name of the table to ingest into.
    chunks_folder : `str`
        The absolute path to the folder that contains the chunk info file that
        will be ingested.
    repl_ctrl : `tuple`
        The result of calling urlparse with the repl_ctrl_uri.
    """
    _log.debug(f"_ingest_chunks_data transaction_id:{transaction_id} table_name:{table_name} chunks_folder:{chunks_folder}")
    # Ingest the chunk configs & get the host+port location for each chunk
    with open(os.path.join(chunks_folder, chunk_info_file)) as f:
        chunk_info = json.load(f)

    # Create locations for the chunk configs (repl system calls this "ingest" chunk)
    locations = _ingest_chunk_configs(transaction_id, [chunk["id"] for chunk in chunk_info["chunks"]], repl_ctrl)

    # Ingest the chunk files:
    def generate_locations():
        for location in locations:
            for chunk_file in (chunk_file_t, chunk_overlap_file_t):
                full_path = os.path.join(chunks_folder, chunk_file.format(chunk_id=location.chunk_id))
                if os.path.exists(full_path):
                    _log.debug(f"Ingesting {full_path} to {location.host}:{location.port}")
                    yield full_path, location.host, location.port
                else:
                    _log.warn(f"Not ingesting {full_path}; it does not exist (probably there is no data for that chunk).")

    for _file, host, port in generate_locations():
        _ingest_data_file(
            transaction_id,
            host,
            port,
            data_file=_file,
            table=table_name,
            partitioned=True,
        )


def _ingest_table_data(transaction_id, table_name, data_file, repl_ctrl):
    """Ingest data for a non-partitioned table.

    Parameters
    ----------
    transaction_id : `int`
        The trasaction id obtained by starting a transaction.
    table_name : `str`
        The name of the table to ingest data to
    data_file : `str`
        The absolute path to the file containing the table data.
    repl_ctrl :
        TODO
    """
    _log.debug(f"_ingest_table_data: transaction_id:{transaction_id} table_name:{table_name} data_file:{data_file}")
    locations = _ingest_regular_table(transaction_id, repl_ctrl)
    for location in locations:
        _log.debug(f"Ingesting {data_file} to {location.host}:{location.port} table {table_name}.")
        _ingest_data_file(
            transaction_id,
            location.host,
            location.port,
            data_file=data_file,
            table=table_name,
            partitioned=False,
        )


def _mysql(uri, local_infile=False):
    """Create a mysql.connection that is connected to a database.

    Parameters
    ----------
    uri : `str`, optional
        The URI of the database to connnect to.
    local_infile : bool, optional
        Passed to the allow_local_infile parameter of the connector, by default False

    Returns
    -------
    connection : `mysql.connection`
        The connected connection object.
    """
    if uri:
        parsed = urlparse(uri)
        hostname = parsed.hostname
        port = parsed.port
        user = parsed.username
        pw = parsed.password
    _log.debug(f"_mysql hostname:{hostname}, port:{port}, user:{user}")
    cnx = mysql.connector.connect(
        user=user,
        password=pw,
        host=hostname,
        port=port,
        allow_local_infile=local_infile
    )
    return cnx


def _create_ref_db(ref_db_admin, name):
    """Create a database in the mysql used for integration test reference.

    Parameters
    ----------
    ref_db_admin : `str`
        URI to the reference db for the admin user.
    name : `str`
        The name of the database to create.
    """
    cnx = _mysql(uri=ref_db_admin)
    _log.debug(f"_create_ref_db name:{name}")
    stmt = f"""CREATE USER IF NOT EXISTS '{mysqld_user_qserv}'@'localhost';
    CREATE USER IF NOT EXISTS '{mysqld_user_qserv}'@'%';
    CREATE DATABASE IF NOT EXISTS {name};
    GRANT ALL ON {name}.* TO '{mysqld_user_qserv}'@'localhost';
    GRANT ALL ON {name}.* TO '{mysqld_user_qserv}'@'%';
    FLUSH PRIVILEGES;"""
    with closing(cnx.cursor()) as cursor:
        res = cursor.execute(stmt, multi=True)
    cnx.close()


def _create_ref_table(uri, db, schema_file):
    cnx = _mysql(uri)
    _log.debug(f"_create_ref_table schema_file:{schema_file}")
    if not cnx.is_connected():
        cnx.connect()
    cursor = cnx.cursor()
    cursor.execute(f"USE {db}")
    with open(schema_file) as f:
        cursor.execute(f.read(), multi=True)
    cursor.close()
    cnx.close()


def _load_ref_data(uri, data_file, db, table, field_sep):
    """Load database data into the reference database.

    Parameters
    ----------
    uri : `str`
        The URI to the reference database for a user that has permission to load
        data into the table.
    data_file : [type]
        [description]
    db : [type]
        [description]
    table : [type]
        [description]
    field_sep : [type]
        [description]
    """
    cnx = _mysql(uri, local_infile=True)
    sql = (f"LOAD DATA LOCAL INFILE '{data_file}' INTO TABLE {table} "
           f"FIELDS TERMINATED BY '{field_sep}' ")
    _log.debug(f"_load_ref_data sql:{sql}")
    if not cnx.is_connected():
        cnx.connect()
    with closing(cnx.cursor()) as cursor:
        cursor.execute(f"USE {db}")
        cursor.execute(sql)
        cnx.commit()
    cnx.close()


class LoadTable:

    def __init__(self,
                 table_name,
                 ingest_config,
                 data_file,
                 partition_config_files,
                 data_staging_dir,
                 ref_db_table_schema_file):
        # the name of the table according to the load yaml
        self.table_name = table_name

        # the table ingest config dict
        self.ingest_config = ingest_config

        # the absolute path to the data file (contains the csv or tsv data)
        self.data_file = data_file

        # the absolute path to the partitioner config files
        self.partition_config_files = partition_config_files

        # the location where data can be staged (has "rw" permissions)
        self.data_staging_dir = data_staging_dir

        # the absolute path to the referecene db table schema file
        self.ref_db_table_schema_file = ref_db_table_schema_file

    @property
    def is_partitioned(self):
        return self.ingest_config["is_partitioned"]

    @property
    def is_gzipped(self):
        return os.path.splitext(self.data_file)[1] == ".gz"

    def __str__(self):
        return (
            f"LoadTable table_name={self.table_name}, "
            f"data_file={self.data_file}, "
            f"data_staging_dir={self.data_staging_dir}, "
            f"ref_db_table_schema_file={self.ref_db_table_schema_file}, "
            f"partition_config_files={self.partition_config_files}, "
            f"ingest_config={self.ingest_config}",
       )

class LoadDb:

    TablePartition = namedtuple("TablePartition", "configs_t output_t")

    def __init__(self, load_db_cfg):
        # The path to the root of the database files.
        self.root = load_db_cfg["root"]

        # self.tables is the list of table names to load into the database.
        self.tables = load_db_cfg["tables"]

        # The template for the absolute path to a table's data file.
        self.datafile_t = os.path.join(self.root, load_db_cfg["data"])

        # The template for the absolute path to a table's schema file.
        self.schema_t = os.path.join(self.root, load_db_cfg["schema"])

        # self.table_partition.configs_t contains the possibly templated
        # absolute paths to the partition config files.
        # self.table_partition.output contains the templated absolute path to
        # the output folder for the partitioner.
        self.table_partition = self.TablePartition(
            [os.path.join(self.root, config) for config in load_db_cfg["partition"]["config"]],
            load_db_cfg["partition"]["output"],  # ("output" is an absolute path.)
        )

        # self.ingest_db_cfg is the database ingest config dict.
        with open(os.path.join(self.root, load_db_cfg["ingest"]["database"])) as f:
            self.ingest_db_cfg = yaml.safe_load(f.read())

        # self.ingest_table_t is the templated absolute path to the table ingest
        # config file.
        self.ingest_table_t = os.path.join(self.root, load_db_cfg["ingest"]["table"])

    @property
    def name(self):
        return self.ingest_db_cfg["database"]

    def iter_tables(self):
        """Generator to get a LoadTable instance for each table to be loaded.
        """
        for table_name in self.tables:
            with open(self.ingest_table_t.format(table_name=table_name)) as f:
                ingest_config = json.load(f)
            yield LoadTable(
                table_name,
                ingest_config,
                self.datafile_t.format(table_name=table_name),
                [p.format(table_name=table_name) for p in self.table_partition.configs_t],
                self.table_partition.output_t.format(table_name=table_name),
                self.schema_t.format(table_name=table_name)
            )

    @property
    def db_name(self):
        return self.ingest_db_cfg["database"]

def _partition(staging_dir, table, data_file):
    """Partition data for qserv ingest.

    Parameters
    ----------
    staging_dir : `str`
        The absolute path to a folder that can be used to stage files
        used during processing that do not need to be kept.
    table : `LoadTable`
        The description of the table to partition.
    data_file : `str`
        The absolute path to the file that contains the table data. (It
        may have been unzipped to a location different than in `table`.)
    """
    # If config file paths are relative, make them absolute with the data file's dirname.
    partition_config_files = [f if os.path.isabs(f) else os.path.join(os.path.dirname(table.data_file), f) for f in table.partition_config_files]
    os.makedirs(staging_dir)
    args = ["sph-partition",]
    for config_file in partition_config_files:
        args.append("--config-file")
        args.append(config_file)
    args.extend([
        f"--in.path={data_file}",
        "--verbose",
        "--mr.num-workers=6",
        "--mr.pool-size=32768",
        "--mr.block-size=16",
        f"--out.dir={staging_dir}"
    ])
    result = subprocess.run(args, stdout=subprocess.PIPE)
    result.check_returncode()
    partition_info = result.stdout.decode("utf-8")
    partition_info = partition_info.replace("-nan", "null")
    with open(os.path.join(staging_dir, chunk_info_file), "w") as f:
        f.write(partition_info)


def _load_database(case_data, ref_db_uri, ref_db_admin, repl_ctrl_uri):
    """Load a database.

    Parameters
    ----------
    case_data : `dict`
        A dictionary containing details about loading a case_data.
    ref_db_uri : `str`
        URI to the reference db for the non-admin user.
    ref_db_admin : `str`
        URI to the reference db for the admin user.
    repl_ctrl_uri : `str`
        URI to the replication controller.
    """
    _log.info(f"Loading database for test {case_data['id']}")
    load_db = LoadDb(case_data)
    repl_ctrl = urlparse(repl_ctrl_uri)

    _ingest_database(json.dumps(load_db.ingest_db_cfg), repl_ctrl)

    _create_ref_db(ref_db_admin, load_db.name)

    for table in load_db.iter_tables():
        _create_ref_table(ref_db_admin, load_db.name, table.ref_db_table_schema_file)
        # TODO maybe we should be cosuming more info from each case's description.yaml (like field sep)

        with TemporaryDirectory(dir=qserv_data_dir) as tmp_dir:
            if table.is_gzipped:
                data_file = os.path.join(tmp_dir, os.path.splitext(os.path.basename(table.data_file))[0])
                unzip(source=table.data_file, destination=data_file)
            else:
                data_file = table.data_file
            # Create partition the partitioned table data into chunks
            staging_dir = os.path.join(tmp_dir, table.data_staging_dir)
            if table.is_partitioned:
                _partition(staging_dir, table, data_file)

            # Assume data is either comma separated (with csv) otherwise tab separated.
            data_file_ext = os.path.splitext(data_file)[1]
            _load_ref_data(ref_db_uri, data_file, load_db.name, table.table_name, "," if data_file_ext == ".csv" else "\\t")

            _ingest_table_config(json.dumps(table.ingest_config), repl_ctrl)
            transaction_id = _start_transaction(load_db.db_name, repl_ctrl)
            if table.is_partitioned:
                _ingest_chunks_data(
                    transaction_id=transaction_id,
                    table_name=table.table_name,
                    chunks_folder=staging_dir,
                    repl_ctrl=repl_ctrl,
                )
            else:
                _ingest_table_data(
                    transaction_id=transaction_id,
                    table_name=table.table_name,
                    data_file=data_file,
                    repl_ctrl=repl_ctrl,
                )
            _commit_transaction(transaction_id, repl_ctrl)

    _publish_database(load_db.db_name, repl_ctrl)


def _remove_database(case_data, ref_db_connection, repl_ctrl_uri):
    _log.debug(f"_remove_database {case_data}")
    remove_db = LoadDb(case_data)
    repl_ctrl = urlparse(repl_ctrl_uri)
    delete_database((remove_db.name,), admin=True, repl_ctrl=repl_ctrl)

    sql = f"DROP DATABASE IF EXISTS {remove_db.name}"
    with closing(_mysql(uri=ref_db_connection)) as cnx:
        with closing(cnx.cursor()) as cursor:
            cursor.execute(sql)


def unzip(source, destination):
    """Unzip a source file and write it to the target location.
    """
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    with gzip.open(source, "rb") as _source, open(destination, "wb") as _target:
        shutil.copyfileobj(_source, _target)


def load(repl_ctrl_uri, ref_db_uri, test_cases_data, ref_db_admin, cases=None, remove=False):
    """Partition and ingest data into qserv.

    Parameters
    ----------
    databases_yaml : `str`
        A yaml file that describes the databases to ingest.
    ref_db_admin : `str`
        The connection URI for the root/admin user of the reference database.
    cases : `list` [`str`], optional
        Restrict loading to these test cases if provided.
    remove : bool, optional
        Instead of loading the databases, remove them. By default False

    Raises
    ------
    RuntimeError
        If the named database is not in the provided yaml.
    """
    if cases:
        db_data = {db["id"]: db for db in test_cases_data}
        try:
            cases_data = [db_data[case] for case in cases]
        except KeyError as e:
            raise RuntimeError(f"{e.args[0]} is not in {test_cases_data}")
    else:
        cases_data = test_cases_data

    for case_data in cases_data:
        if remove:
            _remove_database(case_data, ref_db_admin, repl_ctrl_uri)
        else:
            _load_database(case_data, ref_db_uri, ref_db_admin, repl_ctrl_uri)


def remove(repl_ctrl_uri, ref_db_uri, test_cases_data, ref_db_admin, cases=None):
    load(repl_ctrl_uri, ref_db_uri, test_cases_data, ref_db_admin, cases, remove=True)
