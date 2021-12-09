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
from functools import partial
import json
import logging
import mysql.connector
import os
from pathlib import Path
from sqlalchemy.engine.url import URL
import subprocess
import sys
import time
from typing import Callable, Dict, List, Optional, Sequence, Union
from sqlalchemy.engine.url import make_url

from .utils import split_kv
from ..itest import ITestResults
from ..qserv_backoff import on_backoff
from ..template import apply_template_cfg_file, save_template_cfg
from ...schema import smig, smig_block
from ...schema import MigMgrArgs, SchemaUpdateRequired
from ..replicationInterface import ReplicationInterface
from . import _integration_test, options


smig_dir_env_var = "QSERV_SMIG_DIRECTORY"
default_smig_dir = "/usr/local/qserv/smig"


def smig_dir() -> str:
    return os.environ.get(smig_dir_env_var, default_smig_dir)


admin_smig_dir = "admin/schema"
css_smig_dir = "css/schema"
rproc_smig_dir = "rproc/schema"
qmeta_smig_dir = "qmeta/schema"
worker_smig_dir = "worker/schema"
replication_controller_smig_dir = "replica/schema"

replica_controller_cfg_path = "/config-etc/replicaConfig.sql"
replica_controller_log_template = (
    "/usr/local/qserv/templates/repl-ctl/etc/log4cxx.replication.properties.jinja"
)
replica_controller_log_path = "/config-etc/log4cxx.replication.properties"
replica_controller_http_root = "/usr/local/qserv/www"

mysqld_user_qserv = "qsmaster"

proxy_empty_chunk_path = "/qserv/data/qserv"

czar_proxy_config_template = "/usr/local/qserv/templates/proxy/etc/my-proxy.cnf.jinja"
czar_proxy_config_path = "/config-etc/my-proxy.cnf"
czar_config_template = "/usr/local/qserv/templates/proxy/etc/qserv-czar.cnf.jinja"
czar_config_path = "/config-etc/qserv-czar.cnf"
worker_wgmr_config_path = "/config-etc/wmgr.cnf"

cmsd_manager_cfg_template = "/usr/local/qserv/templates/xrootd/etc/cmsd-manager.cf.jinja"
cmsd_manager_cfg_path = "/config-etc/cmsd-manager.cnf"

cmsd_worker_cfg_template = "/usr/local/qserv/templates/xrootd/etc/cmsd-worker.cf.jinja"
cmsd_worker_cfg_path = "/config-etc/cmsd-worker.cf"
xrdssi_cfg_template = "/usr/local/qserv/templates/xrootd/etc/xrdssi.cf.jinja"
xrdssi_cfg_path = "/config-etc/xrdssi-worker.cf"

xrootd_manager_cfg_template = "/usr/local/qserv/templates/xrootd/etc/xrootd-manager.cf.jinja"
xrootd_manager_cfg_path = "/config-etc/xrootd-manager.cf"

_log = logging.getLogger(__name__)


def _jitter(f: float) -> float:
    return 0.0


@backoff.on_exception(
    exception=SchemaUpdateRequired,
    on_backoff=on_backoff(log=_log),
    wait_gen=backoff.constant,
    interval=10,  # Wait 10 seconds between retries.
    jitter=_jitter,  # Don't add jitter (random small changes up or down) to the wait time.
    giveup=lambda e: bool(os.environ.get("UNIT_TEST", False)),
)
def _wait_for_update(smig_func: Callable[[], None]) -> None:
    """Wrapper for a smig function that includes a backoff for the case where
    the module needs to be updated but does not get updated by this process,
    allowing the process to wait here for the module to be updated.
    """
    smig_func()


def _do_smig(
    module_smig_dir: str,
    module: str,
    connection: str,
    update: bool,
    *,
    mig_mgr_args: MigMgrArgs = None,
) -> None:
    """Run schema migration on a module's database.

    Parameters
    ----------
    module_smig_dir : str
        The path to the module's schema migration files, inside the `smig_dir`.
    module : str
        The name of the module whose schema is being migrated.
    connection : str
        The uri to the database that will be affected.
    update : bool
        If the database is already initialized, do run any available updates.
    mig_mgr_args : MigMgrArgs
        Arguments to the __init__ function of the `SchemaMigMgr` subclass, by
        default None
    """
    smig_func = partial(
        smig,
        do_migrate=True,
        check=False,
        final=None,
        scripts=os.path.join(smig_dir(), module_smig_dir),
        connection=connection,
        module=module,
        mig_mgr_args=mig_mgr_args or dict(),
        update=update,
    )
    if update:
        smig_func()
    else:
        _wait_for_update(smig_func)


def _do_smig_block(module_smig_dir: str, module: str, connection: str) -> None:
    """Wait for a module's schema to be updated to the latest version.

    Parameters
    ----------
    module_smig_dir : str
        The path to the module's schema migration files, inside the `smig_dir`.
    module : str
        The name of the module whose schema is being migrated.
    connection : str
        The uri to the database that will be affected.
    """
    smig_block(
        scripts=os.path.join(smig_dir(), module_smig_dir),
        connection=connection,
        module=module,
    )


class InvalidQueryParameter(RuntimeError):
    """Raised when a URI contains query keys that are not supported for that
    URI.
    """
    pass


def _process_uri(uri: str, query_keys: Sequence[str], option: str, block: bool) -> URL:
    """Convert a string URI to a sqlalchemy URL. Verify query keys are valid.
    If indicated by block==True and a hostname and port are provided, wait until
    the database at `uri` is processing connection requests (even if they are
    rejected, if the socket is open the database is running).

    Parameters
    ----------
    uri : str
        The uri string to process.
    query_keys : Sequence[str]
        The keys that are allowed to be in the query.
    option : str
        The option name that is associated with the URI.
    block : bool
        If true and the a hostname and port are in the uri, then block until the
        the server is processing TCP connections.

    Raises
    ------
    InvalidQueryParameter
        Raised if there are values in `keys` that are not in `query_keys`

    Returns
    -------
    url : sqlalchemy.engine.url.URL
        The `URL` object derived from the parsed `uri`.
    """
    @backoff.on_exception(
        exception=mysql.connector.errors.DatabaseError,
        wait_gen=backoff.expo,
        on_backoff=on_backoff(log=_log),
    )
    def wait_for_db(url: URL) -> None:
        try:
            with closing(
                mysql.connector.connect(
                    user=url.username or "",
                    password=url.password or "",
                    host=url.host or "",
                    port=url.port or "",
                )
            ):
                pass
        except mysql.connector.errors.ProgrammingError:
            # ProgrammingError is raised if we don't have permission to connect (yet...).
            # This is ok; the db is active & reachable and that's all we're waiting for here.
            pass

    url = make_url(uri)
    if (any(remainders := set(url.query.keys()) - set(query_keys))):
        raise InvalidQueryParameter(f"Invalid query key(s) ({remainders}); {option} accepts {query_keys or 'no keys'}.")
    if block and url.host and url.port:
        wait_for_db(url)
    return url


def smig_czar(connection: str, update: bool) -> None:
    """Apply schema migration scripts to czar modules.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database
    update : bool
        False if workers may only be smigged from an `Uninitialized` state, or
        True if they maybe upgraded from a (previously initialized) version.
    """
    for module_smig_dir, module in (
        (admin_smig_dir, "admin"),
        (css_smig_dir, "css"),
        (rproc_smig_dir, "rproc"),
        (qmeta_smig_dir, "qmeta"),
    ):
        _do_smig(module_smig_dir, module, connection, update)


def smig_replication_controller(
    db_uri: Optional[str],
    db_admin_uri: str,
    update: bool,
    set_initial_configuration: Optional[Callable[[], None]] = None,
) -> None:
    """Apply schema migration scripts to the replication controller.

    Parameters
    ----------
    db_uri : `str`, optional
        The connection string for the replication manager database for the
        non-admin user. Required when initializing the database, not needed
        when upgrading the database.
    db_admin_uri : `str`
        Connection string in format mysql://user:pass@host:port/database
    update : bool
        False if workers may only be smigged from an `Uninitialized` state, or
        True if they maybe upgraded from a (previously initialized) version.
    set_initial_configuration : `Callable[[], None]`
        A function that takes no arguments and returns nothing that can be used
        by the replication controller migration manager to initialize the
        replication controller.
    """
    _do_smig(
        replication_controller_smig_dir,
        "replica",
        db_admin_uri,
        update,
        mig_mgr_args=dict(
            set_initial_configuration=set_initial_configuration,
            repl_connection=db_uri,
        )
        if set_initial_configuration
        else None,
    )


def smig_worker(connection: str, update: bool = False) -> None:
    """Apply schema migration scripts to the worker modules.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database
    update : bool, optional
        False if workers may only be smigged from an `Uninitialized` state, or
        True if they maybe upgraded from a (previously initialized) version, by
        default False.
    """
    _do_smig(admin_smig_dir, "admin", connection, update)
    _do_smig(worker_smig_dir, "worker", connection, update)


def enter_manager_cmsd(cms_delay_servers: str) -> None:
    """Start a cmsd manager qserv node.

    Parameters
    ----------
    cms_delay_servers : str
        Percentage value for 'cms.delay servers' in the cmsd-manager.cf file.
    """
    # TODO xrootd_managers needs to be passed in by execution env or mounted in /config-etc
    save_template_cfg(
        dict(
            xrootd_managers=["localhost"],
            cmsd_manager="UNUSED",
            cms_delay_servers=cms_delay_servers,
        )
    )
    apply_template_cfg_file(cmsd_manager_cfg_template, cmsd_manager_cfg_path)
    args: List[Union[str, int]] = [
        "cmsd",
        "-c",
        cmsd_manager_cfg_path,
        "-n",
        "manager",
        "-I",
        "v4",
    ]
    sys.exit(_run(args))


def enter_xrootd_manager(cmsd_manager: str) -> None:
    """Start an xrootd manager qserv node.

    Parameters
    ----------
    cmsd_manager : str
        The host name of the cmsd manager.
    """
    save_template_cfg(
        dict(
            cmsd_manager=cmsd_manager,
        )
    )
    apply_template_cfg_file(xrootd_manager_cfg_template, xrootd_manager_cfg_path)
    sys.exit(
        _run(
            [
                "xrootd",
                "-c",
                xrootd_manager_cfg_path,
                "-n",
                "manager",
                "-I",
                "v4",
            ],
        )
    )


def enter_worker_cmsd(cmsd_manager: str, vnid: str, debug_port: Optional[int], db_uri: str) -> None:
    """Start a worker cmsd node.

    Parameters
    ----------
    cmsd_manager : str
        The host name of the cmsd manager.
    vnid : str
        The virtual network id for this component.
    debug_port : int or None
        If not None, indicates that gdbserver should be run on the given port number.
    db_uri : str
        The non-admin URI to the worker's databse.
    """
    url = _process_uri(
        uri=db_uri,
        query_keys=("socket",),
        option=options.db_uri_option.args[0],
        block=True,
    )
    save_template_cfg(
        dict(
            vnid=vnid,
            cmsd_manager=cmsd_manager,
            db_host=url.host,
            db_port=url.port or "",
            db_socket=url.query.get("socket", ""),
            mysqld_user_qserv=url.username,
        )
    )

    apply_template_cfg_file(cmsd_worker_cfg_template, cmsd_worker_cfg_path)
    apply_template_cfg_file(xrdssi_cfg_template, xrdssi_cfg_path)

    _do_smig_block(admin_smig_dir, "admin", db_uri)

    args = [
        "cmsd",
        "-c",
        cmsd_worker_cfg_path,
        "-n",
        "worker",
        "-I",
        "v4",
        "-l",
        "@libXrdSsiLog.so",
        "-+xrdssi",
        xrdssi_cfg_path,
    ]
    sys.exit(_run(args, debug_port=debug_port))


def enter_worker_xrootd(
    debug_port: Optional[int],
    db_uri: str,
    db_admin_uri: str,
    vnid: str,
    cmsd_manager: str,
    repl_ctl_dn: str,
    mysql_monitor_password: str,
    db_qserv_user: str,
) -> None:
    """Start a worker xrootd node.

    Parameters
    ----------
    debug_port : int or None
        If not None, indicates that gdbserver should be run on the given port number.
    db_uri : str
        The non-admin URI to the proxy's databse.
    db_admin_uri : str
        The admin URI to the proxy's database.
    vnid : str
        The virtual network id for this component.
    cmsd_manager : str
        The host name of the cmsd manager.
    repl_ctl_dn : str
        The fully qualified domain name of the replication controller.
    mysql_monitor_password : str
        The password used by applications that monitor via the worker database.
    db_qserv_user : str
        The qserv user to use for the mysql database.
    """

    # TODO This sets the amount of data that can be locked into memory to
    # almost the entire amount of memory on the machine. I think in a dev-env
    # (docker-only on a single machine) we don't want to grab quite so much
    # memory? TBD WTD here - do/don't set this? set it to how much? needs a way
    # to set env (dev/prod/etc)
    # # Increase limit for locked-in-memory size
    # MLOCK_AMOUNT=$(grep MemTotal /proc/meminfo | awk '{printf("%.0f\n", $2 - 1000000)}')
    # ulimit -l "$MLOCK_AMOUNT"

    url = _process_uri(
        uri=db_uri,
        query_keys=("socket",),
        option=options.db_uri_option.args[0],
        block=True,
    )
    _ = _process_uri(
        uri=db_admin_uri,
        query_keys=("socket",),
        option=options.db_admin_uri_option.args[0],
        block=True,
    )
    save_template_cfg(
        dict(
            vnid=vnid,
            cmsd_manager=cmsd_manager,
            db_host=url.host or "",
            db_port=str(url.port) or "",
            db_socket=url.query.get("socket", ""),
            mysqld_user_qserv=db_qserv_user,
            replication_controller_FQDN=repl_ctl_dn,
            mysql_monitor_password=mysql_monitor_password,
        )
    )

    # enter_worker_cmsd smigs the worker db for that node
    smig_worker(db_admin_uri, update=False)

    # TODO worker (and manager) xrootd+cmsd pair should "share" the cfg file
    # it's in different pods but should be same source & processing.
    # Rename these files to be more agnostic.
    apply_template_cfg_file(cmsd_worker_cfg_template, cmsd_worker_cfg_path)
    apply_template_cfg_file(xrdssi_cfg_template, xrdssi_cfg_path)

    args = [
        "xrootd",
        "-c",
        cmsd_worker_cfg_path,
        "-n",
        "worker",
        "-I",
        "v4",
        "-l",
        "@libXrdSsiLog.so",
        "-+xrdssi",
        xrdssi_cfg_path,
    ]
    sys.exit(_run(args, debug_port=debug_port))


def enter_worker_repl(
    db_admin_uri: str, repl_connection: str, debug_port: Optional[int], run: bool, instance_id: str
) -> None:
    """Start a worker replication node.

    Parameters
    ----------
    db_admin_uri : str
        The admin URI to the worker's database.
    repl_connection : `str`
        The connection string for the replication manager database for the
        non-admin user (created using the `connection`), the user is typically
        "qsreplica".
    debug_port : int or None
        If not None, indicates that gdbserver should be run on the given port number.
    run : `bool`
        Run the subcommand that is executed by entrypoint if `True`. Otherwise,
        print the command and arguments that would have been run.
    instance_id : `str`
        A unique identifier of a Qserv instance served by the Replication
        System. Its value will be passed along various internal communication
        lines of the system to ensure that all services are related to the same
        instance. This mechanism also prevents 'cross-talks' between two (or
        many) Replication System's setups in case of an accidental
        mis-configuration.
    """
    _ = _process_uri(
        uri=db_admin_uri,
        query_keys=(),
        option=options.db_admin_uri_option.args[0],
        block=True,
    )
    _ = _process_uri(
        uri=repl_connection,
        query_keys=(),
        option=options.db_admin_uri_option.args[0],
        block=True,
    )

    # N.B. When the controller smigs the replication database, if it is migrating from Uninitialized
    # it will also set initial configuration values in the replication database. It sets the schema
    # version of the replica database *after* setting the config values, which allows us to wait here
    # on the schema version to be sure that there are values in the database.
    _do_smig_block(replication_controller_smig_dir, "replica", repl_connection)

    ingest_folder = "/qserv/data/ingest"
    if not os.path.exists(ingest_folder):
        os.makedirs(ingest_folder)

    args = [
        "qserv-replica-worker",
        f"--config={repl_connection}",
        f"--qserv-worker-db={db_admin_uri}",
        "--debug",
        f"--instance-id={instance_id}",
    ]
    while True:
        # This loop exists because it is possible for qserv-replica-worker to
        # register itself with the replica controller before the call to
        # qserv-replica-config has finished processing the config. The
        # controller then rejects the replica worker, and the replica worker is
        # not resilient to this condition (does wait and try again to register
        # itself). Do not bother to check the return code because if
        # qserv-replica-worker returned then by definition it failed, and we
        # just wait a moment and restart it.
        # This is recorded in DM-31252
        _run(args, debug_port=debug_port, run=run)
        _log.info("qserv-replica-worker exited. waiting 5 seconds and restarting.")
        time.sleep(5)


def enter_proxy(
    db_uri: str,
    db_admin_uri: str,
    repl_ctl_dn: str,
    mysql_monitor_password: str,
    xrootd_manager: str,
    proxy_backend_address: str,
) -> None:
    """Entrypoint script for the proxy container.

    Parameters
    ----------
    db_uri : str
        The non-admin URI to the proxy's databse.
    db_admin_uri : str
        The admin URI to the proxy's database.
    repl_ctl_dn : str
        The fully qualified domain name of the replication controller.
    mysql_monitor_password : str
        The password used by applications that monitor via the worker database.
    xrootd_manager : `str`
        The host name of the xrootd manager node.
    proxy_backend_address : `str`
        A colon-separated ip address and port number (e.g. "127.0.0.1:3306")
        substituded into my-proxy.cnf.jinja, used by mysql proxy.
    """
    url = _process_uri(
        uri=db_uri,
        query_keys=("socket",),
        option=options.db_uri_option.args[0],
        block=True,
    )
    _ = _process_uri(
        uri=db_admin_uri,
        query_keys=("socket",),
        option=options.db_admin_uri_option.args[0],
        block=True,
    )

    # TODO the empty chunk path should be defined in some default configuration
    # somewhere/somehow. TBD. Note that it must be created in the dockerfile
    # by the root user and chown'd to the qserv user.
    save_template_cfg(
        dict(
            mysqld_user_qserv=url.username,
            replication_controller_FQDN=repl_ctl_dn,
            mysql_monitor_password=mysql_monitor_password,
            empty_chunk_path="/qserv/data/qserv",
            xrootd_manager=xrootd_manager,
            proxy_backend_address=proxy_backend_address,
            czar_db_host=url.host or "",
            czar_db_port=url.port or "",
            czar_db_socket=url.query.get("socket", ""),
        )
    )
    apply_template_cfg_file(czar_proxy_config_template, czar_proxy_config_path)
    apply_template_cfg_file(czar_config_template, czar_config_path)

    smig_czar(db_admin_uri, update=False)

    env = dict(os.environ, QSERV_CONFIG=czar_config_path)

    args = [
        "mysql-proxy",
        "--proxy-lua-script=/usr/local/lua/qserv/scripts/mysqlProxy.lua",
        "--lua-cpath=/usr/local/lua/qserv/lib/czarProxy.so",
        f"--defaults-file={czar_proxy_config_path}",
    ]
    sys.exit(_run(args, env=env))


def enter_replication_controller(
    db_uri: str,
    db_admin_uri: str,
    workers: List[str],
    instance_id: str,
    run: bool,
    xrootd_manager: str,
    qserv_czar_db: str,
) -> None:
    """Entrypoint script for the entrypoint controller.

    Parameters
    ----------
    db_uri : `str`
        The connection string for the replication manager database for the
        non-admin user (created using the `connection`), the user is typically
        "qsreplica".
    db_admin_uri : `str`
        The connection string for the replication manager database for the
        administrative (typically root) user.
    workers : `list` [`str`]
        A list of parameters for each worker in the format "key=value,..."
        For example:
        `["name=worker_0,host=worker-repl-0", "name=worker_1,host=worker-repl-1"]
    instance_id : `str`
        A unique identifier of a Qserv instance served by the Replication
        System. Its value will be passed along various internal communication
        lines of the system to ensure that all services are related to the same
        instance. This mechanism also prevents 'cross-talks' between two (or
        many) Replication System's setups in case of an accidental
        mis-configuration.
    xrootd_manager : `str`
        The host name of the xrootd manager node.
    qserv_czar_db : `str`
        The URI connection string for the czar database.
    run : `bool`
        Run the subcommand that is executed by entrypoint if `True`. Otherwise,
        print the command and arguments that would have been run.
    """

    def set_initial_configuartion(workers: Sequence[str], xrootd_manager: str) -> None:
        """Add the initial configuration to the replication database.
        Should only be called if the replication database has newly been smigged to version 1."""
        args = [
            "qserv-replica-config",
            "UPDATE_GENERAL",
            f"--config={db_uri}",
            f"--xrootd.host={xrootd_manager}",
        ]
        _log.debug(f"Calling {' '.join(args)}")
        _run(args, run=run, check_returncode=True)

        workers_ = [split_kv((w,)) for w in workers]
        for worker in workers_:
            try:
                name = worker.pop("name")
                host = worker.pop("host")
            except KeyError as e:
                raise RuntimeError("The worker option must contain entries 'name' and 'host'") from e
            args = [
                "qserv-replica-config",
                "ADD_WORKER",
                f"--config={db_uri}",
                name,
                host,
            ]
            args += [f"--{key}={val}" for key, val in worker.items()]
            _log.debug(f"Calling {' '.join(args)}")
            _run(args, run=run, check_returncode=True)
        _log.info(f"Finished setting initial configuration {workers_}")

    _ = _process_uri(
        uri=db_uri,
        query_keys=(),
        option=options.db_uri_option.args[0],
        block=True,
    )
    _ = _process_uri(
        uri=db_admin_uri,
        query_keys=("socket",),
        option=options.db_admin_uri_option.args[0],
        block=True,
    )

    # The replication controller depends on this folder existing and does not create it if it's missing.
    # It should get fixed in DM-30074. For now we create it here.
    os.makedirs("/qserv/data/ingest", exist_ok=True)

    if run:
        smig_replication_controller(
            db_admin_uri=db_admin_uri,
            db_uri=db_uri,
            update=False,
            set_initial_configuration=partial(set_initial_configuartion, workers, xrootd_manager),
        )

    env = dict(os.environ, LSST_LOG_CONFIG=replica_controller_log_path)

    args = [
        "qserv-replica-master-http",
        f"--config={db_uri}",
        f"--instance-id={instance_id}",
        f"--qserv-czar-db={qserv_czar_db}",
        f"--http-root={replica_controller_http_root}",
    ]
    _log.debug(f"Calling {' '.join(args)}")
    sys.exit(_run(args, env=env, run=run))


def smig_update(czar_connection: str, worker_connections: List[str], repl_connection: str) -> None:
    """Update smig on nodes that need it.

    All connection strings are in format mysql://user:pass@host:port/database

    Parameters
    ----------
    czar_connection : `str`
        Connection string to the czar database.
    worker_connections : `list` [ `str` ]
        Connection strings to the worker databases.
    repl_connection : `str`
        Connection string replication controller database.
    """
    if czar_connection:
        smig_czar(connection=czar_connection, update=True)
    if worker_connections:
        for c in worker_connections:
            smig_worker(connection=c, update=True)
    if repl_connection:
        smig_replication_controller(db_admin_uri=repl_connection, db_uri=None, update=True)


def _run(
    args: Sequence[Union[str, int]],
    env: Dict[str, str] = None,
    debug_port: Optional[int] = None,
    run: bool = True,
    check_returncode: bool = False,
) -> int:
    """Run a command in a subprocess.

    Parameters
    ----------
    args : List[Union[str, int]]
        The command and arguments to the command.
    env : Dict[str, str], optional
        The environment variables to run the command with, by default None which
        uses the same environment as the current shell.
    debug_port : Optional[int], optional
        If provided, runs the command in gdbserver using the given port number.
        If not provided runs the command normally, by default None
    run : bool, optional
        If False, instead of running the command, the command that would have
        been run is printed and 0 is returned, as though the command had run
        successfully. If True, runs the command normally, by default True
    check_returncode : bool
        If true, will call `check_returncode` on the result of `subprocess.run`.
        This is useful when a script command runs multiple subprocesses during
        its execution and the subprocess is expected to return correctly.
        When a script command runs exactly one subprocess and exits when that
        command exits it is usually sufficient to say `return
        sys.exit(_run(...))` and let the caller handle the exit code.

    Returns
    -------
    exit_code : `int`
        The exit code of the command that was run.
    """
    str_args = [str(a) for a in args]
    if debug_port:
        str_args = ["gdbserver", f"localhost:{debug_port}"] + str_args
    if not run:
        print(" ".join(str_args))
        return 0
    result = subprocess.run(str_args, env=env, cwd="/home/qserv")
    if check_returncode:
        result.check_returncode
    return result.returncode


def delete_database(repl_ctrl_uri: str, database: str, admin: bool) -> None:
    """Remove a database from qserv.

    Parameters
    ----------
    repl_ctrl_uri : `str`
        The uri to the replication controller service.
    database : `str`
        The name of the database to delete.
    admin : `bool`
        True if the admin auth key should be used.
    """
    repl = ReplicationInterface(repl_ctrl_uri)
    repl.delete_database(database, admin)


def load_simple(repl_ctrl_uri: str) -> None:
    """Load a simple predefined database into qserv.

    The database is called "test101" and have a table called Object with one row.

    Parameters
    ----------
    repl_ctrl_uri : `str`
        The uri to the replication controller service.
    """
    repl = ReplicationInterface(repl_ctrl_uri)

    database = "test101"

    repl.ingest_database(
        database_json=json.dumps(
            {
                "database": database,
                "num_stripes": 340,
                "num_sub_stripes": 3,
                "overlap": 0.01667,
                "auto_build_secondary_index": 1,
                "local_load_secondary_index": 1,
                "auth_key": "",
            }
        ),
    )
    repl.ingest_table_config(
        table_json=json.dumps(
            {
                "database": database,
                "table": "Object",
                "is_partitioned": 1,
                "chunk_id_key": "chunkId",
                "sub_chunk_id_key": "subChunkId",
                "is_director": 1,
                "director_key": "objectId",
                "latitude_key": "dec",
                "longitude_key": "ra",
                "schema": [
                    {"name": "objectId", "type": "BIGINT NOT NULL"},
                    {"name": "ra", "type": "DOUBLE NOT NULL"},
                    {"name": "dec", "type": "DOUBLE NOT NULL"},
                    {"name": "property", "type": "DOUBLE"},
                    {"name": "chunkId", "type": "INT UNSIGNED NOT NULL"},
                    {"name": "subChunkId", "type": "INT UNSIGNED NOT NULL"},
                ],
                "auth_key": "",
            }
        ),
    )
    transaction_id = repl.start_transaction(database=database)
    chunk_location = repl.ingest_chunk_config(transaction_id, "0")
    repl.ingest_data_file(
        transaction_id,
        chunk_location.host,
        chunk_location.port,
        data_file=os.path.join(Path(__file__).parent.absolute(), "chunk_0.txt"),
        table="Object",
        partitioned=True,
    )
    repl.commit_transaction(transaction_id)
    repl.publish_database(database)


def integration_test(
    repl_connection: str,
    pull: Optional[bool],
    unload: bool,
    load: Optional[bool],
    reload: bool,
    cases: List[str],
    run_tests: bool,
    tests_yaml: str,
    compare_results: bool,
) -> ITestResults:
    if repl_connection is not None:
        _do_smig_block(admin_smig_dir, "replica", repl_connection)
    return _integration_test.run_integration_tests(
        pull=pull,
        unload=unload,
        load=load,
        reload=reload,
        cases=cases,
        run_tests=run_tests,
        tests_yaml=tests_yaml,
        compare_results=compare_results,
        mysqld_user=mysqld_user_qserv,
    )
