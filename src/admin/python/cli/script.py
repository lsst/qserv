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
from functools import partial
import json
import logging
import os
from pathlib import Path
import subprocess
import shutil
import sys
import time
from typing import Callable, Dict, List, Optional, Union
from urllib.parse import urlparse

from lsst.qserv.admin.qserv_backoff import on_backoff
from lsst.qserv.admin.template import apply_template_cfg_file, save_template_cfg
from lsst.qserv.schema import smig, smig_block
from lsst.qserv.schema.schemaMigMgr import SchemaUpdateRequired
from lsst.qserv.admin.replicationInterface import ReplicationInterface
from . import _integration_test


smig_dir_env_var = "QSERV_SMIG_DIRECTORY"
default_smig_dir = "/usr/local/qserv/smig"


def smig_dir():
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

nginx_controller_cfg_template = "/usr/local/qserv/templates/dashboard/etc/nginx.conf.jinja"
nginx_controller_cfg_path = "/config-etc/nginx.conf"
dashboard_files_src = "/usr/local/qserv/dashboard/www"
dashboard_files_dst = "/config-etc/www"
dashboard_launch_gate = "/config-etc/goahead"


_log = logging.getLogger(__name__)


@backoff.on_exception(
    exception=SchemaUpdateRequired,
    on_backoff=on_backoff(log=_log),
    wait_gen=backoff.constant,
    interval=10,  # Wait 10 seconds between retries.
    jitter=None,  # Don't add jitter (random small changes up or down) to the wait time.
    giveup=lambda e: os.environ.get("UNIT_TEST", False),
)
def _wait_for_update(smig_func: Callable[[], None]) -> None:
    """Wrapper for a smig function that includes a backoff for the case where
    the module needs to be updated but does not get updated by this process,
    allowing the process to wait here for the module to be updated.
    """
    smig_func()


def _do_smig(
    module_smig_dir: str, module: str, connection: str, update: bool, *, mig_mgr_args: Dict[str, str] = None
):
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
    mig_mgr_args : Dict[str, str], optional
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


def _do_smig_block(module_smig_dir: str, module: str, connection: str):
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


def smig_czar(connection, update):
    """Apply schema migration scripts to czar modules.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database
    update : bool, optional
        False if workers may only be smigged from an `Uninitialized` state, or
        True if they maybe upgraded from a (previously initialized) version, by
        default False.
    """
    for module_smig_dir, module in (
        (admin_smig_dir, "admin"),
        (css_smig_dir, "css"),
        (rproc_smig_dir, "rproc"),
        (qmeta_smig_dir, "qmeta"),
    ):
        _do_smig(module_smig_dir, module, connection, update)


def smig_replication_controller(
    connection: str,
    update: Optional[bool],
    set_initial_configuration: Optional[Callable[[], None]] = None,
):
    """Apply schema migration scripts to the replication controller.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database
    update : bool, optional
        False if workers may only be smigged from an `Uninitialized` state, or
        True if they maybe upgraded from a (previously initialized) version, by
        default False.
    set_initial_configuration : `Callable[[], None]`
        A function that takes no arguments and returns nothing that can be used
        by the replication controller migration manager to initialize the
        replication controller.
    """
    _do_smig(
        replication_controller_smig_dir,
        "replica",
        connection,
        update,
        mig_mgr_args=dict(set_initial_configuration=set_initial_configuration)
        if set_initial_configuration
        else None,
    )


def smig_worker(connection: str, update: bool = False):
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
    _do_smig(admin_smig_dir, "admin", connection, update),
    _do_smig(worker_smig_dir, "worker", connection, update),


def init_dashboard(dashboard_port: int, dashboard_html: str, repl_ctl_dn: str, repl_ctl_port: int):
    """Initialize the dashboard configuration files by copying the config files
    into a a shared volume. Finish by creating a file that signals the dashboard
    container init script that it may proceed.

    Parameters
    ----------
    dashboard_port : int
        The port the dashboard will serve on.
    dashboard_html : str
        The path to the folder with the html sources for the nginx dashboard.
    repl_ctl_dn : str
        The fully qualified domain name of the replication controller.
    repl_ctl_port : int
        The port that the replication controller is listening on.
    """
    if os.path.exists(dashboard_launch_gate):
        print(f"{dashboard_launch_gate} exists; exiting without making changes.")
        return

    save_template_cfg(
        dict(
            dashboard=dict(
                port=dashboard_port,
                html=dashboard_html,
            ),
            repl_manager=dict(
                domain_name=repl_ctl_dn,
                http_server_port=repl_ctl_port,
            ),
        )
    )
    apply_template_cfg_file(nginx_controller_cfg_template, nginx_controller_cfg_path)

    shutil.copytree(src=dashboard_files_src, dst=dashboard_files_dst)
    with open(dashboard_launch_gate, "w") as f:
        pass
    print(
        f"Copied dashboard files to {dashboard_files_dst} and wrote the nginx controller config to {nginx_controller_cfg_path}; exiting."
    )


def enter_manager_cmsd(cms_delay_servers: str):
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
    args = [
        "cmsd",
        "-c",
        cmsd_manager_cfg_path,
        "-n",
        "manager",
        "-I",
        "v4",
    ]
    sys.exit(_run(args))


def enter_xrootd_manager(cmsd_manager: str):
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


def enter_worker_cmsd(cmsd_manager: str, vnid: str, debug_port: Optional[int], connection: str):
    """Start a worker cmsd node.

    Parameters
    ----------
    cmsd_manager : str
        The host name of the cmsd manager.
    vnid : str
        The virtual network id for this component.
    debug_port : int or None
        If not None, indicates that gdbserver should be run on the given port number.
    connection : str
        Connection string to the worker database.
    """
    parsed = urlparse(connection)
    save_template_cfg(
        dict(
            vnid=vnid,
            cmsd_manager=cmsd_manager,
            db_host=parsed.hostname,
            db_port=parsed.port,
            mysqld_user_qserv=parsed.username,
        )
    )

    apply_template_cfg_file(cmsd_worker_cfg_template, cmsd_worker_cfg_path)
    apply_template_cfg_file(xrdssi_cfg_template, xrdssi_cfg_path)

    _do_smig_block(admin_smig_dir, "admin", connection)

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
    connection: str,
    vnid: str,
    cmsd_manager: str,
    repl_ctl_dn: str,
    mysql_monitor_password: str,
    db_qserv_user: str,
):
    """Start a worker xrootd node.

    Parameters
    ----------
    debug_port : int or None
        If not None, indicates that gdbserver should be run on the given port number.
    connection : str
        Connection string to the worker database.
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

    parsed = urlparse(connection)
    save_template_cfg(
        dict(
            vnid=vnid,
            cmsd_manager=cmsd_manager,
            db_host=parsed.hostname,
            db_port=parsed.port,
            mysqld_user_qserv=db_qserv_user,
            replication_controller_FQDN=repl_ctl_dn,
            mysql_monitor_password=mysql_monitor_password,
        )
    )

    # enter_worker_cmsd smigs the worker db for that node
    smig_worker(connection, update=False)

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
    vnid: str, connection: str, repl_connection: str, debug_port: Optional[int], run: bool, instance_id: str
):
    """Start a worker replication node.

    Parameters
    ----------
    vnid : str
        The virtual network id for this component.
    connection : str
        Connection string to the worker database.
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
        vnid,
        f"--config={repl_connection}",
        f"--qserv-worker-db={connection}",
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
    db_scheme: str,
    connection: str,
    mysql_user_qserv: str,
    repl_ctl_dn: str,
    mysql_monitor_password: str,
    xrootd_manager: str,
    czar_db_host: str,
    czar_db_port: int,
    czar_db_socket: str,
):
    """Entrypoint script for the proxy container.

    Parameters
    ----------
    db_scheme : str
        The scheme for the proxy backend address.
    connection : str
        The rest of the connection string (in addition to `db_scheme`) for the proxy backend address.
    mysql_user_qserv : str
        The qserv user to use for the mysql database.
    repl_ctl_dn : str
        The fully qualified domain name of the replication controller.
    mysql_monitor_password : str
        The password used by applications that monitor via the worker database.
    xrootd_manager : `str`
        The host name of the xrootd manager node.
    czar_db_host : str
        The name of the czar database host.
    czar_db_port : int
        The port the czar database is serving on.
    czar_db_socket : str
        The unix socket of the czar database host. This can be used if the proxy
        container and the database are running on the same filesystem (e.g. in a
        pod).
    """
    connection = f"{db_scheme}://{connection}"
    parsed = urlparse(connection)

    # TODO the empty chunk path should be defined in some default configuration
    # somewhere/somehow. TBD. Note that it must be created in the dockerfile
    # by the root user and chown'd to the qserv user.
    save_template_cfg(
        dict(
            mysqld_user_qserv=mysql_user_qserv,
            replication_controller_FQDN=repl_ctl_dn,
            mysql_monitor_password=mysql_monitor_password,
            empty_chunk_path="/qserv/data/qserv",
            xrootd_manager=xrootd_manager,
            backend_host=parsed.hostname,
            backend_port=parsed.port,
            czar_db_host=czar_db_host,
            czar_db_port=czar_db_port,
            czar_db_socket=czar_db_socket,
        )
    )
    apply_template_cfg_file(czar_proxy_config_template, czar_proxy_config_path)
    apply_template_cfg_file(czar_config_template, czar_config_path)

    smig_czar(connection, update=False)

    env = dict(os.environ, QSERV_CONFIG=czar_config_path)

    args = [
        "mysql-proxy",
        "--proxy-lua-script=/usr/local/lua/qserv/scripts/mysqlProxy.lua",
        "--lua-cpath=/usr/local/lua/qserv/lib/czarProxy.so",
        f"--defaults-file={czar_proxy_config_path}",
    ]
    sys.exit(_run(args, env=env))


def enter_replication_controller(
    connection: str,
    repl_connection: str,
    workers: List[str],
    instance_id: str,
    run: bool,
    xrootd_manager: str,
    qserv_czar_db: str,
):
    """Entrypoint script for the entrypoint controller.

    Parameters
    ----------
    connection : `str`
        The connection string for the replication manager database for the
        administrative (typically root) user.
    repl_connection : `str`
        The connection string for the replication manager database for the
        non-admin user (created using the `connection`), the user is typically
        "qsreplica".
    workers : `list` [`str`]
        A list of parameters for each worker in the format "key=value,..."
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

    def set_initial_configuartion(workers, xrootd_manager):
        """Add the initial configuration to the replication database.
        Should only be called if the replication database has newly been smigged to version 1."""
        args = [
            "qserv-replica-config",
            "UPDATE_GENERAL",
            f"--config={repl_connection}",
            f"--xrootd.host={xrootd_manager}",
        ]
        _log.debug(f"Calling {' '.join(args)}")
        _run(args, run=run, check_returncode=True)

        workers = [dict(item.split("=") for item in worker.split(",")) for worker in workers]
        for worker in workers:
            try:
                name = worker.pop("name")
                host = worker.pop("host")
            except KeyError as e:
                raise RuntimeError("The worker option must contain entries 'name' and 'host'") from e
            args = [
                "qserv-replica-config",
                "ADD_WORKER",
                f"--config={repl_connection}",
                name,
                host,
            ]
            args += [f"--{key}={val}" for key, val in worker.items()]
            _log.debug(f"Calling {' '.join(args)}")
            _run(args, run=run, check_returncode=True)
        _log.info(f"Finished setting initial configuration {workers}")

    # The replication controller depends on this folder existing and does not create it if it's missing.
    # It should get fixed in DM-30074. For now we create it here.
    os.makedirs("/qserv/data/ingest", exist_ok=True)

    if run:
        smig_replication_controller(
            connection,
            update=False,
            set_initial_configuration=partial(set_initial_configuartion, workers, xrootd_manager),
        )

    env = dict(os.environ, LSST_LOG_CONFIG=replica_controller_log_path)

    args = [
        "qserv-replica-master-http",
        f"--config={repl_connection}",
        f"--instance-id={instance_id}",
        f"--qserv-czar-db={qserv_czar_db}",
    ]
    _log.debug(f"Calling {' '.join(args)}")
    sys.exit(_run(args, env=env, run=run))


def smig_update(czar_connection: str, worker_connections: List[str], repl_connection: str):
    """Update smig on nodes that need it.

    All connection strings are in format mysql://user:pass@host:port/database

    Parameters
    ----------
    czar_connection : `str`
        Connection string to the czar database.
    worker_connections : `list` [ `str` ]
        Connection strings to the worker databases.
    repl_ctrl_connection : `str`
        Connection string replication controller database.
    """
    if czar_connection:
        smig_czar(connection=czar_connection, update=True)
    if worker_connections:
        for c in worker_connections:
            smig_worker(connection=c, update=True)
    if repl_connection:
        smig_replication_controller(repl_connection, update=True)


def _run(
    args: List[Union[str, int]],
    env: Dict[str, str] = None,
    debug_port: Optional[int] = None,
    run: bool = True,
    check_returncode = False,
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
    if debug_port:
        args = ["gdbserver", f"localhost:{debug_port}"] + args
    if not run:
        print(" ".join(args))
        return 0
    result = subprocess.run(args, env=env, cwd="/home/qserv")
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
    chunk_location = repl.ingest_chunk_config(transaction_id, 0)
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


def integration_test(**kwargs):
    repl_connection = kwargs.pop("repl_connection", None)
    if repl_connection is not None:
        _do_smig_block(admin_smig_dir, "replica", kwargs["repl_connection"])
    return _integration_test.run_integration_tests(mysqld_user=mysqld_user_qserv, **kwargs)
