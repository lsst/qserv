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
import flask
import logging
import mysql.connector
import os
import socket
import subprocess
import sys
from urllib.parse import urlparse

from lsst.qserv.admin.backoff import max_backoff_seconds, on_smig_backoff
from lsst.qserv.admin.template import apply_template_cfg, apply_template_cfg_file, save_template_cfg
from lsst.qserv.replica import replicaConfig
from lsst.qserv.schema import smig

smig_dir_env_var = "QSERV_SMIG_DIRECTORY"
default_smig_dir = "/usr/local/qserv/smig"
admin_smig_dir = "admin/schema"
css_smig_dir = "css/schema"
rproc_smig_dir = "rproc/schema"
qmeta_smig_dir = "qmeta/schema"
worker_smig_dir = "worker/schema"
replication_controller_smig_dir = "replica/schema"

replica_controller_cfg_path = "/config-etc/replicaConfig.sql"
replica_controller_log_template = "/usr/local/qserv/templates/repl-ctl/etc/log4cxx.replication.properties.jinja"
replica_controller_log_path = "/config-etc/log4cxx.replication.properties"

nginx_controller_cfg_template = "/usr/local/qserv/templates/dashboard/etc/nginx.conf.jinja"
nginx_controller_cfg_path = "/config-etc/nginx.conf"

mysqld_socket = "/qserv/data/mysql/mysql.sock" # I think this can be deleted; no longer used.

mysqld_user_qserv = "qsmaster"

proxy_empty_chunk_path = "/qserv/data/qserv"

czar_proxy_config_template = "/usr/local/qserv/templates/proxy/etc/my-proxy.cnf"
czar_proxy_config_path = "/config-etc/my-proxy.cnf"
czar_config_template = "/usr/local/qserv/templates/proxy/etc/qserv-czar.cnf.jinja"
czar_config_path  = "/config-etc/qserv-czar.cnf"
worker_wgmr_config_path = "/config-etc/wmgr.cnf"

cmsd_manager_cfg_template = "/usr/local/qserv/templates/xrootd/etc/cmsd-manager.cf.jinja"
cmsd_manager_cfg_path = "/config-etc/cmsd-manager.cnf"

xrootd_manager_cfg_template = "/usr/local/qserv/templates/xrootd/etc/xrootd-manager.cf.jinja"
xrootd_manager_cfg_path = "/config-etc/xrootd-manager.cf"

_log = logging.getLogger(__name__)


def _do_smig(module_smig_dir, module, connection):
    smig(
        verbose=False,
        do_migrate=True,
        check=False,
        final=None,
        scripts=os.path.join(os.environ.get(smig_dir_env_var, default_smig_dir), module_smig_dir),
        connection=connection,
        config_file=None,  # could use this instead of connection string
        config_section=None,  # goes with config_file
        module=module,
    )


@backoff.on_exception(
    backoff.expo,
    mysql.connector.errors.DatabaseError,
    max_time=max_backoff_seconds,
    on_backoff=on_smig_backoff(log=_log),
    # Do give up, unless error is that the connection was refused (assume db is starting up)
    #giveup=lambda e: ,
)
def _get_vnid(connection):
    """For workers; get the virtual network id from the database."""
    parsed = urlparse(connection)
    with closing(mysql.connector.connect(
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port,
    )) as connection:
        with closing(connection.cursor()) as cursor:
            cursor.execute("SELECT id FROM qservw_worker.Id;")
            res = list(cursor.fetchone())
            print(f"res:{res}")
            if len(res) != 1:
                raise RuntimeError("Error getting id from qservw_worker database.")
            if res[0] == "null":
                raise RuntimeError("VNID is null in database.")
            return res[0]


def _wait_for_mysql_running():
    """Wait for mysql to be running."""
    pass
    # TODO we don't have mysql installed on worker nodes, we have to use
    # mysql connection lib. This can be rewritten to use that.
    # However, is that the right way to go? Or do we want to just make our
    # python to db calls robust against a not present database (retry?)


    # while True:
    #     result = subprocess.run(
    #         [
    #             "mysql",
    #             "--socket",
    #             f"\"{mysqld_socket}\"",
    #             f"--user=\"{mysqld_user_qserv}\"",
    #             "--skip-column-names",
    #             "-e",
    #             "SELECT CONCAT('Mariadb is up: ', version())",
    #         ]
    #     )
    #     if result.returncode == 0:
    #         break
    #     else:
    #         _log.info("Waiting for MySQL startup.")



def smig_czar(connection):
    """Apply schema migration scripts to czar modules.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database.
    """
    for module_smig_dir, module in (
        (admin_smig_dir, "admin"),
        (css_smig_dir, "css"),
        (rproc_smig_dir, "rproc"),
        (qmeta_smig_dir, "qmeta"),
    ):
        _do_smig(module_smig_dir, module, connection)


def smig_replication_controller(connection):
    """Apply schema migration scripts the replication controller.

    Parameters
    ----------
    connection : `str`
        Connection string in format mysql://user:pass@host:port/database.
    """
    _do_smig(replication_controller_smig_dir, "replica", connection)


def enter_cmsd_manager():
    # TODO xrootd_managers needs to be passed in by execution env or mounted in /config-etc
    save_template_cfg(dict(
        xrootd_managers=["localhost"],
        cmsd_manager="UNUSED",
    ))
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


def enter_xrootd_manager(cmsd_manager):
    save_template_cfg(dict(
        cmsd_manager=cmsd_manager,
    ))
    apply_template_cfg_file(xrootd_manager_cfg_template, xrootd_manager_cfg_path)
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


def enter_worker_cmsd(cmsd_manager, vnid, db_scheme, db_user, db_pswd, db_host, db_port, debug_port,
                      xrd_port, db_qserv_user, repl_ctl_dn, mysql_monitor_password):

    save_template_cfg(dict(
        host=socket.gethostname(), # nptodo is this used in setting VNID?
        cmsd_manager=cmsd_manager,
        db_scheme=db_scheme,
        db_user=db_user,
        db_pswd=db_pswd,
        db_host=db_host,
        db_port=db_port,
        vnid=vnid,
        mysqld_user_qserv=db_qserv_user,
        replication_controller_FQDN=repl_ctl_dn,
        mysql_monitor_password=mysql_monitor_password,
    ))

    connection = apply_template_cfg("{{ db_scheme }}://{{ db_user }}:{{ db_pswd }}@{{ db_host }}:{{ db_port }}")
    _log.debug(f"Worker database connection "
               f"{apply_template_cfg('{{ db_scheme }}://{{ db_user }}:*****@{{ db_host }}:{{ db_port }}')}")
    _do_smig(admin_smig_dir, "admin", connection),
    _do_smig(worker_smig_dir, "worker", connection),

    rendered_config_path = "/config-etc/cmsd-worker.cf"
    apply_template_cfg_file("/usr/local/qserv/templates/xrootd/etc/cmsd-worker.cf.jinja",
                            rendered_config_path)
    rendered_xrdssi_config_path = "/config-etc/xrdssi-worker.cf"
    apply_template_cfg_file("/usr/local/qserv/templates/xrootd/etc/xrdssi.cf.jinja",
                            rendered_xrdssi_config_path)
    args = [
        "cmsd",
        "-c",
        rendered_config_path,
        "-n",
        "worker",
        "-I",
        "v4",
        "-l",
        "@libXrdSsiLog.so",
        "-+xrdssi",
        rendered_xrdssi_config_path,
    ]
    sys.exit(_run(args, debug_port=debug_port))


def enter_worker_xrootd(debug_port, xrd_port, db_host, db_port, vnid, cmsd_manager):
    """Entrypoint script for the xrootd container that runs on worker nodes.
    """
    # enter_worker_cmsd smigs the worker db for that node

    # TODO This sets the amount of data that can be locked into memory to
    # almost the entire amount of memory on the machine. I think in a dev-env
    # (docker-only on a single machine) we don't want to grab quite so much
    # memory? TBD WTD here - do/don't set this? set it to how much? needs a way
    # to set env (dev/prod/etc)
    # # Increase limit for locked-in-memory size
    # MLOCK_AMOUNT=$(grep MemTotal /proc/meminfo | awk '{printf("%.0f\n", $2 - 1000000)}')
    # ulimit -l "$MLOCK_AMOUNT"

    save_template_cfg(dict(
        vnid=vnid,
        cmsd_manager=cmsd_manager,
        db_host=db_host,
        db_port=db_port,
    ))

    # TODO worker (and manager) xrootd+cmsd pair should "share" the cfg file
    # it's in different pods but should be same source & processing.
    # Rename these files to be more agnostic.
    rendered_config_path = "/config-etc/cmsd-worker.cf"
    apply_template_cfg_file("/usr/local/qserv/templates/xrootd/etc/cmsd-worker.cf.jinja",
                            rendered_config_path)
    rendered_xrdssi_config_path = "/config-etc/xrdssi-worker.cf"
    apply_template_cfg_file("/usr/local/qserv/templates/xrootd/etc/xrdssi.cf.jinja",
                            rendered_xrdssi_config_path)
    args = [
        "xrootd",
        "-c",
        rendered_config_path,
        "-n",
        "worker",
        "-I",
        "v4",
        "-l",
        "@libXrdSsiLog.so",
        "-+xrdssi",
        rendered_xrdssi_config_path,
    ]
    env = dict(os.environ, COMPONENT_NAME="worker")
    sys.exit(_run(args, debug_port=debug_port))


def enter_worker_repl(vnid, connection, qserv_db_pswd, debug_port, run, instance_id):
    args = [
        "qserv-replica-worker",
        vnid,
        f"--config={connection}",
        f"--qserv-db-password='{qserv_db_pswd}'",
        "--debug",
        f"--instance-id={instance_id}",
    ]
    _run(args, debug_port=debug_port, run=run)


def enter_proxy(db_scheme, connection, mysql_user_qserv, repl_ctl_dn, mysql_monitor_password, xrootd_manager):
    """Entrypoint script for the proxy container.
    """
    # TODO the empty chunk path should be defined in some default configuration
    # somewhere/somehow. TBD. Note that it must be created in the dockerfile
    # by the root user and chown'd to the qserv user.
    save_template_cfg(dict(
        mysqld_user_qserv=mysql_user_qserv,
        replication_controller_FQDN=repl_ctl_dn,
        mysql_monitor_password=mysql_monitor_password,
        empty_chunk_path="/qserv/data/qserv",
        xrootd_manager=xrootd_manager,
    ))
    connection = f"{db_scheme}://{connection}"
    smig_czar(connection)

    apply_template_cfg_file(czar_proxy_config_template, czar_proxy_config_path)
    apply_template_cfg_file(czar_config_template, czar_config_path)

    env = dict(os.environ, QSERV_CONF=czar_config_path)

    args = [
        "mysql-proxy",
        "--proxy-lua-script=/usr/local/lua/qserv/scripts/mysqlProxy.lua",
        "--lua-cpath=/usr/local/lua/qserv/lib/czarProxy.so",
    ]
    sys.exit(_run(args, env=env))


def enter_replication_controller(db_scheme, connection, repl_connection, workers,
                                 instance_id, run):
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
    """
    if run:
        smig_replication_controller(f"{db_scheme}://{connection}")

    workers = [dict(item.split("=") for item in worker.split(",")) for worker in workers]

    for worker in workers:
        try:
            name = worker.pop("name")
            host = worker.pop("host")
        except KeyError e:
            raise RuntimeError("The worker option must contain entries 'name' and 'host'") from e
        args = [
            "qserv-replica-config",
            "ADD_WORKER",
            name,
            host,
        ]
        args += [f"--{key}={val}" for key, val in worker.items()]
        _log.debug(f"Calling {' '.join(args)}")
        _run(args, run=run)

    env = dict(os.environ, LSST_LOG_CONFIG=replica_controller_log_path)

    args = [
        "qserv-replica-master-http",
        f"--config={repl_connection}",
        f"--instance-id={instance_id}",
    ]
    _log.debug(f"Calling {' '.join(args)}")
    _run(args, env=env, run=run)


def _run(args, env=None, debug_port=None, run=True):
    if debug_port:
        args = ["gdbserver", f"localhost:{debug_port}"] + args
    if not run:
        print(" ".join(args))
        return 0
    result = subprocess.run(
        args,
        stdout=sys.stdout,
        stderr=sys.stderr,
        env=env,
    )
    return result.returncode
