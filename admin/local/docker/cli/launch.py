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


import itertools
import jinja2
import os
from pathlib import Path
import subprocess
import sys
from tempfile import NamedTemporaryFile
import time
import yaml

import systemCfg


FILE_DIR = os.path.dirname(__file__)
QSERV_ROOT = os.path.abspath(os.path.join(FILE_DIR, "../../../../"))

mysqlRootPwd = "CHANGEME"
wmgr_secret_vol_name = "secret-wmgr-qserv"
config_wmgr_start = "config-wmgr-start"

# all container and volume names should have this format:
# qserv_{node_type}_{idx}_{description}

qserv_user_volume = "qserv-user-volume"
data_volume_name = "qserv_{name}_data"
manager_repl_data_volume_name = "qserv_manager_repl_data"

worker_xrootd_cfg_volume_name = "qserv_{name}_xrootd_cfg"
worker_repl_cfg_volume_name = "qserv_{name}_repl_cfg"
czar_cfg_volume_name = "qserv_{name}_cfg"

# This is the standard location that data volumes are located inside
# containers.
data_volume_target = "/qserv/data"

mariadb_container = "mariadb"
manager_xrootd_container_name = "qserv_manager_{idx}_xrootd"
manager_cmsd_container_name = "qserv_manager_{idx}_cmsd"
czar_proxy_container_name = "qserv_{name}_proxy"
mariadb_contaienr_name = "qserv_{name}_mariadb"
cmsd_container_name = "qserv_{name}_cmsd"
xrootd_container_name = "qserv_{name}_xrootd"
repl_container_name = "qserv_{name}_repl"
dashboard_container_name = "qserv_{name}"
manager_repl_mariadb_container_name = "qserv_manager_repl_mariadb"
manager_repl_controller_container_name = "qserv_manager_repl_controller"

qserv_build = "qserv_build"

worker_vnid_name = "worker_{idx}"

manager_repl_my_cnf_src = "src/admin/templates/repl-db/etc/my.cnf"
czar_my_cnf_src = "src/admin/templates/mariadb/etc/my.cnf"
worker_my_cnf_src = "src/admin/templates/mariadb/etc/my.cnf"

dashboard_nginx_conf_src = "src/admin/templates/dashboard/etc/nginx.conf.jinja"

def _do_not_run(args):
    """Modify args to not run the entrypoint script when calling docker run.

    - Removes "entrypoint" and all the following arguments from args.
    - Adds "-it" to the `docker run` arguments.

    Parameters
    ----------
    args : `list` [`Any`]
        The arguments that will be passed to `subprocess.run`

    Returns
    -------
    args, entrypoint : `list` [`list` [`Any`], `list` [`Any`]]
        args is the modified arguments to be passed to `subprocess run`,
        entrypoint is the command that would be executed in the container but
        has been removed.
    """
    location = args.index("entrypoint")
    entrypoint = args[location:]
    del args[location:]
    args.insert(args.index("run") + 1, "-it")
    return args, entrypoint


def _run_unconfined(args):
    """Modify args to run the container unconfined.

    Parameters
    ----------
    args : `list` [`Any`]
        The arguments that will be passed to `subprocess.run`

    Returns
    -------
    args : `list` [`Any`]
        The modified arguments to be passed to `subprocess run`.
    """
    args.insert(args.index("run") + 1, "seccomp=unconfined")
    args.insert(args.index("run") + 1, "--security-opt")
    args.insert(args.index("run") + 1, "--privileged")
    return args


def _use_dev_container_python(args):
    """Modify args to use the lsst python files in the build/install folder in
    dev container, instead of using the python files in the container.

    This is useful for iterative python development.

    Parameters
    ----------
    args : `list` [`Any`]
        The arguments that will be passed to `subprocess.run`

    Returns
    -------
    args : `list` [`Any`]
        The modified arguments to be passed to `subprocess run`.
    """
    location = args.index("run") + 1
    for i in reversed([
        "--mount",
        f"type=volume,source={qserv_user_volume},destination=/home/qserv/src,readonly",
        "--env",
        # (don't forget, this is the path in the container)
        "PYTHONPATH=/home/qserv/src/code/qserv/build/install/python",
    ]):
        args.insert(location, i)
    return args


def _add_log_level(args, log_level):
    """Add the log level argument immediately after the entrypoint argument.

    Parameters
    ----------
    args : `list` [`Any`]
        The arguments that will be passed to `subprocess.run`
    log_level : TODO
        The log level to add

    Returns
    -------
    args : `list` [`Any`]
        The modified arguments to be passed to `subprocess run`.
    """
    location = args.index("entrypoint") + 1
    args.insert(location, log_level)
    args.insert(location, "--log-level")
    return args


def _db_port_for(node_type, idx):
    """Get a database port number for a node by its type and possibly its id
    index.
    """
    raise NotImplementedError("this is being deprecated, for the cfg file.")

    # # todo:
    # # 3306 is hard coded in the my.cnf file, it should be fetched from somehwere
    # if node_type == "czar":
    #     if idx > 0:
    #         raise NotImplementedError("Currently only support one czar.")
    #     return czar_mysql_port_base
    # elif node_type == "worker":
    #     return worker_mysql_port_base + idx
    # elif node_type == "master-repl":
    #     if idx is not None:
    #         raise NotImplementedError("idx offset is not implemented for 'master-repl'.")
    #     return repl_mysql_port
    # else:
    #     raise RuntimeError(f"node_type not supported: {node_type}")


def _check_result(result, fail=True):
    """Check the result of a subproces.run call and if there is an error print
    it to stderr.

    Parameters
    ----------
    result : `subprocess.CompletedProcess`
        Result from a call to `subprocess.run`
    fail : `bool`
        If True, call `result.check_returncode` and allow it to raise if there
        is an error code.
    """
    if result.returncode:
        err = result.stderr.decode("utf-8")
        for line in err.split("\n"):
            print(line, sys.stderr)
    if fail:
        result.check_returncode()  # throw if there is an error code.


def decodeDockerOutput(output):
    output = output.decode("utf-8").strip()
    output = output.split("\n")
    return [o for o in output if o]


def getContainerName(containerId):
    """Use the container id to get the container name.

    Parameters
    ----------
    containerId : `str`
        The id of the container.

    Returns
    -------
    name : `str`
        The name of the container.
    """
    # Get the name of the container using the container id:
    result = subprocess.run(
        ["docker", "inspect", containerId, "--format", "{{.Name}}"],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
    return result.stdout.decode("utf-8").strip().lstrip("/")


def _get_qserv_containers(name_like):
    """Get a list of running container names that match a given string.
    """
    args = ["docker", "container", "ls", "-aq", "--filter", f"name={name_like}", "--format", "{{.Names}}"]
    result = subprocess.run(args, stdout=subprocess.PIPE)
    result.check_returncode()
    return [i for i in result.stdout.decode("utf-8").strip().split("\n") if i]


def _get_qserv_volumes(name_like):
    """Get a list of existing volume names that match a given string.
    """
    args = ["docker", "volume", "ls", "-q", "--filter", f"name={name_like}"]
    result = subprocess.run(args, stdout=subprocess.PIPE)
    result.check_returncode()
    return [i for i in result.stdout.decode("utf-8").strip().split("\n") if i]


def rm_all(dry_run):
    rm_container(("all",), dry_run=dry_run)
    rm_volume(("all",), dry_run=dry_run)


def rm_container(name, dry_run):
    if name == ("all",):
        name = [""]
    for n in name:
        if n.startswith("^qserv"):
            pass
        elif n.startswith("qserv"):
            n = f"^{n}"
        else:
            n = f"^qserv_{n}"
        remove_containers = _get_qserv_containers(n)
        if not remove_containers:
            print(f"No containers found, nothing {'to remove' if dry_run else 'removed'}.")
            return
        if dry_run:
            print(f"These volumes would be removed: {', '.join(remove_containers)}.")
            return
        args = ["docker", "rm", "-f"] + remove_containers
        result = subprocess.run(args, stdout=subprocess.PIPE)
        result.check_returncode()
        print(f"Removed containers: {remove_containers}")


def rm_volume(name, dry_run):
    if name == ("all",):
        name = [""]
    for n in name:
        if n.startswith("^qserv_"):
            pass
        elif n.startswith("qserv_"):
            n = f"^{n}"
        else:
            n = f"^qserv_{n}"
        remove_volumes = _get_qserv_volumes(n)
        if not remove_volumes:
            print(f"No volumes found, nothing {'to remove' if dry_run else 'removed'}.")
            return
        if dry_run:
            print(f"These volumes would be removed: {', '.join(remove_volumes)}.")
            return
        args = ["docker", "volume", "rm"] + remove_volumes
        result = subprocess.run(args, stdout=subprocess.PIPE)
        result.check_returncode()
        print(f"Removed volumes: {remove_volumes}")


def logs(name, separator):
    for n in name:
        containers = _get_qserv_containers(n)
        if not containers:
            print("No logs found.")
            return
        for container in containers:
            print(separator or f"\n*** {container} ***\n")
            args = ["docker", "logs", container]
            result = subprocess.run(args)
            result.check_returncode()


def start_mariadb(container_name, data_volume, port, run, my_cnf_src):
    """Launch a mariadb-scisql container.

    Mounts the appropriate my.cnf file into the container.

    Parameters
    ----------
    node_type : `str`
        "worker", "czar", or "master-repl"
    run : `bool`
        TODO
    """
    cnfPath = os.path.abspath(
        os.path.join(QSERV_ROOT, my_cnf_src)
    )

    args = [
        "docker",
        "run",
        "-d",
        "-e",
        f"MYSQL_ROOT_PASSWORD={mysqlRootPwd}",
        "--mount",
        f"type=bind,source={cnfPath},target=/etc/mysql/my.cnf,readonly",
        "--mount",
        f"type=volume,source={data_volume},target={data_volume_target}",
        "--net=host",
        "--name",
        container_name,
        "--hostname",
        container_name,
        "qserv/lite-mariadb",
        "--port",
        port,
    ]
    _run_non_qserv(args, run=run)


def start_czar_proxy(run, python_dev, debug, log_level, czar_cfg):
    """Start the czar/proxy container.

    Parameters
    ----------
    dbHost : `str`
        The host ip address or name of the mariadb instance hosting Czar
        databases.
    dbPort : `str`
        The port number of the mariadb instance hosting Czar databases.
    log_level : TODO
        The log level to use.
    """
    mysql_host = "localhost"
    container_name = czar_proxy_container_name.format(name=czar_cfg["vnid"])
    data_volume = data_volume_name.format(name=czar_cfg["vnid"])
    cfg_volume_name = czar_cfg_volume_name.format(name=czar_cfg["vnid"])
    args = [
        "docker",
        "run",
        "-d",
        "--net=host",
        "--name",
        container_name,
        "--hostname",
        container_name,
        # "--mount", # I think this is only ever needed for python debugging
        # f"type=volume,source={qserv_user_volume},destination=/home/qserv,readonly",
        "--mount",
        f"type=volume,source={data_volume},target={data_volume_target}",
        "--mount",
        f"type=volume,source={cfg_volume_name},destination=/config-etc",
        "qserv/lite-qserv",
        "entrypoint",
        "proxy",
        "--connection",
        f"root:{mysqlRootPwd}@{mysql_host}:{str(czar_cfg['mysql_port'])}",
        "--mysql-user-qserv",
        czar_cfg["mysql_user_qserv"],
    ]
    _run(args=args, python_dev=python_dev, debug=debug, run=run, log_level=log_level)


def start(cfg, log_level):
    start_manager(idx=0, container=None, run=True, log_level=log_level)
    start_czar(idx=0, container=None, run=True, python_dev=False, debug=False, cfg=cfg, log_level=log_level)
    start_worker(idx=None, container=None, run=True, python_dev=False, debug=False, cfg=cfg, log_level=log_level)
    start_manager_repl(container=None, run=True, python_dev=False, debug=False, cfg=cfg, log_level=log_level)
    start_dashboard(cfg=cfg, run=True)


def start_worker(idx, run, python_dev, debug, log_level, container, cfg):
    """Start a collection of worker containers, same as what runs in a worker
    pod in kubernetes.
    """
    if idx is None:
        workers = cfg["worker"]
    else:
        workers = [cfg["worker"][idx]]
    for worker_cfg in workers:
        if not container or "cmsd" in container:
            start_worker_cmsd(
                run=run, python_dev=python_dev, debug=debug, log_level=log_level, cfg=cfg,
                worker_cfg=worker_cfg
            )
        if not container or "xrootd" in container:
            start_worker_xrootd(
                run=run, python_dev=python_dev, debug=debug, log_level=log_level, worker_cfg=worker_cfg
            )
        if not container or "mariadb" in container:
            start_mariadb(
                run=run,
                container_name=mariadb_contaienr_name.format(name=worker_cfg["vnid"]),
                data_volume=data_volume_name.format(name=worker_cfg["vnid"]),
                port=str(worker_cfg["mysql_port"]),
                my_cnf_src=worker_my_cnf_src,
            )
        if not container or "repl" in container:
            start_worker_repl(
                run=run, python_dev=python_dev, debug=debug, log_level=log_level, cfg=cfg,
                worker_cfg=worker_cfg
            )



def start_czar(idx, run, python_dev, debug, container, log_level, cfg):
    """Start a collection of czar containers, same as what runs in a czar pod
    in kubernetes.
    """
    if idx is None:
        czars = cfg["czar"]
    else:
        czars = [cfg["czar"][idx]]
    for czar_cfg in czars:
        if not container or "mariadb" in container:
            start_mariadb(
                run=run,
                container_name=mariadb_contaienr_name.format(name=czar_cfg["vnid"]),
                data_volume=data_volume_name.format(name=czar_cfg["vnid"]),
                port=str(czar_cfg["mysql_port"]),
                my_cnf_src=czar_my_cnf_src,
            )
        if not container or "proxy" in container:
            start_czar_proxy(
                run=run,
                python_dev=python_dev,
                debug=debug,
                log_level=log_level,
                czar_cfg=czar_cfg,
            )


def start_manager(idx, container, log_level, run):
    if not container or "xrootd" in container:
        start_manager_xrootd(idx, log_level=log_level, run=run)
    if not container or "cmsd" in container:
        start_manager_cmsd(idx, log_level, run=run)



def _start_manager_repl_controller(run, debug, log_level, python_dev, cfg):
    dbHost = "localhost"
    dbPort = cfg["repl_manager"]["mysql_port"]
    args = [
        "docker",
        "run",
        "-d",
        "--net=host",
        "--name",
        manager_repl_controller_container_name,
        "--hostname",
        manager_repl_controller_container_name,
        "qserv/lite-qserv",
        "entrypoint",
        "replication-controller",
        "--connection",
        f"root:{mysqlRootPwd}@{dbHost}:{dbPort}",
        "--repl-connection",
        f"qsreplica@{dbHost}:{dbPort}",
        "--instance-id",
        cfg["instance_id"],
        "--qserv-db-pswd",
        cfg["repl_manager"]["mysql_root_password"],
    ]
    for worker in cfg["worker"]:
        args.append("--worker")
        args.append(",".join([f"{k}={v}" for k, v in worker.items()]))

    _run(args, log_level=log_level, run=run, python_dev=python_dev)


def start_manager_repl(container, run, debug, python_dev, cfg, log_level):
    if not container or "mariadb" in container:
        start_mariadb(
            run=run,
            container_name=manager_repl_mariadb_container_name,
            data_volume=manager_repl_data_volume_name,
            port=str(cfg["repl_manager"]["mysql_port"]),
            my_cnf_src=manager_repl_my_cnf_src,
        )
    if not container or "controller" in container:
        _start_manager_repl_controller(
            run=run, debug=debug, log_level=log_level, python_dev=python_dev, cfg=cfg
        )


def start_manager_xrootd(idx, log_level, run):
    container_name = manager_xrootd_container_name.format(idx=idx)
    args = [
        "docker",
        "run",
        "-d",
        "--net=host",
        "--name",
        container_name,
        "--hostname",
        container_name,
        "qserv/lite-qserv",
        "entrypoint",
        "xrootd-manager",
    ]
    _run(args, log_level=log_level, run=run)


def start_manager_cmsd(idx, log_level, run):
    container_name = manager_cmsd_container_name.format(idx=idx)
    args = [
        "docker",
        "run",
        "-d",
        "--net=host",
        "--name",
        container_name,
        "--hostname",
        container_name,
        "qserv/lite-qserv",
        "entrypoint",
        "cmsd-manager",
    ]
    _run(args, log_level=log_level, run=run)


def start_worker_cmsd(run, python_dev, debug, log_level, cfg, worker_cfg):
    """Start the cmsd app for a worker.

    Parameters
    ----------
    run : `bool`
        Do/don't run the entrypoint script.
    python_dev : `bool`
        Do/don't mount the python files in the development container.
    debug : `bool`
        Run the container in debug mode.
    """
    container_name = cmsd_container_name.format(name=worker_cfg["vnid"])
    xrootd_volume_name = worker_xrootd_cfg_volume_name.format(name=worker_cfg["vnid"])
    data_volume = data_volume_name.format(name=worker_cfg["vnid"])
    args = [
        "docker",
        "run",
        # insert any new commands after this line.
        "-d",
        "--net=host",
        "--mount",
        f"type=volume,source={xrootd_volume_name},destination=/config-etc",
        "--mount", # I think this mount is needed for cmsd config. TODO add a comment why.
        "type=tmpfs,destination=/home/qserv/worker",
        "--mount",
        f"type=volume,source={data_volume},target={data_volume_target}",
        "--name",
        container_name,
        "--hostname",
        container_name,
        "qserv/lite-qserv",
        "entrypoint",
        "worker-cmsd",
        "--db-user",
        "root",
        "--db-pswd",
        "CHANGEME",
        "--db-host",
        "localhost",
        "--db-port",
        str(worker_cfg["mysql_port"]),
        "--xrd-port",
        str(worker_cfg["xrd_port"]),
        "--vnid",
        worker_cfg["vnid"],
        "--repl-ctl-dn",
        cfg["repl_manager"]["domain_name"],
        "--mysql-monitor-password",
        cfg["monitor"]["password"],
    ]
    _run(args=args, python_dev=python_dev, debug=debug, run=run, log_level=log_level)


def start_worker_xrootd(run, python_dev, debug, log_level, worker_cfg):
    """Start the xrootd app for a worker.

    Parameters
    ----------
    run : `bool`
        Do/don't run the entrypoint script.
    python_dev : `bool`
        Do/don't mount the python files in the development container.
    debug : `bool`
        Run the container in debug mode.
    """
    container_name = xrootd_container_name.format(name=worker_cfg["vnid"])
    xrood_volume_name = worker_xrootd_cfg_volume_name.format(name=worker_cfg["vnid"])
    data_volume = data_volume_name.format(name=worker_cfg["vnid"])
    args = [
        "docker",
        "run",
        # insert any new commands after this line.
        "-d",
        "--net=host",
        "--mount",
        f"type=volume,source={xrood_volume_name},destination=/config-etc",
        "--mount",
        "type=tmpfs,destination=/home/qserv/worker",
        "--mount",
        f"type=volume,source={data_volume},target={data_volume_target}",
        "--name",
        container_name,
        "--hostname",
        container_name,
        "qserv/lite-qserv",
        "entrypoint",
        "worker-xrootd",
        "--xrd-port",
        str(worker_cfg["xrd_port"]),
        "--db-port",
        str(worker_cfg["mysql_port"]),
        "--vnid",
        worker_cfg["vnid"],

    ]
    _run(args=args, python_dev=python_dev, debug=debug, run=run, log_level=log_level)


def start_worker_repl(run, python_dev, debug, log_level, cfg, worker_cfg):
    container_name = repl_container_name.format(name=worker_cfg["vnid"])
    repl_conn_host = "localhost"
    repl_conn_port = cfg["repl_manager"]["mysql_port"]
    repl_conn_user = "qsreplica"
    repl_conn_pw = ""
    repl_db_name = "qservReplica"
    repl_conn = \
        f"mysql://{repl_conn_user}:{repl_conn_pw}@{repl_conn_host}:{repl_conn_port}/{repl_db_name}"
    container_name = repl_container_name.format(name=worker_cfg["vnid"])
    xrood_volume_name = worker_xrootd_cfg_volume_name.format(name=worker_cfg["vnid"])
    data_volume = data_volume_name.format(name=worker_cfg["vnid"])
    repl_cfg_volume = worker_repl_cfg_volume_name.format(name=worker_cfg["vnid"])
    args = [
        "docker",
        "run",
        "-d",
        "--net=host",
        "--mount",
        f"type=volume,source={repl_cfg_volume},destination=/config-etc",
        "--mount",
        f"type=volume,source={data_volume},target={data_volume_target}",
        "--name",
        container_name,
        "--hostname",
        container_name,
        "qserv/lite-qserv",
        "entrypoint",
        "worker-repl",
        "--repl-connection",
        #f"mysql://qsreplica@{repl_connection_host}:{repl_connection_port}",
        repl_conn,
        "--qserv-db-pswd",
        cfg["repl_manager"]["mysql_root_password"],
        "--vnid",
        worker_cfg["vnid"],
        "--instance-id",
        cfg["instance_id"],
    ]
    _run(args=args, debug=debug, run=run, log_level=log_level, python_dev=python_dev)


def start_qserv_build_container():
    """Start the qserv build container.

    Returns
    -------
    containerName : `str`
        The name of the container that was started.
    """

    result = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--init",
            "-v",
            f"{qserv_user_volume}:/home/qserv",
            "-v",
            f"root:/root",
            "-v",
            "/var/run/docker.sock:/var/run/docker.sock",
            "-u",
            "qserv",
            "--name",
            qserv_build,
            "qserv-build",
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    result.check_returncode()
    containerId = result.stdout.decode("utf-8").strip()
    return getContainerName(containerId)


def start_dashboard(cfg, run):
    """Start the qserv nginx dashboard container.

    Parameters
    ----------
    cfg : `dict`
        Key value pairs that describe the desired system configuration.
    run : `bool`
        If false, prints the arguments and exits, does not launch any container.
    """
    with open(os.path.join(QSERV_ROOT, dashboard_nginx_conf_src)) as f:
        conf_template = f.read()

    template = jinja2.Template(
        conf_template,
        undefined=jinja2.StrictUndefined,
    )
    try:
        conf_rendered = template.render(cfg)
    except jinja2.exceptions.UndefinedError as e:
        raise RuntimeError(f"Missing template value: {str(e)}")

    nginx_conf_file = os.path.join(Path.home(), ".qserv_dashboard_nginx_config.conf")
    with open(nginx_conf_file, "w") as f:
        f.write(conf_rendered)

    container_name = dashboard_container_name.format(name=cfg["dashboard"]["vnid"])
    conf_target = "/etc/nginx/nginx.conf"  # location specified by the container for conf files.
    args = [
        "docker",
        "run",
        "-d",
        "-p"
        "8081:80",
        "--name",
        container_name,
        "--mount",
        f"source={nginx_conf_file},target={conf_target},type=bind,readonly",
        "qserv/lite-nginx",
    ]
    _run_non_qserv(args, run=run)


def _run(args, python_dev=False, debug=False, run=True, log_level=None):
    """Run an entrypoint script in a docker container in a subprocess.

    `args` is expected to start with ["docker", "run"] follwed by docker
    arguments and then "entrypoint" followed by entrypoint arguments.

    The args are modified according to other arguments to this function.

    Parameters
    ----------
    args : `list` [`str`]
        The arguments to run the container and entrypoint command.
    python_dev : `bool`
        If True, mounts the development container and changes the PYTHONPATH
        environment variable in the container to use the the python files in
        the "install" folder in the development container.
    debug : `bool`
        If True, passes `--debug` to the entrypoint command. This causes
        entrypoint to launch `gdbserver` (instead of the qserv executable) and
        passes the executable and arguments as arguments to `gdbserver`.
    run : `bool`
        If False, strips "entrypoint" and everything after it from `args`, so
        the container is launched but "entrypoint" is not called. `-it` is
        added to the `docker run` arguments so that it stays running. The
        entrypoint command and all the following arguments that would have
        been called are printed, making it easy to `docker exec` the container
        and to run the entrypoint command manually.
    log_level : TODO
        The log level to use
    entrypoint : `bool`
        True if the container runs a qserv entrypoint script.
    """
    if python_dev:
        args = _use_dev_container_python(args)
    if debug:
        args = _run_unconfined(args)
        args.append("--debug")
        args.append(str(debug))
    if log_level:
        args = _add_log_level(args, log_level)
    # Process --no-run last so we can print the whole entrypoint command.
    if not run:
        args, entrypoint = _do_not_run(args)
        print(" ".join(str(arg) for arg in args))
        print(" ".join(str(e) for e in entrypoint))
        return

    result = subprocess.run(
        args,
        stderr=subprocess.STDOUT,
        stdout=sys.stdout,
    )
    if result.returncode:
        print(f"Error running command: {' '.join(args)}", sys.stderr)
    result.check_returncode()


def _run_non_qserv(args, run):
    """Run a container other than the qserv container.

    Parameters
    ----------
    args : `list` [`str`]
        The arguments to run the container and entrypoint command.
    run : `bool`
        If false, prints the arguments and exits, does not launch any container.
    """
    if not run:
        print(" ".join(args))
        return

    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        result.check_returncode()
    except subprocess.CalledProcessError as e:
        raise RuntimeError(result.stdout + result.stderr) from e
