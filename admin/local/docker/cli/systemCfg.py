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


import os
import pathlib
import yaml


defaultCfgPath = os.path.join(pathlib.Path.home(), ".qserv_config.yaml")

# TODO maybe we don't need all these different 'base' ports? Maybe just a
# 'start' and an incrementor to use while the cfg is being written?
# We might also want a "skip" list option so if problems are encountered with
# used ports the file could be regenerated & automatically skip those ports.

repl_base_port = 25000
czar_mysql_base_port = 3306

worker_mysql_base_port = 33306
repl_mysql_port = 44406
repl_http_server_port = 25508

worker_vnid_name = "worker_{idx}"
czar_vnid_name = "czar_{idx}"
dashboard_vnid_name = "dashboard"

worker_xrd_base_port = 2131

dashboard_port = 2525

qserv_instance_id = "qserv_in_containers"
# czar_mysql_port_base = 3306
# worker_mysql_port_base = 33306
# repl_mysql_port = 44406


def make_config(cfg_file, num_workers):

    worker_vnids = [f"worker_{i}" for i in range(num_workers)]

    ports_per_worker = 5  # this is the number of ports assigned in the generator below.
    workers = [{
        # replication parameters (might want to be prefixed with 'repl_'?)
        "svc_port": i,
        "fs_port": i + 1,
        "loader_port": i + 2,
        "exporter_port": i + 3,
        "http_loader_port": i + 4}
        for i in range(repl_base_port, repl_base_port + len(worker_vnids) * ports_per_worker, ports_per_worker)]
    # import pdb; pdb.set_trace()
    for idx in range(num_workers):
        workers[idx]["vnid"] = worker_vnid_name.format(idx=idx)
        workers[idx]["mysql_port"] = worker_mysql_base_port + idx
        workers[idx]["xrd_port"] = worker_xrd_base_port + idx
        #workers[idx]["repl_mysql_port"] = TODO

    czars = [
        dict(
            mysql_port=czar_mysql_base_port,
            vnid=czar_vnid_name.format(idx=0),
            mysql_user_qserv="qsmaster",
        )
    ]

    repl_manager = dict(
        mysql_port=repl_mysql_port,
        # TODO how/when to inject the repl controller FQDN?
        domain_name="repl_mgr_domain_name",
        mysql_root_password = "CHANGEME",
        http_server_port=repl_http_server_port,
    )

    # for the monitoring application
    monitor = dict(
        password="CHANGEME_MONITOR",
    )

    dashboard = dict(
        port=dashboard_port,
        vnid=dashboard_vnid_name,
    )


    cfg = dict(
        instance_id=qserv_instance_id,
        worker=workers,
        czar=czars,
        repl_manager=repl_manager,
        monitor=monitor,
        dashboard=dashboard,
    )


    with open(cfg_file, "w") as f:
        f.write(yaml.dump(cfg))
