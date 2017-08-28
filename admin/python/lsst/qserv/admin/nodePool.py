# LSST Data Management System
# Copyright 2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
Module defining NodePool class and related methods.

NodePool is a class which allow to launch parallel ssh commands to
a set of NodeAdmin instances. Each NodeAdmin instance can be a Qserv
master or worker node.

It can be used to install Qserv on a cluster.

@author  Fabrice Jammes, IN2P3
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging
import multiprocessing
import os
from subprocess import Popen
import time

# -----------------------------
# Imports for other modules --
# -----------------------------

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------
_LOG = logging.getLogger(__name__)


def cpu_count():
    ''' Returns the number of CPUs in the system
    '''
    try:
        return multiprocessing.cpu_count()
    except NotImplementedError:
        return 1


class NodePool(object):
    """
    Class providing tools to send ssh command to a set of machines.
    Use system ssh.
    """

    def __init__(self, sshNodes, max_task=None):
        """
        Container of nodes which will be managed in parallel using ssh

        @param sshNodes:    a list of SSHCommand instances
        @param max_task:    number of parallel SSH command which will be run
                            concurrently, equal to the number of cpu of the local
                            machine by default
        """

        self.nodes = sshNodes

        if max_task:
            self._max_task = max_task
        else:
            self._max_task = cpu_count()

    def execParallel(self, command, stdin=os.devnull):
        """
        Execute one SSH command in parallel on a set of nodes;
        fork one process for each node.
        Log of each command are stored in $CWD/parallel-cmd-log

        @param command: command to run
        @param stdin: file redirect to SSH client process standard input

        @return the number of nodes in error
        """

        if not command or not self.nodes:
            _LOG.info("No parallel execution launched")
            return 0

        remaining_nodes = list(self.nodes)

        logdir = "parallel-cmd-log"
        try:
            os.makedirs(logdir)
        except OSError:
            if not os.path.isdir(logdir) or not os.access(logdir, os.W_OK):
                raise

        def logfile(name):
            f = "{0}-{1}-{2}.txt".format(node.host, process_id, name)
            return os.path.join(logdir, f)

        def done(p):
            return p.poll() is not None

        def success(p):
            return p.returncode == 0

        _LOG.info("nb node %r, max task: %r", len(remaining_nodes), self._max_task)
        running_processes = []
        process_id = 0
        nodes_failed = []

        _LOG.info("Run command %r, stdin redirected to: %r", command, stdin)

        while running_processes or remaining_nodes:

            # batch running_processes
            while remaining_nodes and len(running_processes) < self._max_task:
                process_id += 1
                node = remaining_nodes.pop()

                task = node.getCommand(command)
                if _LOG.isEnabledFor(logging.DEBUG):
                    _LOG.debug("Run: %r", ' '.join(task))

                with open(stdin) as _in, open(logfile('stdout'), "wb") as _out, open(logfile('stderr'), "wb") as _err:
                    running_processes.append(
                        [Popen(task, stdin=_in, stdout=_out, stderr=_err), node.host, process_id])

            time.sleep(0.05)

            # check for ended running_processes
            for p in running_processes:
                process, host, p_id = p
                if done(process):
                    running_processes.remove(p)
                    if success(process):
                        _LOG.info("Success on %r (#%r)", host, p_id)
                    else:
                        _LOG.error("Failure on %r (#%r)", host, p_id)
                        nodes_failed.append((host, p_id))

        if nodes_failed:
            nb_failed = len(nodes_failed)
            _LOG.error("%r failure(r): %r", nb_failed, nodes_failed)
            return nb_failed
        else:
            _LOG.info("Success on all hosts")
            return 0
