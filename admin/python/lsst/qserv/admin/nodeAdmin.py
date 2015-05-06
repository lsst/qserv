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
Module defining NodeAdmin class and related methods.

NodeAdmin is an interface which controls or communicates with qserv
worker instance. Exact form of control is still to be defined, for now
we are going to support communication with mysql instance or ability to
start/stop worker processes. Current implementation is based on SSH
tunneling (or direct mysql connection if possible), in the future there
is supposed to be a special service for worker administration.

NodeAdmin uses information about worker nodes defined in Qserv CSS, for
details see https://dev.lsstcorp.org/trac/wiki/db/Qserv/CSS#Node-related.
For testing purposes it it also possible to provide worker information as
a set of parameters to constructor.

@author  Andy Salnikov, SLAC
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.db.exception import produceExceptionClass
from lsst.db.db import Db
from lsst.qserv.admin.ssh import SSHCommand, SSHTunnel

#----------------------------------
# Local non-exported definitions --
#----------------------------------

_LOG = logging.getLogger(__name__)

_Exception = produceExceptionClass('WorkerAdminException', [
    (100, "ARG_ERR", "Missing or inconsistent arguments"),
    (105, "MYSQL_CONN_ERR", "Missing mysql connection info"),
    (110, "CSS_HOST_NAME_MISS", "Missing host name is in CSS"),
    (110, "MYSQL_CONN_FMT", "Unsupported format of mysql connection string"),
    ])

#------------------------
# Exported definitions --
#------------------------

class NodeAdmin(object):
    """
    Class representing administration/communication endpoint for qserv worker.
    """

    def __init__(self, name=None, qservAdmin=None, host=None, runDir=None,
                 mysqlConn=None, kerberos=False, ssh_user=None):
        """
        Make new endpoint for remote worker. Worker can be specified
        either by its name or by the complete set of parameters. If name
        is given then all other parameters are taken from CSS and qservAdmin
        has to be specified as well. If name is not given then all other
        parameters (except qservAdmin) need to be specified.

        This will throw if node with given name is not defined in CSS.

        @param name:        Name of the worker as defined in CSS.
        @param qservAdmin:  QservAdmin instance, required if name is provided.
        @param host:        worker host name, required if name is not provided.
        @param runDir:      qserv run directory on remote host, optional. Only
                            needed if name is not provided and methods that need
                            this parameter are called.
        @param mysqlConn:   comma-separated set of mysql connection options.
                            Optional, only needed if name is not provided and methods
                            that need this parameter are called.
        @param kerberos:    Use kerberos authentication (with ssh command only)
        @param ssh_user:    ssh user account used (with ssh command only)
        """

        if name:

            # need qservAdmin
            if not qservAdmin:
                raise ValueError('qservAdmin has to be specified if name is used')

            params = qservAdmin.getNode(name)
            self.host = params.get('host')
            if not self.host:
                raise _Exception(_Exception.CSS_HOST_NAME_MISS, "node=" + name)
            self.runDir = params.get('runDir')
            self.mysqlConnStr = params.get('mysqlConn')
            self._name = name

            self._kerberos = params.get('kerberos')
            self._ssh_user = params.get('ssh_user')

        else:

            if not host:
                raise _Exception(_Exception.ARG_ERR, 'either name or host has to be provided')

            self.host = host
            self.runDir = runDir
            if mysqlConn:
                self.mysqlConnStr = mysqlConn
                self._name = host + ':' + mysqlConn

            self._kerberos = kerberos
            self._ssh_user = ssh_user

    def name(self):
        """
        Returns worker name as defined in CSS, if instance was not made from
        CSS returns some arbitrary unique name.
        """
        return self._name

    def _mysqlTunnel(self):
        """
        Setup the tunnel for mysql connection if necessary.

        Returns tuple consisting of three elements:
        - host name for connection
        - port number for connection
        - tunnel object or None if tunnel is not needed, if tunnel is not None
          then this object controls lifetime of the actual SSH tunnel
        """

        if not self.mysqlConnStr:
            raise _Exception(_Exception.MYSQL_CONN_ERR)

        # connection string is defined in https://dev.lsstcorp.org/trac/wiki/db/Qserv/CSS#Node-related
        port = None
        tunnel = False
        for option in self.mysqlConnStr.split(','):
            option = option.strip()
            if option.isdigit():
                # must be port number, use direct connection
                port = int(option)
            elif option.startswith('lo:'):
                # port number on worker-local interface
                port = int(option[3:])
                tunnel = True
            else:
                raise _Exception(_Exception.MYSQL_CONN_FMT, repr(option))

        if tunnel:
            # start ssh tunnel
            host = '127.0.0.1'   # do not use 'localhost' here
            tunnel = SSHTunnel(self.host, 0, self.host, port, host)
            port = tunnel.port
        else:
            host = self.host
            tunnel = None

        return host, port, tunnel

    def mysqlConn(self, **kwargs):
        """
        Connect to worker mysql server, return instance of db.Db class.

        This method figures out best way to connect to remote mysql server
        using either direct TCP connection or starting SSH tunnel and using
        the tunnel for TCP connection.

        This method accepts the same set of keyword arguments as `db.Db`
        constuctor, some keywords (host, port) are ignored and will be
        provided based on information in this instance.
        """

        host, port, tunnel = self._mysqlTunnel()

        # start connection
        kw = kwargs.copy()
        kw['host'] = host
        kw['port'] = port
        db = Db(**kw)

        # we also need to control tunnel object lifetime as tunnel will
        # be closed when this object is destroyed
        if tunnel is not None:
            db._tunnel_lifetime_ref_ = tunnel

        return db

    def _createSSHCommand(self):
        cmd = SSHCommand(self.host, self.runDir, self._kerberos, self._ssh_user)
        return cmd

    def execCommand(self, command, capture=False):
        """
        Execute command on worker host.

        Command argument is a string which will be passed to the shell
        running on worker host. Before executing the command working
        directory will be set to the run directory of the corresponding
        qserv instance. To start qserv instance for example one can pass
        "bin/qserv-start.sh" as an argument.

        If ssh or remote command fails an exception will be raised.

        @param command: command to be executed by remote shell
        @param capture: if set to True then output produced by remote
                        command is captured and returned as string
        """

        cmd = self._createSSHCommand()
        return cmd.execute(command, capture)

    def getSshCmd(self, command):
        cmd = self._createSSHCommand()
        return cmd.getCommand(command)
