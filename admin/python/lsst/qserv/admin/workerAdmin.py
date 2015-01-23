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
Module defining WorkerAdmin class and related methods.

WorkerAdmin is an interface which controls or communicates with qserv
worker instance. Exact form of control is still to be defined, for now
we are going to support communication with mysql instance or ability to
start/stop worker processes. Current implementation is based on SSH
tunneling (or direct mysql connection if possible), in the future there
supposed to be a special service for worker administration.

@author  Andy Salnikov, SLAC
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging
import socket
import subprocess

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.db.exception import produceExceptionClass
from lsst.db.db import Db

#----------------------------------
# Local non-exported definitions --
#----------------------------------
_LOG = logging.getLogger('WADM')


_Exception = produceExceptionClass('WorkerAdminException', [
    (100, "ARG_ERR", "Missing or inconsistent arguments"),
    (105, "MYSQL_CONN_ERR", "Missing mysql connection info"),
    (110, "CSS_HOST_NAME_MISS", "Missing host name is in CSS"),
    (110, "MYSQL_CONN_FMT", "Unsupported format of mysql connection string"),
    ])


def _getFreePort():
    """
    Try to find one free local port number. Returned port number
    is not guaranteed to be free, race condition is unavoidable.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class _SSH(object):
    """
    Base class for services using SSH.
    """

    # options for ssh.
    # - if port forwarding fails there is no reason to stay alive
    # - known_hosts file has tendency to become inconsistent, ignore it
    sshOptions = ['StrictHostKeyChecking=no']


class _SSHCommand(_SSH):
    """
    Class implementing execution of the command on remote host.
    """

    def __init__(self, host, cwd=None):
        """
        Start ssh and wait for commands.

        @param host:         String, host name or IP address to SSH to.
        @param cwd:          If specified then use it as current working directory
        """

        args = ['ssh', '-n', '-T', '-x']
        for option in self.sshOptions:
            args += ['-o', option]
        args += [host]
        self.cmd = args
        self.cwd = cwd

    def execute(self, command, capture=False):
        """
        Send command to remote host.

        It will raise exception if ssh returns with error code.

        @param command: command to be executed by remote shell
        @param capture: if set to True then output produced by remote
                        command is captured and returned as string
        """

        cmd = ""
        if self.cwd:
            cmd = "cd '{0}'; ".format(self.cwd)
        cmd += command
        args = self.cmd + [cmd]
        _LOG.debug('ssh tunnel: executing %s', args)
        if capture:
            result = subprocess.check_output(args, subprocess.STDOUT, close_fds=True)
            return result
        else:
            subprocess.check_call(args, close_fds=True)


class _SSHTunnel(_SSH):
    """
    Class imlementing SSH tunneling of TCP port(s).

    Instance of this class starts separate SSH process configured for TCP
    forwarding. Instance also controls lifetime of the process, when
    instance disappears the process is killed.
    """

    sshOptions = _SSH.sshOptions + ['ExitOnForwardFailure=yes']

    def __init__(self, host, localPort, fwdHost, fwdPort, localAddress=None, cipher='arcfour'):
        """
        Start SSH tunnelling with specified parameters.

        This can generate same exceptions as subprocess.check_call method.

        @param host:         String, host name or IP address to SSH to.
        @param localPort:    Local port number (or range) that SSH will listen to, pass
                             0 to try to auto-select port number, use `port` member
                             later to access actual port used.
        @param fwdHost:      Remote host name or IP address to forward connections to.
        @param fwdPort:      Remote port number to forward connections to.
        @param localAddress: Local address to bind, by default all interfaces are used,
                             to bind to local interace only use "127.0.0.1" or "localhost".
        @param cipher:       Encryption cipher name, default is to use arcfour which is one
                             of the least CPU-intensive ciphers.
        """

        self.port = localPort or _getFreePort()

        # we are going to run ssh in background using -f option so that we can
        # wait until it forks itself after opening local port, that needs control
        # socket which will be used to shut it down later
        self.controlSocket = '/tmp/sshtunnel-control-socket-' + str(self.port)

        fwdSpec = [str(self.port), fwdHost, str(fwdPort)]
        if localAddress:
            fwdSpec.insert(0, localAddress)
        args = ['ssh', '-N', '-T', '-L', ':'.join(fwdSpec), '-x', '-c', cipher,
                '-f', '-M', '-S', self.controlSocket]
        for option in self.sshOptions:
            args += ['-o', option]
        args += [host]

        _LOG.debug('ssh tunnel: executing %s', args)
        subprocess.check_call(args, close_fds=True)

    def __del__(self):
        """
        When this instance is destroyed we also want to stop SSH.
        """
        # use control socket to stop ssh
        args = ['ssh', '-S', self.controlSocket, '-O', 'exit', 'localhost']
        try:
            _LOG.debug('ssh tunnel: signaling process to exit: %s', args)
            subprocess.check_call(args, stderr=open('/dev/null', 'w'), close_fds=True)
        except Exception as exc:
            _LOG.warning('Exception while trying to shutdown ssh tunnel: %s', exc)

#------------------------
# Exported definitions --
#------------------------

class WorkerAdmin(object):
    """
    Class representing administration/communication endpoint for qserv worker.
    """

    def __init__(self, name=None, qservAdmin=None, host=None, runDir=None, mysqlConn=None):
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

        else:

            if not host:
                raise _Exception(_Exception.ARG_ERR, 'either name or host has to be provided')

            self.host = host
            self.runDir = runDir
            self.mysqlConnStr = mysqlConn


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
            tunnel = _SSHTunnel(self.host, 0, self.host, port, host)
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

    def execCommand(self, command, capture=False):
        """
        Execute command on worker host.

        Command argument is a string which will be passed to the shell
        running on worker host. Before executing the command working
        directory will be set to the run directory of the corresponding
        qserv instance. To start qserv instance for example one can pass
        "bin/qserv-start.sh" as an argument.

        If ssh or remote command fails the exception will be raised.

        @param command: command to be executed by remote shell
        @param capture: if set to True then output produced by remote
                        command is captured and returned as string
        """

        cmd = _SSHCommand(self.host, self.runDir)
        return cmd.execute(command, capture)
