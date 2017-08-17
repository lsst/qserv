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
Module defining ssh utilities

@author  Andy Salnikov, SLAC
@author  Fabrice Jammes, IN2P3
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging
import socket
import subprocess

# -----------------------------
# Imports for other modules --
# -----------------------------

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)

# ------------------------
# Exported definitions --
# ------------------------


class SSHCommand(object):
    """
    Class implementing execution of the command on remote host.
    """

    # options for ssh:
    # - known_hosts file has tendency to become inconsistent, ignore it
    sshOptions = ['StrictHostKeyChecking=no']

    def __init__(self, host, cwd=None, kerberos=False, user=None):
        """
        Start ssh and wait for commands.

        @param host:         String, host name or IP address to SSH to.
        @param cwd:          If specified then use it as current working directory
        """

        args = ['ssh', '-T', '-x']

        if kerberos:
            args.append('-K')

        if user:
            args += ['-l', user]

        if _LOG.isEnabledFor(logging.DEBUG):
            args += ['-vvv']

        for option in self.sshOptions:
            args += ['-o', option]
        args += [host]
        self.cmd = args
        self.cwd = cwd
        self.host = host

    def getCommand(self, command):
        cmd = ""
        if self.cwd:
            cmd = "cd '{0}' && ".format(self.cwd)
        cmd += command
        args = self.cmd + [cmd]
        return args

    def execute(self, command, capture=False):
        """
        Send command to remote host.

        It will raise exception if ssh returns with error code.

        @param command: command to be executed by remote shell
        @param capture: if set to True then output produced by remote
                        command is captured and returned as string
        """
        args = self.getCommand(command)
        _LOG.debug("ssh command: %r", args)
        if capture:
            result = subprocess.check_output(args, subprocess.STDOUT, close_fds=True)
            return result
        else:
            subprocess.check_call(args, close_fds=True)


class SSHTunnel(object):
    """
    Class implementing SSH tunneling of TCP port(s).

    Instance of this class starts separate SSH process configured for TCP
    forwarding. Instance also controls lifetime of the process, when
    instance disappears the process is killed.
    """

    # options for ssh:
    # - known_hosts file has tendency to become inconsistent, ignore it
    # - if port forwarding fails there is no reason to stay alive
    sshOptions = ['StrictHostKeyChecking=no', 'ExitOnForwardFailure=yes']

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

        _LOG.debug('ssh tunnel: executing %r', args)
        subprocess.check_call(args, close_fds=True)

    def __del__(self):
        """
        When this instance is destroyed we also want to stop SSH.
        """
        # use control socket to stop ssh
        args = ['ssh', '-S', self.controlSocket, '-O', 'exit', 'localhost']
        try:
            _LOG.debug('ssh tunnel: signaling process to exit: %r', args)
            subprocess.check_call(args, stderr=open('/dev/null', 'w'), close_fds=True)
        except Exception as exc:
            _LOG.warning('Exception while trying to shutdown ssh tunnel: %r', exc)


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
