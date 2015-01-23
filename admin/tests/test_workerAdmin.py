#!/usr/bin/env python

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
This is a unit test for workerAdmin module.

@author  Andy Salnikov, SLAC

"""

import logging
import os
import socket
import tempfile
import unittest
from threading import Thread

import lsst.qserv.admin.workerAdmin as workerAdmin
import lsst.qserv.admin.qservAdmin as qservAdmin


logging.basicConfig(level=logging.INFO)
_LOG = logging.getLogger('TEST')


class _EchoServer(Thread):
    """ simple echo server class """

    def __init__(self):
        Thread.__init__(self)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('127.0.0.1', 0))
        self.port = self.sock.getsockname()[1]

    def run(self):
        self.sock.listen(1)
        stop = False
        while not stop:
            conn, addr = self.sock.accept()
            while 1:
                # echo received data until client disconnects
                data = conn.recv(1024)
                if not data: break
                conn.send("@" + data)
                # EXIT means stop after client disconnects
                stop = data == 'EXIT'
            conn.close()

class _EchoClient(object):
    """ simple echo client class """

    def __init__(self, port, host='127.0.0.1'):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
    def echo(self, data):
        self.sock.send(data)
        data = self.sock.recv(1024)
        return data
    def close(self):
        self.sock.close()

def _makeAdmin(data=None):
    """
    Create QservAdmin instance with some pre-defined data.
    """
    if data is None:
        # read from /dev/null
        connection = '/dev/null'
    else:
        # make temp file and save data in it
        file = tempfile.NamedTemporaryFile(delete=False)
        connection = file.name
        file.write(data)
        file.close()

    # make an instance
    config = dict(technology='mem', connection=connection)
    admin = qservAdmin.QservAdmin(config=config)

    # remove tmp file
    if connection != '/dev/null':
        os.unlink(connection)

    return admin


class TestWorkerAdmin(unittest.TestCase):

    def test_SSHTunnel(self):
        """ Test for SSH tunneling class """

        # run simple echo server
        server = _EchoServer()
        server.daemon = True    # if exception happens we have to ignore it
        server.start()

        # first test direct connection to echo server
        client = _EchoClient(server.port)
        data = client.echo('ECHO')
        self.assertEqual(data, '@ECHO')
        client.close()

        # start port forwarder
        fwd = workerAdmin._SSHTunnel('localhost', 0, 'localhost', server.port, 'localhost')
        client = _EchoClient(fwd.port)
        data = client.echo('ECHO')
        self.assertEqual(data, '@ECHO')
        data = client.echo('EXIT')
        self.assertEqual(data, '@EXIT')
        client.close()
        del fwd


    def test_WorkerAdminExceptions(self):
        """ Check that some instantiations of WorkerAdmin cause exceptions """

        # no arguments to constructor
        self.assertRaises(Exception, workerAdmin.WorkerAdmin)

        # name given but no qservAdmin
        self.assertRaises(Exception, workerAdmin.WorkerAdmin, name="worker")

        # host given but no mysqlConn, causes exception on method call
        wAdmin = workerAdmin.WorkerAdmin(host="worker")
        self.assertRaises(Exception, wAdmin.mysqlConn)


    def test_WorkerMysqlTunnel_NoTunnel(self):
        """ Check direct connection to worker mysql """

        # we use echo server instead of actual mysql
        server = _EchoServer()
        server.daemon = True    # if exception happens we have to ignore it
        server.start()

        # setup for direct connection
        mysqlConn = str(server.port)
        wAdmin = workerAdmin.WorkerAdmin(host="localhost", mysqlConn=mysqlConn)

        # setup tunneling, expect direct connection
        host, port, tunnel = wAdmin._mysqlTunnel()
        self.assertEqual(host, 'localhost')
        self.assertEqual(port, server.port)
        self.assertIs(tunnel, None)

        # exchange some data over channel
        client = _EchoClient(port, host)
        data = client.echo('ECHO')
        self.assertEqual(data, '@ECHO')
        data = client.echo('EXIT')
        self.assertEqual(data, '@EXIT')
        client.close()


    def test_WorkerMysqlTunnel_Tunnel(self):
        """ Check tunneling for mysql """

        # we use echo server instead of actual mysql
        server = _EchoServer()
        server.daemon = True    # if exception happens we have to ignore it
        server.start()

        # setup for direct connection
        mysqlConn = 'lo:' + str(server.port)
        wAdmin = workerAdmin.WorkerAdmin(host="localhost", mysqlConn=mysqlConn)

        # setup tunneling
        host, port, tunnel = wAdmin._mysqlTunnel()
        self.assertEqual(host, '127.0.0.1')
        self.assertIsNot(tunnel, None)

        # exchange some data over channel
        client = _EchoClient(port, host)
        data = client.echo('ECHO')
        self.assertEqual(data, '@ECHO')
        data = client.echo('EXIT')
        self.assertEqual(data, '@EXIT')
        client.close()


    def test_WorkerMysqlTunnel_CSS(self):
        """ Check that tunneling works with CSS data """

        # we use echo server instead of actual mysql
        server = _EchoServer()
        server.daemon = True    # if exception happens we have to ignore it
        server.start()

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker\t\\N
/NODES/worker.json\t{{"type": "worker", "host": "localhost", "runDir": "/tmp/worker-1", "mysqlConn": "lo:{port}"}}
"""
        initData = initData.format(version=qservAdmin.VERSION, port=server.port)
        admin = _makeAdmin(initData)

        # setup with CSS
        wAdmin = workerAdmin.WorkerAdmin(name="worker", qservAdmin=admin)

        # setup tunneling
        host, port, tunnel = wAdmin._mysqlTunnel()
        self.assertEqual(host, '127.0.0.1')
        self.assertIsNot(tunnel, None)

        # exchange some data over channel
        client = _EchoClient(port, host)
        data = client.echo('ECHO')
        self.assertEqual(data, '@ECHO')
        data = client.echo('EXIT')
        self.assertEqual(data, '@EXIT')
        client.close()


    def test_WorkerExecCommand_Capture(self):
        """ Check execution of simple command with output capture """

        # setup for direct connection
        wAdmin = workerAdmin.WorkerAdmin(host="localhost", runDir='/tmp')

        output = wAdmin.execCommand('/bin/echo ECHO', capture=True)
        self.assertEqual(output, "ECHO\n")


    def test_WorkerExecCommand(self):
        """ Check execution of simple command """

        # setup for direct connection
        wAdmin = workerAdmin.WorkerAdmin(host="localhost", runDir='/tmp')

        output = wAdmin.execCommand('/bin/true')
        self.assertIs(output, None)


    def test_WorkerExecCommand_Fail(self):
        """ Check execution of simple command, failure results in exception """

        # setup for direct connection
        wAdmin = workerAdmin.WorkerAdmin(host="localhost", runDir='/tmp')

        self.assertRaises(Exception, wAdmin.execCommand, '/bin/false')


####################################################################################

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestWorkerAdmin)
    unittest.TextTestRunner(verbosity=3).run(suite)
