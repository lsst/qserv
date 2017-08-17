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

from __future__ import absolute_import, division, print_function

import logging
import socket
import unittest
from threading import Thread

from lsst.qserv.admin.ssh import SSHTunnel


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
            while True:
                # echo received data until client disconnects
                data = conn.recv(1024)
                if not data:
                    break
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


class TestSSHTunnel(unittest.TestCase):
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
        fwd = SSHTunnel('localhost', 0, 'localhost', server.port, 'localhost')
        client = _EchoClient(fwd.port)
        data = client.echo('ECHO')
        self.assertEqual(data, '@ECHO')
        data = client.echo('EXIT')
        self.assertEqual(data, '@EXIT')
        client.close()
        del fwd


#

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSSHTunnel)
    unittest.TextTestRunner(verbosity=3).run(suite)
