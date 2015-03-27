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
This is a unit test for nodePool module.

@author  Fabrice Jammes, IN2P3

"""

import logging
import os
import socket
import tempfile
import unittest

import lsst.qserv.admin.workerAdmin as workerAdmin
import lsst.qserv.admin.qservAdmin as qservAdmin
import lsst.qserv.admin.nodePool as nodePool


logging.basicConfig(level=logging.INFO)
_LOG = logging.getLogger('TEST')

class TestNodePool(unittest.TestCase):

    def test_NodePoolExecParallel(self):
        """ Check execution of simple command in parallel,

        """

        host_tpl = "ccqserv{0}"
        node_start = 100
        node_stop = 120
        user = "fjammes"
        krb = True

        # setup for direct connection
        wAdmins = [workerAdmin.WorkerAdmin(host=host_tpl.format(n),
                                           runDir="/bin",
                                           kerberos=krb,
                                           ssh_user=user)
                   for n in range(node_start, node_stop)]
        nPool = nodePool.NodePool(wAdmins)
        nPool.execParallel('./ls')

#
#     def test_NodePoolExecParallel_Fail(self):
#         """ Check execution of simple command, failure results in exception """
#
#         # setup for direct connection
#         wAdmin = workerAdmin.WorkerAdmin(host="localhost", runDir='/tmp')
#
#         self.assertRaises(Exception, wAdmin.execCommand, '/bin/false')


####################################################################################

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNodePool)
    unittest.TextTestRunner(verbosity=3).run(suite)
