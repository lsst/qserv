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
import unittest

import lsst.qserv.admin.nodePool as nodePool
from lsst.qserv.admin.ssh import SSHCommand


logging.basicConfig(level=logging.INFO)
_LOG = logging.getLogger('TEST')


class TestNodePool(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        host = "localhost"
        cls._nb_nodes = 3
        krb = False

        nodeAdmins = [SSHCommand(host=host, kerberos=krb)
                      for n in range(cls._nb_nodes)]
        cls._nodePool = nodePool.NodePool(nodeAdmins)

    def test_ExecParallel(self):
        """ Check parallel execution of command """
        failed = self._nodePool.execParallel('echo ECHO')
        self.assertEqual(failed, 0)

    def test_ExecParallel_Stdin(self):
        """ Check parallel execution of command from stdin """
        failed = self._nodePool.execParallel('echo ECHO')
        self.assertEqual(failed, 0)

    def test_ExecParallel_Fail(self):
        """ Check parallel execution of command, with failure """
        failed = self._nodePool.execParallel('false')
        self.assertEqual(failed, self._nb_nodes)


#

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNodePool)
    unittest.TextTestRunner(verbosity=3).run(suite)
