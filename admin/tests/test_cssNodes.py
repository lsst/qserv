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
This is a unit test for CSS node definitions.

@author  Andy Salnikov, SLAC

"""

import os
import tempfile
import unittest
import StringIO

import lsst.qserv.admin.qservAdmin as qservAdmin


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


class TestConfigParser(unittest.TestCase):

    def testGetNode(self):
        """ Test for getting nodes """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-1", "mysqlConn": "3306"}}
/NODES/worker-2.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-2", "mysqlConn": "3307"}}
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        node = admin.getNode('worker-1')
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = admin.getNode('worker-2')
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-2')
        self.assertEqual(node['mysqlConn'], '3307')

        self.assertRaises(Exception, admin.getNode, 'worker-3')


    def testGetNodeUnpacked(self):
        """ Test for getting nodes, data in CSS is not packed """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1\t\\N
/NODES/worker-1/type\tworker
/NODES/worker-1/host\tworker.domain
/NODES/worker-1/runDir\t/tmp/worker-1
/NODES/worker-1/mysqlConn\t3306
/NODES/worker-2\t\\N
/NODES/worker-2/type\tworker
/NODES/worker-2/host\tworker.domain
/NODES/worker-2/runDir\t/tmp/worker-2
/NODES/worker-2/mysqlConn\t3307
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        node = admin.getNode('worker-1')
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = admin.getNode('worker-2')
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-2')
        self.assertEqual(node['mysqlConn'], '3307')

        self.assertRaises(Exception, admin.getNode, 'worker-3')


    def testGetNodes(self):
        """ Test for getting all nodes """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-1", "mysqlConn": "3306"}}
/NODES/worker-2.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-2", "mysqlConn": "3307"}}
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        nodes = admin.getNodes()

        self.assertEqual(sorted(nodes.keys()), ['worker-1', 'worker-2'])

        node = nodes['worker-1']
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = nodes['worker-2']
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-2')
        self.assertEqual(node['mysqlConn'], '3307')

    def testAddNode(self):
        """ Test for creating new nodes """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        nodeName = 'worker-1'
        nodeType = 'worker'
        host = 'worker.domain'
        runDir = '/tmp/worker-1'
        mysqlConn = '3306'
        admin.addNode(nodeName, nodeType, host, runDir, mysqlConn)

        nodeName = 'worker-2'
        nodeType = 'worker'
        host = 'worker.domain'
        runDir = '/tmp/worker-2'
        mysqlConn = '3307'
        admin.addNode(nodeName, nodeType, host, runDir, mysqlConn)

        node = admin.getNode('worker-1')
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = admin.getNode('worker-2')
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-2')
        self.assertEqual(node['mysqlConn'], '3307')

        self.assertRaises(Exception, admin.getNode, 'worker-3')

####################################################################################

if __name__ == "__main__":
    unittest.main()
