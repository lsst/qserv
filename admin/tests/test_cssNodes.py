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

import lsst.qserv.admin.qservAdmin as qservAdmin
import lsst.qserv.admin.workerMgmt as workerMgmt


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


class TestCssNodes(unittest.TestCase):

    def testGetNode(self):
        """ Test for getting nodes """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1\tACTIVE
/NODES/worker-1.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-1", "mysqlConn": "3306"}}
/NODES/worker-2\tINACTIVE
/NODES/worker-2.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-2", "mysqlConn": "3307"}}
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        node = admin.getNode('worker-1')
        self.assertEqual(node['state'], qservAdmin.NodeState.ACTIVE)
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = admin.getNode('worker-2')
        self.assertEqual(node['state'], qservAdmin.NodeState.INACTIVE)
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
/NODES/worker-1\tACTIVE
/NODES/worker-1/type\tworker
/NODES/worker-1/host\tworker.domain
/NODES/worker-1/runDir\t/tmp/worker-1
/NODES/worker-1/mysqlConn\t3306
/NODES/worker-2\tINACTIVE
/NODES/worker-2/type\tworker
/NODES/worker-2/host\tworker.domain
/NODES/worker-2/runDir\t/tmp/worker-2
/NODES/worker-2/mysqlConn\t3307
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        node = admin.getNode('worker-1')
        self.assertEqual(node['state'], qservAdmin.NodeState.ACTIVE)
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = admin.getNode('worker-2')
        self.assertEqual(node['state'], qservAdmin.NodeState.INACTIVE)
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
/NODES/worker-1\tACTIVE
/NODES/worker-1.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-1", "mysqlConn": "3306"}}
/NODES/worker-2\tINACTIVE
/NODES/worker-2.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-2", "mysqlConn": "3307"}}
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        nodes = admin.getNodes()

        self.assertEqual(sorted(nodes.keys()), ['worker-1', 'worker-2'])

        node = nodes['worker-1']
        self.assertEqual(node['state'], qservAdmin.NodeState.ACTIVE)
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = nodes['worker-2']
        self.assertEqual(node['state'], qservAdmin.NodeState.INACTIVE)
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
        state = qservAdmin.NodeState.INACTIVE
        admin.addNode(nodeName, nodeType, host, runDir, mysqlConn, state)

        node = admin.getNode('worker-1')
        self.assertEqual(node['state'], qservAdmin.NodeState.ACTIVE)
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-1')
        self.assertEqual(node['mysqlConn'], '3306')

        node = admin.getNode('worker-2')
        self.assertEqual(node['state'], qservAdmin.NodeState.INACTIVE)
        self.assertEqual(node['type'], 'worker')
        self.assertEqual(node['host'], 'worker.domain')
        self.assertEqual(node['runDir'], '/tmp/worker-2')
        self.assertEqual(node['mysqlConn'], '3307')

        self.assertRaises(Exception, admin.getNode, 'worker-3')

    def testSetNodeState(self):
        """ Test for changing node state """

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
        self.assertEqual(node['state'], qservAdmin.NodeState.ACTIVE)

        node = admin.getNode('worker-2')
        self.assertEqual(node['state'], qservAdmin.NodeState.ACTIVE)

        admin.setNodeState('worker-1', qservAdmin.NodeState.INACTIVE)
        node = admin.getNode('worker-1')
        self.assertEqual(node['state'], qservAdmin.NodeState.INACTIVE)

        admin.setNodeState('worker-1', qservAdmin.NodeState.ACTIVE)
        node = admin.getNode('worker-1')
        self.assertEqual(node['state'], qservAdmin.NodeState.ACTIVE)

        admin.setNodeState(['worker-1', 'worker-2'], qservAdmin.NodeState.INACTIVE)
        node = admin.getNode('worker-1')
        self.assertEqual(node['state'], qservAdmin.NodeState.INACTIVE)
        node = admin.getNode('worker-2')
        self.assertEqual(node['state'], qservAdmin.NodeState.INACTIVE)

    def testMgmtSelect(self):
        """ Test for WorkerMgmt.select methods """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1\tACTIVE
/NODES/worker-1.json\t{{"type": "worker", "host": "worker.domain", "runDir": "/tmp/worker-1", "mysqlConn": "3306"}}
/NODES/worker-2\tINACTIVE
/NODES/worker-2.json\t{{"type": "backup", "host": "worker.domain", "runDir": "/tmp/worker-2", "mysqlConn": "3307"}}
"""

        initData = initData.format(version=qservAdmin.VERSION)
        admin = _makeAdmin(initData)

        mgr = workerMgmt.WorkerMgmt(admin)

        # should return all nodes
        nodes = mgr.selectDict()
        self.assertEqual(len(nodes), 2)
        self.assertIn('worker-1', nodes)
        self.assertIn('worker-2', nodes)

        # select 'ACTIVE' or 'INACTIVE', should return two nodes
        nodes = mgr.selectDict(state=[qservAdmin.NodeState.ACTIVE, qservAdmin.NodeState.INACTIVE])
        self.assertEqual(len(nodes), 2)
        self.assertIn('worker-1', nodes)
        self.assertIn('worker-2', nodes)

        # select 'ACTIVE', should return one node
        nodes = mgr.selectDict(state=[qservAdmin.NodeState.ACTIVE])
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-1', nodes)

        # select 'INACTIVE', should return one node
        nodes = mgr.selectDict(state=qservAdmin.NodeState.INACTIVE)
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-2', nodes)

        # select 'worker' or 'backup', should return two nodes
        nodes = mgr.selectDict(nodeType=['worker', 'backup'])
        self.assertEqual(len(nodes), 2)
        self.assertIn('worker-1', nodes)
        self.assertIn('worker-2', nodes)

        # select 'worker', should return one node
        nodes = mgr.selectDict(nodeType=['worker'])
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-1', nodes)

        # select 'backup', should return one node
        nodes = mgr.selectDict(nodeType='backup')
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-2', nodes)

        # combinations
        nodes = mgr.selectDict(state=qservAdmin.NodeState.ACTIVE, nodeType='worker')
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-1', nodes)
        nodes = mgr.selectDict(state=qservAdmin.NodeState.INACTIVE, nodeType='backup')
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-2', nodes)
        nodes = mgr.selectDict(state=qservAdmin.NodeState.ACTIVE, nodeType='backup')
        self.assertEqual(len(nodes), 0)
        nodes = mgr.selectDict(state=qservAdmin.NodeState.INACTIVE, nodeType='worker')
        self.assertEqual(len(nodes), 0)

####################################################################################

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCssNodes)
    unittest.TextTestRunner(verbosity=3).run(suite)
