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

import unittest

from lsst.qserv import css
import lsst.qserv.admin.nodeMgmt as nodeMgmt


def _makeCss(data=None):
    """
    Create CssAccess instance with some pre-defined data.
    """

    # make an instance
    data = data or ""
    instance = css.CssAccess.createFromData(data, "")

    return instance


class TestCssNodes(unittest.TestCase):

    def testGetNode(self):
        """ Test for getting nodes """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1\tACTIVE
/NODES/worker-1/.packed.json\t{{"type": "worker", "host": "worker.domain", "port": 5012}}
/NODES/worker-2\tINACTIVE
/NODES/worker-2/.packed.json\t{{"type": "worker", "host": "worker.domain", "port": 5013}}
"""

        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        node = css_inst.getNodeParams('worker-1')
        self.assertEqual(node.state, "ACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5012)

        node = css_inst.getNodeParams('worker-2')
        self.assertEqual(node.state, "INACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5013)

        with self.assertRaises(css.NoSuchNode):
            css_inst.getNodeParams('worker-3')

    def testGetNodeUnpacked(self):
        """ Test for getting nodes, data in CSS is not packed """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1\tACTIVE
/NODES/worker-1/type\tworker
/NODES/worker-1/host\tworker.domain
/NODES/worker-1/port\t5012
/NODES/worker-2\tINACTIVE
/NODES/worker-2/type\tworker
/NODES/worker-2/host\tworker.domain
/NODES/worker-2/port\t5013
"""

        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        node = css_inst.getNodeParams('worker-1')
        self.assertEqual(node.state, "ACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5012)

        node = css_inst.getNodeParams('worker-2')
        self.assertEqual(node.state, "INACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5013)

        with self.assertRaises(css.NoSuchNode):
            css_inst.getNodeParams('worker-3')

    def testGetNodes(self):
        """ Test for getting all nodes """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1\tACTIVE
/NODES/worker-1/.packed.json\t{{"type": "worker", "host": "worker.domain", "port": 5012}}
/NODES/worker-2\tINACTIVE
/NODES/worker-2/.packed.json\t{{"type": "worker", "host": "worker.domain", "port": 5013}}
"""

        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        nodes = css_inst.getAllNodeParams()

        self.assertEqual(sorted(nodes.keys()), ['worker-1', 'worker-2'])

        node = nodes['worker-1']
        self.assertEqual(node.state, "ACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5012)

        node = nodes['worker-2']
        self.assertEqual(node.state, "INACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5013)

    def testAddNode(self):
        """ Test for creating new nodes """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
"""
        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        params = css.NodeParams('worker', 'worker.domain', 5012, 'ACTIVE')
        nodeName = 'worker-1'
        css_inst.addNode(nodeName, params)

        params = css.NodeParams('worker', 'worker.domain', 5013, 'INACTIVE')
        nodeName = 'worker-2'
        css_inst.addNode(nodeName, params)

        node = css_inst.getNodeParams('worker-1')
        self.assertEqual(node.state, "ACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5012)

        node = css_inst.getNodeParams('worker-2')
        self.assertEqual(node.state, "INACTIVE")
        self.assertEqual(node.type, 'worker')
        self.assertEqual(node.host, 'worker.domain')
        self.assertEqual(node.port, 5013)

        with self.assertRaises(css.NoSuchNode):
            css_inst.getNodeParams('worker-3')

    def testDeleteNode(self):
        """ Test for deleting nodes """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/TEST\t\\N
/DBS/TEST/TABLES\t\\N
/DBS/TEST/TABLES/TEST\t\\N
/DBS/TEST/TABLES/TEST/.packed.json\t{{}}
/DBS/TEST/TABLES/TEST/CHUNKS\t\\N
/DBS/TEST/TABLES/TEST/CHUNKS/1\t\\N
/DBS/TEST/TABLES/TEST/CHUNKS/1/REPLICAS\t\\N
/DBS/TEST/TABLES/TEST/CHUNKS/1/REPLICAS/001\t\\N
/DBS/TEST/TABLES/TEST/CHUNKS/1/REPLICAS/001/.packed.json\t{{"nodeName": "worker-2"}}
/NODES\t\\N
/NODES/worker-1\tACTIVE
/NODES/worker-1/.packed.json\t{{"type": "worker", "host": "worker.domain", "port": 5012}}
/NODES/worker-2\tINACTIVE
/NODES/worker-2/.packed.json\t{{"type": "worker", "host": "worker.domain", "port": 5013}}
"""

        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        css_inst.getNodeParams('worker-1')
        css_inst.getNodeParams('worker-2')

        # delete
        css_inst.deleteNode('worker-1')
        with self.assertRaises(css.NoSuchNode):
            css_inst.getNodeParams('worker-1')
        css_inst.getNodeParams('worker-2')

        # must throw because there is a table using it
        with self.assertRaises(css.NodeInUse):
            css_inst.deleteNode('worker-2')

        # drop table that uses it and retry
        css_inst.dropTable('TEST', 'TEST')
        css_inst.deleteNode('worker-2')
        with self.assertRaises(css.NoSuchNode):
            css_inst.getNodeParams('worker-2')

    def testSetNodeState(self):
        """ Test for changing node state """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
"""

        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        params = css.NodeParams('worker', 'worker.domain', 5012, 'ACTIVE')
        nodeName = 'worker-1'
        css_inst.addNode(nodeName, params)

        params = css.NodeParams('worker', 'worker.domain', 5013, 'ACTIVE')
        nodeName = 'worker-2'
        css_inst.addNode(nodeName, params)

        node = css_inst.getNodeParams('worker-1')
        self.assertEqual(node.state, "ACTIVE")

        node = css_inst.getNodeParams('worker-2')
        self.assertEqual(node.state, "ACTIVE")

        css_inst.setNodeState('worker-1', "INACTIVE")
        node = css_inst.getNodeParams('worker-1')
        self.assertEqual(node.state, "INACTIVE")

        css_inst.setNodeState('worker-1', "ACTIVE")
        node = css_inst.getNodeParams('worker-1')
        self.assertEqual(node.state, "ACTIVE")

        css_inst.setNodeState('worker-1', "INACTIVE")
        css_inst.setNodeState('worker-2', "INACTIVE")
        node = css_inst.getNodeParams('worker-1')
        self.assertEqual(node.state, "INACTIVE")
        node = css_inst.getNodeParams('worker-2')
        self.assertEqual(node.state, "INACTIVE")

    def testMgmtSelect(self):
        """ Test for WorkerMgmt.select methods """

        # instantiate CSS with some initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker-1\tACTIVE
/NODES/worker-1/.packed.json\t{{"type": "worker", "host": "worker.domain", "port": 5012}}
/NODES/worker-2\tINACTIVE
/NODES/worker-2/.packed.json\t{{"type": "backup", "host": "worker.domain", "port": 5013}}
"""

        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        mgr = nodeMgmt.NodeMgmt(css_inst)

        # should return all nodes
        nodes = mgr.selectDict()
        self.assertEqual(len(nodes), 2)
        self.assertIn('worker-1', nodes)
        self.assertIn('worker-2', nodes)

        # select 'ACTIVE' or 'INACTIVE', should return two nodes
        nodes = mgr.selectDict(state=["ACTIVE", "INACTIVE"])
        self.assertEqual(len(nodes), 2)
        self.assertIn('worker-1', nodes)
        self.assertIn('worker-2', nodes)

        # select 'ACTIVE', should return one node
        nodes = mgr.selectDict(state=["ACTIVE"])
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-1', nodes)

        # select 'INACTIVE', should return one node
        nodes = mgr.selectDict(state="INACTIVE")
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
        nodes = mgr.selectDict(state="ACTIVE", nodeType='worker')
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-1', nodes)
        nodes = mgr.selectDict(state="INACTIVE", nodeType='backup')
        self.assertEqual(len(nodes), 1)
        self.assertIn('worker-2', nodes)
        nodes = mgr.selectDict(state="ACTIVE", nodeType='backup')
        self.assertEqual(len(nodes), 0)
        nodes = mgr.selectDict(state="INACTIVE", nodeType='worker')
        self.assertEqual(len(nodes), 0)

#


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCssNodes)
    unittest.TextTestRunner(verbosity=3).run(suite)
