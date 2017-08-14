#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014 AURA/LSST.
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
This is a unit test for chunkMapping module.

@author  Andy Salnikov, SLAC

"""

import unittest

from lsst.qserv import css
from lsst.qserv.admin.chunkMapping import ChunkMapping


def _makeCss(data=None):
    """
    Create CssAccess instance with some pre-defined data.
    """

    # make an instance
    data = data or ""
    instance = css.CssAccess.createFromData(data, "")

    return instance


class TestChunkMapping(unittest.TestCase):

    def testRR(self):
        """ Testing round-robin without CSS backend """

        workers = ['worker1', 'worker2']
        database = 'TESTDB'
        table = 'TABLE'
        mapper = ChunkMapping(workers, database, table)

        # current implementation works in round-robin mode
        wmap = [mapper.worker(w) for w in range(10)]
        for i, worker in enumerate(wmap):
            expect = workers[i % len(workers)]
            self.assertEqual(worker, expect)

    def testRepeat(self):
        """ Testing repeatability without CSS backend """

        workers = ['worker1', 'worker2']
        database = 'TESTDB'
        table = 'TABLE'
        mapper = ChunkMapping(workers, database, table)

        # repeat the same thing twice, must get identical output
        wmap1 = [mapper.worker(w) for w in range(10)]
        wmap2 = [mapper.worker(w) for w in range(10)]
        self.assertEqual(wmap1, wmap2)

    def testCss1(self):
        """ Test for reading data from CSS """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/{db}\t\\N
/DBS/{db}/TABLES\t\\N
/DBS/{db}/TABLES/{table}\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333/REPLICAS\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333/REPLICAS/1\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333/REPLICAS/1/.packed.json\t{{"nodeName": "worker333"}}
/DBS/{db}/TABLES/{table}/CHUNKS/765\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/765/REPLICAS\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/765/REPLICAS/1\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/765/REPLICAS/1/.packed.json\t{{"nodeName": "worker765"}}
"""

        workers = ['worker1', 'worker2']
        database = 'TESTDB'
        table = 'TABLE'

        initData = initData.format(version=css.VERSION, db=database, table=table)
        css_inst = _makeCss(initData)

        mapper = ChunkMapping(workers, database, table, css_inst)

        # check that pre-defined chunks work
        worker = mapper.worker(333)
        self.assertEqual(worker, 'worker333')
        worker = mapper.worker(765)
        self.assertEqual(worker, 'worker765')

        # chunks that are not in CSS should return workers from the list
        worker = mapper.worker(1)
        self.assertEqual(worker, 'worker1')
        worker = mapper.worker(2)
        self.assertEqual(worker, 'worker2')

    def testCss2(self):
        """ Test for reading data from CSS, use chunks map from different table """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/{db}\t\\N
/DBS/{db}/TABLES\t\\N
/DBS/{db}/TABLES/{table}\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333/REPLICAS\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333/REPLICAS/1\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/333/REPLICAS/1/.packed.json\t{{"nodeName": "worker333"}}
/DBS/{db}/TABLES/{table}/CHUNKS/765\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/765/REPLICAS\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/765/REPLICAS/1\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS/765/REPLICAS/1/.packed.json\t{{"nodeName": "worker765"}}
"""

        workers = ['worker1', 'worker2']
        database = 'TESTDB'
        table = 'TABLE'

        initData = initData.format(version=css.VERSION, db=database, table="TBL123")
        css_inst = _makeCss(initData)

        mapper = ChunkMapping(workers, database, table, css_inst)

        # check that pre-defined chunks work
        worker = mapper.worker(333)
        self.assertEqual(worker, 'worker333')
        worker = mapper.worker(765)
        self.assertEqual(worker, 'worker765')

        # chunks that are not in CSS should return workers from the list
        worker = mapper.worker(1)
        self.assertEqual(worker, 'worker1')
        worker = mapper.worker(2)
        self.assertEqual(worker, 'worker2')

    def testCss3(self):
        """ Test for saving data to CSS """

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/DBS\t\\N
/DBS/{db}\t\\N
/DBS/{db}/TABLES\t\\N
/DBS/{db}/TABLES/{table}\t\\N
/DBS/{db}/TABLES/{table}/CHUNKS\t\\N
"""

        workers = ['worker1', 'worker2']
        database = 'TESTDB'
        table = 'TABLE'

        initData = initData.format(version=css.VERSION, db=database, table=table)
        css_inst = _makeCss(initData)

        mapper = ChunkMapping(workers, database, table, css_inst)

        # chunks that are not in CSS should return workers from the list
        worker = mapper.worker(1)
        self.assertEqual(worker, 'worker1')
        worker = mapper.worker(2)
        self.assertEqual(worker, 'worker2')

        # save stuff to CSS
        mapper.save()

        # save all CSS to string
        data = css_inst.getKvI().dumpKV()

        # build another CSS instance from saved data
        css_inst = _makeCss(data)

        # new mapper, use different worker set to avoid confusion
        workers = ['worker1000', 'worker2000']
        mapper = ChunkMapping(workers, database, table, css_inst)

        # get workers for chunks from css
        worker = mapper.worker(1)
        self.assertEqual(worker, 'worker1')
        worker = mapper.worker(2)
        self.assertEqual(worker, 'worker2')
        worker = mapper.worker(3)
        self.assertEqual(worker, 'worker1000')
        worker = mapper.worker(4)
        self.assertEqual(worker, 'worker2000')

#


if __name__ == "__main__":
    unittest.main()
