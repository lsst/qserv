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
import unittest

from lsst.qserv import css
import lsst.qserv.admin.nodeAdmin as nodeAdmin


logging.basicConfig(level=logging.INFO)
_LOG = logging.getLogger('TEST')


def _makeCss(data=None):
    """
    Create CssAccess instance with some pre-defined data.
    """

    # make an instance
    data = data or ""
    instance = css.CssAccess.createFromData(data, "")

    return instance


class TestWorkerAdmin(unittest.TestCase):

    def test_WorkerAdminExceptions(self):
        """ Check that some instantiations of NodeAdmin cause exceptions """

        # no arguments to constructor
        self.assertRaises(Exception, nodeAdmin.NodeAdmin)

        # name given but no CssAccess
        self.assertRaises(Exception, nodeAdmin.NodeAdmin, name="worker")

        # name not given, must have both host and port
        self.assertRaises(Exception, nodeAdmin.NodeAdmin, host="worker")
        self.assertRaises(Exception, nodeAdmin.NodeAdmin, port=5012)

        # instantiate kvI with come initial data
        initData = """\
/\t\\N
/css_meta\t\\N
/css_meta/version\t{version}
/NODES\t\\N
/NODES/worker\t\\N
/NODES/worker/.packed.json\t{{"type": "worker", "host": "localhost", "port": 5012}}
"""
        initData = initData.format(version=css.VERSION)
        css_inst = _makeCss(initData)

        # setup with CSS, this should not throw
        nodeAdmin.NodeAdmin(name="worker", css=css_inst)

#


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestWorkerAdmin)
    unittest.TextTestRunner(verbosity=3).run(suite)
