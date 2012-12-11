#!/usr/bin/env python

# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
#
# Tool for testing qserv metadata on the worker.


import os
import ConfigParser
import logging
import unittest

from lsst.qserv.admin.meta import Meta, readConnInfoFromFile


class TestMeta(unittest.TestCase):
    def setUp(self):
        loggerName = "testLogger"
        logger = logging.getLogger(loggerName)
        hdlr = logging.FileHandler("/tmp/qmwTestTool.log")
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr) 
        logger.setLevel(logging.DEBUG)

        self._meta = Meta(loggerName, *readConnInfoFromFile("~/.qmwadm"))
        # cleanup
        try: self._meta.destroyMeta()
        except Exception, e: pass

    def tearDown(self):
        try: self._meta.destroyMeta()
        except Exception, e: pass

    ###########################################################################
    #### test_basics
    ###########################################################################
    def test_all(self):
        print "=====>> print meta (but meta does not exist)"
        self.assertRaises(Exception, self._meta.printMeta)

        print "=====>> list dbs (but meta does not exist)"
        self.assertRaises(Exception, self._meta.listDbs)

        print "=====>> registerDb (but meta does not exist)"
        self.assertRaises(Exception, self._meta.registerDb, "dfdfd")

        print "=====>> install meta"
        self._meta.installMeta()

        print "=====>> list dbs"
        print self._meta.listDbs()

        print "=====>> registerDb"
        self._meta.registerDb("Summer2012")

        print "=====>> print meta"
        print self._meta.printMeta()

        print "=====>> registerDb (already registered)"
        self.assertRaises(Exception, self._meta.registerDb, "Summer2012")

        print "=====>> registerDb (db does not exists on server)"
        self.assertRaises(Exception, self._meta.registerDb, "dfd")

        print "=====>> unregisterDb"
        self._meta.unregisterDb("Summer2012")

        print "=====>> unregisterDb (db not registered)"
        self.assertRaises(Exception, self._meta.unregisterDb, "dfdfd")







###############################################################################
##### main
###############################################################################
if __name__ == '__main__':
    unittest.main()
