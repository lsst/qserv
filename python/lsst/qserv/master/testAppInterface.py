#!/usr/bin/env python

# 
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

# testAppInterface.py : A module with Python unittest code for testing
# functionality available through the appInterface module.  Currently
# only includes minimal fuzz testing and (unfinished) query replaying.


# Standard Python imports
import unittest
import sys
import time

# Package imports
import lsst.qserv.master
from lsst.qserv.master import appInterface as app
from lsst.qserv.master import config

class TestAppInterface(unittest.TestCase):
    def setUp(self):
        pass

    def testFuzz(self, seed=None):
        """Fuzz-test the app interface.
        This consists of feeding oddly-shaped garbage to the app interface
        and seeing if we get an error message or a crash.
        Goal: Test the outward-facing, non-developer interfaces.
        """
        self._applyQueryFromFile("/etc/motd")

    def testShortCircuit(self):
        r = app.computeShortCircuitQuery("select current_user()",{})
        self.assertEqual(r, ("qserv@%", "", "")
        pass

    def _makeBadHint(self):
        "This should be randomized."
        d = {"I":32,"like":44,"peaches,":66, 
             11: "and", 22:"peaches",33:"like",44:"me"}
        return d

    def _applyQueryFromFile(self, filename):
        x = self._readFile(filename)
        if x:
            a = app.AppInterface()
            q = x
            result = a.queryNow(q, self._makeBadHint())
        print result

    def _readFile(self, filename):
        try:
            x = open(filename, "r").read()
            return x
        except:
            return None
    
            
        
