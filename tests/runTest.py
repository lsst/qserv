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

import MySQLdb as sql
import optparse
import unittest

# pass these through arguments
caseNo = "caseO1"
mysqlAuth = "/home/becla/.lsst/dbAuth.txt"


class TestQueries(unittest.TestCase):
    def setUp(self):
        global _caseNo
        global _mysqlUser
        global _mysqlPass
        global _mysqlSocket

        self._conn = sql.connect(unix_socket=_mysqlSocket,
                                 user=_mysqlUser,
                                 passwd=_mysqlPass)
        self_cursor = self._conn.cursor()

    def tearDown(self):
        self._cursor.close()
        self._conn.close()

    def countQuery(self, query, retNo):
        self._cursor.execute(query)
        rows = self._cursor.fetchone()
        self.assertEqual(rows[0][0], retNo, 
              query + " returned %s, expected %s." % (rows[0][0], retNo))

    def checksumQuery(self, query, retChecksum):
        self._cursor.execute(query)
        rows = self._cursor.fetchall()
        # calculate checksum of rows
        # compare with expected checksum

    def test_0xxxSeries(self):
        return oneSeries(self, "0")

    def test_1xxxSeries(self):
        return oneSeries(self, "1")

    def test_2xxxSeries(self):
        return oneSeries(self, "2")

    def test_3xxxSeries(self):
        return oneSeries(self, "3")

    def test_4xxxSeries(self):
        return oneSeries(self, "4")

    def oneSeries(self, seriesNo):
        global _caseNo
        print "Processing %s/queries/%sxxx_*.sql" % (_caseNo, seriesNo)


def main():
    global _caseNo
    global _mysqlUser
    global _mysqlPass
    global _mysqlSocket

    parser = optparse.OptionParser()
    parser.add_option("-c", "--caseNo", dest="caseNo",
                      default="case01",
                      help="test case")
    parser.add_option("-a", "--authFile", dest="authFile",
                      help="File with mysql connection info")
    (_options, args) = parser.parse_args()

    _caseNo = _options.caseNo
    authFile = _options.authFile
    f = open(authFile)
    for line in f:
        line = line.rstrip()
        (key, value) = line.split(':')
        if key == 'user':
            _mysqlUser = value
        elif key == 'pass':
            _mysqlPass = value
        elif key == 'sock':
            _mysqlSocket = value
    f.close()

    unittest.main()

if __name__ == '__main__':
    main()
