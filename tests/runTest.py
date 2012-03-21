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
import os
import re

class TestQueries():
    def setUp(self, socket, user, password, db):
        print "connecting via ", socket, " as ", user, "/", password
        self._conn = sql.connect(unix_socket=socket,
                                 user=user,
                                 passwd=password,
                                 db=db)
        self._cursor = self._conn.cursor()

    def tearDown(self):
        self._cursor.close()
        self._conn.close()

    def runQuery(self, query, printResults):
        self._cursor.execute(query)
        rows = self._cursor.fetchall()
        if printResults:
            for r in rows:
                print r


def main():
    parser = optparse.OptionParser()
    parser.add_option("-c", "--caseNo", dest="caseNo",
                      default="01",
                      help="test case number")
    parser.add_option("-a", "--authFile", dest="authFile",
                      help="File with mysql connection info")
    parser.add_option("-s", "--stopAt", dest="stopAt",
                      default = 799,
                      help="Stop at query with given number")
    parser.add_option("-o", "--resultDir", dest="resultDir",
                      default = "/tmp",
                      help="Directory for storing results (full path)")
    parser.add_option("-q", "--withQserv", dest="withQserv",
                      default = False,
                      help="Flag indicating if the test is with qserv or not")
    parser.add_option("-v", "--verbose", dest="verboseMode",
                      default = 'n',
                      help="Run in verbose mode (y/n)")
    (_options, args) = parser.parse_args()

    if _options.authFile is None:
        print "--authFile flag not set"
        return -1

    authFile = _options.authFile
    stopAt = int(_options.stopAt)
    if _options.verboseMode == 'y' or \
       _options.verboseMode == 'Y' or \
       _options.verboseMode == '1':
        verboseMode = True
    else:
        verboseMode = False

    if _options.withQserv == 'y' or \
       _options.withQserv == 'Y' or \
       _options.withQserv == '1':
        withQserv = True
    else:
        withQserv = False

    f = open(authFile)
    for line in f:
        line = line.rstrip()
        (key, value) = line.split(':')
        if key == 'user':
            mysqlUser = value
        elif key == 'pass':
            mysqlPass = value
        elif key == 'sock':
            mysqlSock = value
    f.close()

    mysqlDb = "qservTest_case%s" % _options.caseNo

    t = TestQueries()
    t.setUp(mysqlSock, mysqlUser, mysqlPass, mysqlDb)

    qDir = "case%s/queries/" % _options.caseNo
    print "Testing queries from %s" % qDir
    queries = sorted(os.listdir(qDir))
    noQservLine = re.compile('[\w\-\."%% ]*-- noQserv')
    for qFN in queries:
        if qFN.endswith(".sql"):
            if int(qFN[:3]) <= stopAt:
                qF = open(qDir+qFN, 'r')
                qText = ""
                for line in qF:
                    line = line.rstrip().lstrip()
                    line = re.sub(' +', ' ', line)
                    if withQserv and line.startswith("-- withQserv"):
                        qText += line[13:] # skip the "-- withQserv " text
                    elif line.startswith("--") or line == "":
                        pass # do nothing with comment lines and empty lines
                    else:
                        qData = noQservLine.search(line)
                        if not withQserv:
                            if qData:
                                qText += qData.group(0)[:-10]
                            else:
                                qText += line
                        elif not qData:
                            qText += line
                    qText += ' '
                qText += " INTO OUTFILE '%s/%s'" % \
                    (_options.resultDir, qFN.replace('.sql', '.txt'))
                print "running %s: %s\n" % (qFN, qText)
                t.runQuery(qText, verboseMode)

    t.tearDown()

if __name__ == '__main__':
    main()
