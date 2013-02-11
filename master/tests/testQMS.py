#! /usr/bin/env python

# Local package imports
from lsst.qserv.metadata import qmsInterface
from lsst.qserv.metadata.qmsStatus import QmsStatus
from lsst.qserv.metadata.qmsStatus import getErrMsg

# Standard packages
import ConfigParser
import os
import socket
import unittest
import xmlrpclib

# hardcoded at the moment...
objectSchemaFile = '/u1/qserv/qserv/master/examples/qms/tbSchema_Object.sql'


class QmsTest(unittest.TestCase):
    """Tests sanity of Qserv Metadata Server."""
    def setUp(self):
        assert os.access(objectSchemaFile, os.R_OK), \
            "Can't read %s, you need to change it (hardcoded at the moment)" %\
            objectSchemaFile
        self._defaultXmlPath = "qms"
        self._qms = self._connectToQMS()
        self._qms.destroyMeta() # start fresh
        self._p1 = "\n---------- "
        self._p2 = " ----------"

    def tearDown(self):
        pass

    def _connectToQMS(self):
        config = ConfigParser.ConfigParser()
        config.read(os.path.expanduser("~/.qmsadm"))
        s = "qmsConn"
        assert config.has_section(s), \
            "Can't find section '%s' in .qmsadm" % s
        assert config.has_option(s,"host") and config.has_option(s,"port"), \
            "Bad %s, can't find host or port" % self._dotFileName
        (host, port) = (config.get(s, "host"), config.getint(s, "port"))
        url = "http://%s:%s/%s" % (host, port, self._defaultXmlPath)
        qms = xmlrpclib.Server(url)
        assert self._echoTest(qms), \
            "Can't connect to %s, is qms server running?" % url
        print "Connected to %s" % url
        return qms

    def _echoTest(self, qms):
        echostring = "QMS test string echo back. 1234567890.()''?"
        try:
            ret = qms.echo(echostring)
        except socket.error, err:
            print "Unable to connect to qms (%s)" % err
            return False
        if ret != echostring:
            print "Expected %s, got %s" % (echostring, ret)
            return False
        return True

    def _installQms(self, expectedRet):
        print self._p1, "install qms", self._p2
        self.assertEqual(self._qms.installMeta()==QmsStatus.SUCCESS,
                         expectedRet)
        if not expectedRet: print "(this failed, as expected)"

    def _createDb(self, theName, theOptions, expectedRet):
        print self._p1, "create db %s" % theName, self._p2
        self.assertEqual(self._qms.createDb(theName, theOptions)==
                         QmsStatus.SUCCESS, expectedRet)
        if not expectedRet: print "(this failed, as expected)"

    def _printMeta(self):
        print self._p1, "print meta", self._p2
        r = self._qms.printMeta()
        if r is not None:
            print r

    def _listDbs(self):
        print self._p1, "list dbs", self._p2
        print self._qms.listDbs()

    def _checkDbExists(self, theName, expectedRet):
        if expectedRet:
            print self._p1, "checking if db %s exists " % theName, 
            "(it should)", self._p2
        else:
            print self._p1, "checking if db %s exists " % theName, 
            "(it should not)", self._p2
        self.assertEqual(self._qms.checkDbExists(theName) == 1, expectedRet)

    def _retrieveDbInfo(self, theName, expectedRet):
        print self._p1, "retrieving info for %s" % theName, self._p2
        (retStat, values) = self._qms.retrieveDbInfo(theName)
        self.assertEqual(retStat==QmsStatus.SUCCESS, expectedRet)
        if retStat == QmsStatus.SUCCESS:
            for (k, v) in values.items():
                print "%s: %s" % (k, v)
        else:
            print "(this failed, as expected)"

    def _createTable(self, dbName, theOptions, schemaFileName, expectedRet):
        print self._p1,"creating table %s.%s" % \
            (dbName, theOptions['tableName']), self._p2
        schemaStr = open(schemaFileName, 'r').read()
        self.assertEqual(self._qms.createTable(dbName, 
                                               theOptions, 
                                               schemaStr)==QmsStatus.SUCCESS, 
                         expectedRet)
        if not expectedRet: print "(this failed, as expected)"

    def _retrieveTableInfo(self, dbName, tableName, expectedRet):
        print self._p1, "retrieving info for table %s.%s" % \
            (dbName, tableName), self._p2
        (retStat, values) = self._qms.retrieveTableInfo(dbName, tableName)
        self.assertEqual(retStat==QmsStatus.SUCCESS, expectedRet)
        if retStat == QmsStatus.SUCCESS:
            for (k, v) in values.items():
                print "%s: %s" % (k, v)
        else:
            print "(this failed, as expected)"

    def _dropTable(self, dbName, tableName, expectedRet):
        print self._p1, "dropping table %s.%s" % (dbName,tableName),self._p2
        self.assertEqual(
            self._qms.dropTable(dbName, tableName)==QmsStatus.SUCCESS,
            expectedRet)
        if not expectedRet: print "(this failed, as expected)"

    def _destroyMeta(self, expectedRet):
        print self._p1, "destroyMeta", self._p2
        self.assertEqual(self._qms.destroyMeta()==QmsStatus.SUCCESS,
                         expectedRet)
        if not expectedRet: print "(this failed, as expected)"

    def _dropDb(self, dbName, expectedRet):
        print self._p1, "dropping db %s" % dbName,self._p2
        self.assertEqual(
            self._qms.dropDb(dbName)==QmsStatus.SUCCESS, expectedRet)
        if not expectedRet: print "(this failed, as expected)"

    def test_1(self):
        #self._printMeta()
        self._installQms(True)
        self._installQms(False)
        self._printMeta()
        self._createDb('Summer2012', 
                       {'defaultOverlap_nearNeighbor': '0.25', 
                        'partitioning': 'on', 
                        'defaultOverlap_fuzziness': '0.0001',
                        'partitioningStrategy': 'sphBox', 
                        'nStripes': '10', 
                        'nSubStripes': '23'}, True)
        self._createDb('NonPartA', {'partitioning': 'off'}, True)
        self._createDb('NonPartA', {'partitioning': 'off'}, False)

        self._printMeta()
        self._listDbs()
        self._checkDbExists('Summer2012', True)
        self._checkDbExists('Summer2012xx', False)
        self._retrieveDbInfo('Summer2012', True)
        self._retrieveDbInfo('NonPartA', True)
        self._retrieveDbInfo('nonExistingDb', False)
        self._createTable('Summer2012', 
                          {'partitioning': 'on', 
                           'partitioningStrategy': 'sphBox',
                           'logicalPart': '2',
                           'thetaColName': 'decl_PS',
                           'tableName': 'Object',
                           'clusteredIndex': 'IDX_objectId',
                           'overlap': '0.025',
                           'physChunking': '0x0021',
                           'phiColName': 'ra_PS'},
                          objectSchemaFile,
                          True)
        self._createTable('Summer2012', 
                          {'partitioning': 'on', 
                           'partitioningStrategy': 'sphBox',
                           'logicalPart': '2',
                           'thetaColName': 'decl_PS',
                           'tableName': 'Object',
                           'clusteredIndex': 'IDX_objectId',
                           'overlap': '0.025',
                           'physChunking': '0x0021',
                           'phiColName': 'ra_PS'},
                          objectSchemaFile,
                          False) # this table already exists, should fail

        #self._createTable('Summer2012', 
        #                  {'partitioning': 'on', 
        #                   'partitioningStrategy': 'sthElse',
        #                   'logicalPart': '2',
        #                   'thetaColName': 'decl_PS',
        #                   'tableName': 'Object1',
        #                   'clusteredIndex': 'IDX_objectId',
        #                   'overlap': '0.025',
        #                   'physChunking': '0x0021',
        #                   'phiColName': 'ra_PS'},
        #                  objectSchemaFile,
        #                  False) # wrong partitioningStrategy, should fail

        #self._createTable('Summer2012', 
        #                  {'partitioning': 'on', 
        #                   'partitioningStrategy': 'sthElse',
        #                   'logicalPart': '2',
        #                   'tableName': 'Object2',
        #                   'clusteredIndex': 'IDX_objectId',
        #                   'overlap': '0.025',
        #                   'physChunking': '0x0021',
        #                   'phiColName': 'ra_PS'},
        #                  objectSchemaFile,
        #                  False) # missing thetaColName, should fail

        self._retrieveTableInfo('Summer2012', 'Object', True)
        self._retrieveTableInfo('NonPart', 'Object', False)
        self._retrieveTableInfo('Summer2012', 'somethingElse', False)
        self._retrieveTableInfo('weirdDb', 'somethingOther', False)

        self._dropTable('Summer2012', 'Object', True)
        self._dropTable('Summer2012', 'Object', False)
        self._dropTable('NonPart', 'Object', False)
        self._dropTable('Summer2012', 'somethingElse', False)
        self._dropTable('weirdDb', 'somethingOther', False)

        self._dropDb('Summer2012', True)
        self._dropDb('Summer2012', False)
        self._dropDb('Summer20xxxx', False)

        self._destroyMeta(True)
        self._destroyMeta(False)

def main():
    unittest.main()

if __name__ == "__main__":
    main()

