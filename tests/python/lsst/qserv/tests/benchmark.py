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

""" This test tool :
- loads multiple datasets in Qserv and MySQL,
- launches queries against them
- checks if results between both DB are the same
"""

__author__ = "Jacek Becla, Fabrice Jammes"

import logging
import optparse
import shutil

from lsst.qserv import qservdataloader, mysqldataloader, datareader
from lsst.qserv.admin import commons
from lsst.qserv.sql import cmd, connection, const
import os
import re
import stat

from filecmp import dircmp


class Benchmark():

    def __init__(self, case_id, out_dirname_prefix,
                 config_dir=None,
                 log_file_prefix='qserv-tests',
                 logging_level=logging.DEBUG ):

        self.logger = commons.console_logger(logging_level)
        self.dataLoader = dict()
        self._sqlInterface = dict()
        self._mode=None
        self._dbName=None

        if config_dir is None:
            self.config = commons.read_user_config()
        else:
            config_file_name=os.path.join(config_dir,"qserv-build.conf")
            self.config = commons.read_config(config_file_name)

        self._case_id = case_id
        self._logFilePrefix = log_file_prefix

        if out_dirname_prefix == None :
            out_dirname_prefix = self.config['qserv']['tmp_dir']
        self._out_dirname = os.path.join(out_dirname_prefix, "qservTest_case%s" % case_id)

        qserv_tests_dirname = os.path.join(
            self.config['qserv']['base_dir'],
            'qserv','tests', 'testdata',
            "case%s" % self._case_id
            )

        self._input_dirname = os.path.join(qserv_tests_dirname,'data')

        self.dataReader = datareader.DataReader(self._input_dirname, "case%s" % self._case_id)

        self._queries_dirname = os.path.join(qserv_tests_dirname,"queries")

        self.logger = commons.file_logger(
            log_file_prefix,
            log_path=self.config['qserv']['log_dir']
            )

    def runQueries(self, stopAt):
        self.logger.debug("Running queries : (stop-at : %s)" % stopAt)
        if self._mode == 'qserv':
            withQserv = True
            self._sqlInterface['query'] = cmd.Cmd(config = self.config,
                                                  mode = const.MYSQL_PROXY,
                                                  database = self._dbName
                                                  )
        else:
            withQserv = False
            self._sqlInterface['query'] = cmd.Cmd(config = self.config,
                                                mode = const.MYSQL_SOCK,
                                                database = self._dbName
                                                )

        myOutDir = os.path.join(self._out_dirname, "outputs",self._mode)
        if not os.access(myOutDir, os.F_OK):
            os.makedirs(myOutDir)
            # because mysqld will write there
            os.chmod(myOutDir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

        qDir = self._queries_dirname
        self.logger.info("Testing queries from %s" % qDir)
        queries = sorted(os.listdir(qDir))
        noQservLine = re.compile('[\w\-\."%% ]*-- noQserv')
        for qFN in queries:
            if qFN.endswith(".sql"):
                if int(qFN[:4]) <= stopAt:
                    query_filename = os.path.join(qDir,qFN)
                    qF = open(query_filename, 'r')
                    qText = ""
                    for line in qF:
                        line = line.rstrip().lstrip()
                        line = re.sub(' +', ' ', line)
                        if withQserv and line.startswith("-- withQserv"):
                            qText += line[13:] # skip the "-- withQserv" text
                        elif line.startswith("--") or line == "":
                            pass # do nothing with commented or empty lines
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
                    outFile = os.path.join(myOutDir, qFN.replace('.sql', '.txt'))
                    #qText += " INTO OUTFILE '%s'" % outFile
                    self.logger.info("Mode %s Running %s: %s\n" % (self._mode,qFN, qText))
                    self._sqlInterface['query'].execute(qText, outFile)


    def gunzip(self, table_name, zipped_data_filename):
        # check if the corresponding data file exists
        if not os.path.exists(zipped_data_filename):
            raise Exception, "File: '%s' not found" %  zipped_data_filename

        tmp_suffix = ("%s%s" % (table_name,self.dataReader.dataConfig['data-extension']))
        tmp_data_file = os.path.join(self._out_dirname,tmp_suffix)

        self.logger.info(" ./Uncompressing: %s into %s" %  (zipped_data_filename, tmp_data_file))
        commons.run_command(["gunzip", "-c", zipped_data_filename], stdout_file=tmp_data_file)
        return tmp_data_file

    def loadData(self):
        """
        Creates tables and load data for input file located in caseXX/data/
        """
        self.logger.info("Loading data from %s (%s mode)" % (self._input_dirname, self._mode))

        for table_name in  self.dataReader.tables:
            self.logger.debug("Using data of %s" % table_name)
            (schema_filename, data_filename, zipped_data_filename) =  self.dataReader.getSchemaAndDataFilenames(table_name)

            if zipped_data_filename is not None :
                tmp_data_file = self.gunzip(table_name, zipped_data_filename)
                input_filename = tmp_data_file
            else:
                input_filename = data_filename

            self.dataLoader[self._mode].createAndLoadTable(table_name, schema_filename, input_filename)

            if zipped_data_filename is not None :
                os.unlink(tmp_data_file)

    def cleanup(self):
        # cleanup of previous tests
        if os.path.exists(self._out_dirname):
            shutil.rmtree(self._out_dirname)
        os.makedirs(self._out_dirname)


    def connectAndInitDatabases(self):
        self.logger.info("Creation of data loader for %s mode" % self._mode)
        if (self._mode=='mysql'):
            self.dataLoader[self._mode] = mysqldataloader.MysqlDataLoader(
                self.config,
                self.dataReader.dataConfig,
                self._dbName,
                self._out_dirname,
                self._logFilePrefix
                )
        elif (self._mode=='qserv'):
            self.dataLoader[self._mode] = qservdataloader.QservDataLoader(
                self.config,
                self.dataReader.dataConfig,
                self._dbName,
                self._out_dirname,
                self._logFilePrefix
            )
        self.logger.info("Initializing database for %s mode" % self._mode)
        self.dataLoader[self._mode].connectAndInitDatabase()

    def finalize(self):
        if (self._mode=='qserv'):
            self.dataLoader[self._mode].createQmsDatabase()
            self.dataLoader[self._mode].configureQservMetaEmptyChunk()
        # in order to close socket connections
        del(self.dataLoader[self._mode])

    def run(self, mode_list, load_data, stop_at_query=7999):

        self.cleanup()
        self.dataReader.readInputData()

        for mode in mode_list:
            self._mode = mode

	    if self._mode == 'qserv':
	        self._dbName = "LSST"
	    else:
                self._dbName = "qservTest_case%s_%s" % (self._case_id, self._mode)

            if load_data:
                self.connectAndInitDatabases()
                self.loadData()
                self.finalize()

            if self._mode == 'qserv':
                # restart xrootd in order to reload  export paths w.r.t loaded chunks, cf. #2478
                self.restart('xrootd')

                # Qserv fails to start if QMS is empty, so starting it again may be required
                self.restart('qserv-master')

            self.runQueries(stop_at_query)



    def restart(self,service_name):

        initd_path = os.path.join(self.config['qserv']['base_dir'],'etc','init.d')
        daemon_script = os.path.join(initd_path,service_name)
        out = os.system("%s stop" % daemon_script)
        out = os.system("%s start" % daemon_script)

    def areQueryResultsEquals(self):

        outputs_dir = os.path.join(self._out_dirname, "outputs")

        mysql_out_dir = os.path.join(outputs_dir,"mysql")
        qserv_out_dir = os.path.join(outputs_dir,"qserv")

        dcmp = dircmp( mysql_out_dir, qserv_out_dir)

        if len(dcmp.diff_files)==0:
            return True
        else:
            for name in dcmp.diff_files:
                self.logger.info("diff_file %s found in %s and %s" % (name, dcmp.left, dcmp.right))
            return False

def parseOptions():
    op = optparse.OptionParser()
    op.add_option("-i", "--case-no", dest="case_no",
              default="01",
              help="test case number")
    mode_option_values = ['mysql','qserv','all']
    op.add_option("-m", "--mode", type='choice', dest="mode", choices=mode_option_values,
              default='all',
              help= "Qserv test modes (direct mysql connection, or via qserv) : '" +
              "', '".join(mode_option_values) +
              "' [default: %default]")
    op.add_option("-c", "--config-dir", dest="config_dir",
            help= "Path to directory containing qserv-build.conf and"
            "qserv-build.default.conf")
    op.add_option("-s", "--stop-at-query", type="int", dest="stop_at_query",
              default = 7999,
              help="Stop at query with given number"  +
              "' [default: %default]")
    op.add_option("-l", "--load", action="store_true", dest="load_data",default=False,
              help="Run queries on previously loaded data"  +
              "' [default: %default]")
    op.add_option("-o", "--out-dir", dest="out_dirname",
              help="Full path to directory for storing temporary results. The results will be stored in <OUTDIR>/qservTest_case<CASENO>/")
    options = op.parse_args()[0]

    if options.mode=='all':
        options.mode_list = ['mysql','qserv']
    else:
        options.mode_list = [options.mode]

    return options
