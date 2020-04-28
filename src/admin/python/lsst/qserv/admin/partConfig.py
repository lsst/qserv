# LSST Data Management System
# Copyright 2014-2016 AURA/LSST.
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
Module defining PartConfig class and related methods.

PartConfig class reads bunch of configuration files and collecs
all parameters related to table partitioning.

@author  Andy Salnikov, SLAC
"""

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging
import itertools

# -----------------------------
# Imports for other modules --
# -----------------------------
from lsst.qserv.admin.configParser import ConfigParser

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

# ------------------------
# Exported definitions --
# ------------------------


class PartConfig(dict):
    """
    Class which holds all table partition options.
    Implemented as a dictionary with some extra methods.
    """

    # keys that must be defined in partitioner config files for any table
    requiredConfigKeys = []
    # keys that must be defined for refMatch tables
    requiredRefMatchKeys = ['part.num-stripes', 'part.num-sub-stripes',
                            'part.default-overlap', 'dirTable1', 'dirColName1',
                            'dirTable2', 'dirColName2']
    # keys that must be defined for partitioned non-refMatch tables
    requiredPartKeys = ['part.num-stripes', 'part.num-sub-stripes',
                        'part.default-overlap', 'part.overlap',
                        'dirDb', 'dirTable']

    def __init__(self, files):
        """
        Process all config files, throws on error
        """

        dict.__init__(self)

        def _options(group):
            '''massage options list, takes list of (key, opt) pairs'''
            options = list(opt for k, opt in group)
            if len(options) == 1:
                options = options[0]
            return options

        for config in files:

            # parse whole thing
            try:
                cfgParser = ConfigParser(open(config))
                options = cfgParser.parse()
            except Exception as ex:
                logging.error('Failed to parse configuration file: %r', ex)
                raise

            # options are returned as a list of (key, value) pairs, there will be
            # more than one key appearance for some options, merge this together and
            # make a dict out of it
            options.sort()
            options = dict((key, _options(group)) for key, group
                           in itertools.groupby(options, lambda pair: pair[0]))

            # in partitioner config files loaded earlier have higher priority
            # (options are not overwritten by later configs), do the same here
            for k, v in options.items():
                if k not in self:
                    self[k] = v

        # check that we have a set of required options defined
        missing = [key for key in self.requiredConfigKeys if key not in self]
        if self.isRefMatch:
            missing += [key for key in self.requiredRefMatchKeys if key not in self]
        elif self.partitioned:
            missing += [key for key in self.requiredPartKeys if key not in self]
        if missing:
            logging.error('Required options are missing from configuration files: %r',
                          ' '.join(missing))
            raise KeyError('required options are missing: ' + ' '.join(missing))

    @property
    def partitioned(self):
        """Returns True if table is partitioned"""
        return 'part.pos' in self or 'part.pos1' in self

    @property
    def isSubChunked(self):
        """
        Returns True if table is sub-chunked. By default all partitioned tables
        are sub-chunked unles part.subChunks is set to 0
        """
        return self.partitioned and bool(int(self.get('part.subChunks', 1)))

    @property
    def isRefMatch(self):
        """Returns True if table is a match table"""
        return 'part.pos1' in self and 'part.pos2' in self

    @property
    def isView(self):
        """Returns True if table is a view"""
        return bool(self.get('view', False))

    def isDirector(self, dbName, tableName):
        """
        Returns True if dbName.tableName is a director table. Director table name
        is determined by dirDb and dirTable parameters, if dirTable is not set then
        Object is assumed to be a director table.
        """
        return not self.isRefMatch and dbName == self['dirDb'] and tableName == self['dirTable']

    def cssDbOptions(self):
        """
        Returns dictionary of CSS options for database.
        """
        options = {'partitioningStrategy': "sphBox",
                   'nStripes': int(self['part.num-stripes']),
                   'nSubStripes': int(self['part.num-sub-stripes']),
                   'overlap': float(self['part.default-overlap']),
                   'storageClass': self.get('storageClass', 'L2')
                   }
        return options

    def cssTableOptions(self):
        """
        Returns dictionary of CSS options for a table.
        """
        options = {'compression': False,
                   'match': False
                   }

        if self.isRefMatch:

            # refmatch table
            options['match'] = True
            options['dirTable1'] = self['dirTable1']
            options['dirColName1'] = self['dirColName1']
            options['dirTable2'] = self['dirTable2']
            options['dirColName2'] = self['dirColName2']
            options['flagColName'] = self['flagColName']
            options['subChunks'] = int(self.get('part.subChunks', 1))

        elif self.partitioned:

            # partitioned table
            pos = self['part.pos'].split(',')
            raCol, declCol = pos[0].strip(), pos[1].strip()
            options['latColName'] = declCol
            options['lonColName'] = raCol
            options['overlap'] = float(self['part.overlap'])
            options['subChunks'] = bool(int(self.get('part.subChunks', 1)))
            options['dirDb'] = self['dirDb']
            options['dirTable'] = self['dirTable']
            options['lockInMem'] = bool(self.get('sharedScan.lockInMem', False))
            options['scanRating'] = int(self.get('sharedScan.scanRating', 0))
            if 'dirColName' in self:
                options['dirColName'] = self['dirColName']
            else:
                # for director table use id column name instead
                options['dirColName'] = self['id']

        return options
