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
Module defining PartConfig class and related methods.

PartConfig class reads bunch of configuration files and collecs
all parameters related to table partitioning.

@author  Andy Salnikov, SLAC
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging
import itertools
import UserDict

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.qserv.admin.configParser import ConfigParser

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

class PartConfig(UserDict.UserDict):
    """
    Class which holds all table partition options.
    Implemented as a dictionary with some extra methods.
    """

    # keys that must be defined in partitioner config files
    requiredConfigKeys = ['part.num-stripes', 'part.num-sub-stripes']

    def __init__(self, files):
        """
        Process all config files, throws on error
        """

        UserDict.UserDict.__init__(self)

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
                logging.error('Failed to parse configuration file: %s', ex)
                raise

            # options are returned as a list of (key, value) pairs, there will be
            # more than one key appearance for some options, merge this together and
            # make a dict out of it
            options.sort()
            options = dict((key, _options(group)) for key, group
                           in itertools.groupby(options, lambda pair: pair[0]))

            # in partitioner config files loaded earlier have higer priority
            # (options are not overwritten by later configs), do the same here
            options.update(self.data)
            self.data = options

        # check that we have a set of required options defined
        for key in self.requiredConfigKeys:
            if key not in self.data:
                logging.error('Required option is missing from configuration files: %s', key)
                raise KeyError('missing required option')

    @property
    def partitioned(self):
        """Returns True if table is partitioned"""
        return 'part.pos' in self.data or 'part.pos1' in self.data

    @property
    def isView(self):
        """Returns True if table is a view"""
        return bool(self.data.get('view', False))

    def cssDbOptions(self):
        """
        Returns dictionary of CSS options for database.
        """
        options = {'nStripes': self['part.num-stripes'],
                   'nSubStripes': self['part.num-sub-stripes'],
                   'storageClass': self.get('storageClass', 'L2')
                   }
        return options

    def cssTableOptions(self):
        """
        Returns dictionary of CSS options for a table.
        """
        options = {'compression': '0',
                   'match': '0'
                   }

        # refmatch table has part.pos1 instead of part.pos, CSS expects a string, not a number
        isRefMatch = 'part.pos1' in self and 'part.pos2' in self

        if 'part.pos' in self:

            # partitioned table
            pos = self['part.pos'].split(',')
            raCol, declCol = pos[0].strip(), pos[1].strip()
            options['latColName'] = declCol
            options['lonColName'] = raCol
            options['overlap'] = self['part.overlap']
            options['subChunks'] = self.get('part.subChunks', '1')
            options['dirTable'] = self.get('dirTable', 'Object')
            options['dirColName'] = self.get('dirColName', 'objectId')

        elif 'part.pos1' in self and 'part.pos2' in self:

            # refmatch table
            options['match'] = '1'
            options['dirTable1'] = self.get('dirTable1', 'Object')
            options['dirColName1'] = self.get('dirColName1', 'objectId')
            options['dirTable2'] = self.get('dirTable2', 'Object')
            options['dirColName2'] = self.get('dirColName2', 'objectId')

        return options
