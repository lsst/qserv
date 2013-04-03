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


"""
config.py : a module for qserv configuration file parsing and defaults
"""

# Standard Python
import ConfigParser
from cStringIO import StringIO
import os
import sys

# Defaults for the configuration itself
defaultFilename = "/etc/qms.cnf"
envFilename = None
envFilenameVar = "QMS_CONFIG"

# qserv metadata server built-in defaults:
# Note that section names and key names are lower-cased by python.
defaultQmsConfig = StringIO("""\
[qmsFrontend]
port=7082

[qmsdb]
host=
port=0
unix_socket=/valid/path/here/mysql.sock
db=qservMetadata
user=qms
passwd=notShowing

[logging]
outFile=/tmp/qms.log
level=warning
""")

# Module variables:
config = None
loadedFile = None

######################################################################
## Methods
######################################################################
def load(filename=None):
    if filename:
        _loadFile(filename)
    elif envFilename:
        _loadFile(envFilename)
    elif defaultFilename:
        _loadFile(defaultFilename)
    else:
        _loadFile([])
    pass

def printTo(outHandle):
    config.write(outHandle)
    pass

######################################################################
## Error classes
######################################################################
class ConfigError(Exception):
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)

######################################################################
## Local
######################################################################
def _initialize():
    """Performs some static initialization upon loading."""
    global envFilenameVar
    global envFilename
    if os.environ.has_key(envFilenameVar):
        envFilename = os.environ[envFilenameVar]
    pass

def _loadFile(filename):
    global loadedFile
    global config
    loadedFile = None
    config = ConfigParser.ConfigParser()
    config.readfp(defaultQmsConfig)    # Read built-in defaults first
    if getattr(filename, '__iter__', False):
        if not os.access(filename, os.R_OK):
            print "Unable to load %s" % filename
        map(config.read, filename) # Load a list of filenames
        loadedFile = filename[-1] # Remember the last loaded
    else:
        if not os.access(filename, os.R_OK):
            print "Unable to load %s" % filename
        config.read(filename)
        loadedFile = filename
    pass

# Static initialization
_initialize()

# Self-test
if __name__ == "__main__":
    load()
    printTo(sys.stdout)
