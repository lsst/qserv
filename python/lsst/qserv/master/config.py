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

# config.py : a module for qserv configuration file parsing and defaults
# 
# The config module should contain all possible configuration options
# and parameters for the qserv master's operation.  Currently, it does
# not include configuration of the xrootd system, nor does it include
# configuration of workers (which are configured via environment
# variables). 
# 
# A sample configuration is included in this module, and should be
# be similar to the sample configuration in examples/lsst-dev01.qserv.cnf
# 
# It should be the "executable" python code's (e.g., startQserv.py's)
# responsibility to invoke loading, since it should be the one parsing
# arguments.  

# Standard Python
import ConfigParser
from cStringIO import StringIO
import os
import sys

# Defaults for the configuration itself
defaultFilename = "/etc/qserv.cnf"
envFilename = None
envFilenameVar = "QSERV_CONFIG"

# qserv built-in defaults:
defaultConfig = StringIO("""\
[frontend]
xrootd=lsst-dev01:1094
xrootd_user=qsmaster
scratch_path=/dev/shm/qserv
port=7080

[mgmtdb]
db=qservMeta
# Steal resultdb settings for now.

[resultdb]
host=
port=0
unix_socket=/u1/local/mysql.sock
db=qservResult
user=qsmaster
passwd=

[partitioner]
stripes=18
substripes=10

[table]
chunked=Source,ForcedSource
subchunked=Object

[mysql]
mysqlclient=

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
    """An error in qserv configuration (Bad/missing values)."""
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)


######################################################################
## Local
######################################################################
def _initialize():
    "Perform some static initialization upon loading"
    if os.environ.has_key(envFilenameVar):
        envFilename = os.environ[envFilenameVar]
    
    pass

def _loadFile(filename):
    global loadedFile
    global config
    loadedFile = None
    config = ConfigParser.ConfigParser()
    config.readfp(defaultConfig) # Read built-in defaults first
    if getattr(filename, '__iter__', False):
        map(config.read, filename) # Load a list of filenames
        loadedFile = filename[-1] # Remember the last loaded
    else:
        config.read(filename)
        loadedFile = filename
    pass

# Static initialization
_initialize()

# Self-test
if __name__ == "__main__":
    load()
    printTo(sys.stdout)
