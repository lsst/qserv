# 
# LSST Data Management System
# Copyright 2008-2014 AURA/LSST.
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
# and parameters for the qserv czar's operation.  Currently, it does
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

# Package
from lsst.qserv.czar import StringMap # C++ STL map<string,string>


# Defaults for the configuration itself
defaultFilename = "/etc/qserv.cnf"
envFilename = None
envFilenameVar = "QSERV_CONFIG"

# qserv built-in defaults:
# Note that section names and key names are lower-cased by python.
defaultConfig = StringIO("""\
[frontend]
xrootd=localhost:1094
scratch_path=/dev/shm/qserv
port=7080

[mgmtdb]
db=qservMeta
# Steal resultdb settings for now.

[css]
# allowed values:zoo, mem. Default is zoo
technology=zoo

# For zoo, provide zookeeper connection information
# For mem, provide location of file containing key-value pairs
# (to learn how to dump key-value pairs, see
# qserv/core/modules/css/KvInterfaceImplMem.cc)
# TODO : localhost doesn't work on Ubuntu 14.04, why ?
connection=127.0.0.1:2181
# Timeout in milliseconds
timeout=10000

[resultdb]
host=
port=0
unix_socket=/u1/local/mysql.sock
db=qservResult
user=qsmaster
passwd=

[partitioner]
emptyChunkListFile=

[tuning]
memoryEngine=yes

[debug]
chunkLimit=-1

[mysql]
mysqlclient=

[log]
logConfig=
""")
# Note: It is important to have defaults for config variables specified
#  here. Completely missing sections and keys will raise exceptions
#  that, even if caught properly, will be reported by errors by
#  Python's unittest framework.

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

def getStringMap():
    m = StringMap()
    for s in config.sections():
        for (k,v) in config.items(s):
            m[s + "." + k] = v
    return m

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
    def readOrDie(fn):
        assert os.access(fn, os.R_OK)
        config.read(fn)
    if getattr(filename, '__iter__', False):
        map(readOrDie, filename) # Load a list of filenames
        loadedFile = filename[-1] # Remember the last loaded
    elif filename:
        readOrDie(filename)
        loadedFile = filename
    # For null/false filenames, skip reading and use defaults
    pass

# Static initialization
_initialize()

# Self-test
if __name__ == "__main__":
    load()
    printTo(sys.stdout)
