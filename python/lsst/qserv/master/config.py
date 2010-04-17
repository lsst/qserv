# config.py module for qserv configuration file parsing and defaults
# 
# It should be the "executable" python code's responsibility to invoke
# loading, since it should be the one parsing arguments. 

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
        self.reason = value
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
