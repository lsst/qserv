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

[resultdb]
#host=localhost
#port=8000
unix_socket=/data/lsst/run/mysql.sock
db=test
""")


# Module variables:
config = None
loadedFile = None

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

####################################################
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
