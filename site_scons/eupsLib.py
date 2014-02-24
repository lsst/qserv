#
# LSST Data Management System
# Copyright 2012-2014 LSST Corporation.
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
# eupsLib.py : adapter for importing dependency paths from EUPS
#
# (formerly eups.py, but renamed to prevent clashes with importing
# real ExtUPS by "import eups")
#

eupsManaged = [
    "PROTOBUF",
    "MYSQL",
    "BOOST",
    "XROOTD",
    "TWISTED",
    "LUA",
    "LUASOCKET",
    "LUAEXPAT",
    "LUAXMLRPC",
    "MYSQLPROXY",
    "ZOPEINTERFACE", # for twisted
    # need GEOMETRY lib as well.
]

class Importer:
    def __init__(self):
        self.includes = []
        self.pythonPaths = []
        pass

    def _addIncDir(self, incDir):
        self.includes = []
        pass
    def _addPyPath(self, pyPath):
        self.pythonPaths = []
        pass

    def importDeps(self, env):
        # protobuf, crypto, ssl, mysqlclient_r,
        # boost_regex, boost_thread, boost_signals, xrootd
        paths = { "include" : self._addIncDir,
                  "lib/python/site-packages" : self._addPyPath }

        for p in eupsProducts:
            # EUPS sets the lib path and bin path, so we don't have to.
            # But we need the inc path.
            prodDir = p + "_DIR"
            if prodDir in os.environ:
                for (pSuffix, action) in paths:
                    subDir = os.path.join(os.environ[prodDir], pSuffix)
                    try:
                        if os.listdir(subDir): action(subDir)
                    except: # Leave alone, can't access for whatever reason.
                        pass

        pass
    def getIncludes(self):
        return self.includes
    def getPythonPaths(self):
        return self.pythonPaths

    pass

