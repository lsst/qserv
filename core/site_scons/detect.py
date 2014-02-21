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
# detect_deps.py handles dependency detection for Qserv software

import SCons
import os, subprocess, sys

def checkMySql(env):
    """Checks for MySQL includes and libraries in the following directories:
    * each prefix in searchRoots (lib/, lib64/, include/)
    * a built MySQL directory specified by the env var MYSQL_ROOT
    """
    conf = env.Configure()
    print "DEBUG checkMySql() %s %s" % (env["LIBPATH"], env["CPPPATH"])

    if conf.CheckLibWithHeader("mysqlclient_r", "mysql/mysql.h",
                                   language="C++", autoadd=0):
        if conf.CheckDeclaration("mysql_next_result",
                                 "#include <mysql/mysql.h>","c++" ):
            conf.Finish()
            return True
        else:
            print >> sys.stderr, "mysqlclient too old. (check MYSQL_ROOT)."
    else:
        # MySQL support not found or inadequate.
        print >> sys.stderr, "Could not locate MySQL headers (mysql/mysql.h)"\
            + " or find multithreaded mysql lib(mysqlclient_r)"

    conf.Finish()
    return None

class BoostChecker:
    def __init__(self, env):
        self.env = env
        self.suffix = None
        self.suffixes = ["-gcc41-mt", "-gcc34-mt", "-mt", ""]
        self.cache = {}
        pass

    def getLibName(self, libName):
        if libName in self.cache:
            return self.cache[libName]

        r = self._getLibName(libName)
        self.cache[libName] = r
        return r

    def _getLibName(self, libName):
        if self.suffix == None:
            conf = self.env.Configure()

            def checkSuffix(sfx):
                return conf.CheckLib(libName + sfx, language="C++", autoadd=0)
            for i in self.suffixes:
                if checkSuffix(i):
                    self.suffix = i
                    break
            if self.suffix == None:
                print "Can't find boost_" + libName
                assert self.suffix != None
            conf.Finish()
            pass
        return libName + self.suffix
    pass # BoostChecker


null_source_file = """
int main(int argc, char **argv) {
        return 0;
}
"""

def checkLibs(context, libList):
    lastLIBS = context.env['LIBS']
    print "DEBUG : checkLibs() %s" % lastLIBS
    context.Message('checkLibs() : Checking for %s...' % ",".join(libList))
    context.env.Append(LIBS=libList)
    result = context.TryLink(null_source_file, '.cc')
    context.Result(result)
    context.env.Replace(LIBS=lastLIBS)
    return result


## Look for xrootd headers
def findXrootdInclude(env):
    hdrName = os.path.join("XrdPosix","XrdPosixLinkage.hh")
    conf = env.Configure()
    foundPath = None

    if conf.CheckCXXHeader(hdrName): # Try std location
        conf.Finish()
        return (True, None)

    # Extract CPPPATHs and look for xrootd/ within them.
    pList = env.Dump("CPPPATH") # Dump returns a stringified list

    # Convert to list if necessary
    if pList and type(pList) == type("") and str(pList)[0] == "[":
        pList = eval(pList)
    elif type(pList) != type(list): pList = [pList] # Listify
    pList.append("/usr/include")
    #pList.append("/usr/local/include")
    for p in pList:
        path = os.path.join(p, "xrootd")
        if os.access(os.path.join(path, hdrName), os.R_OK):
            conf.Finish()
            return (True, path)
    conf.Finish()
    return (False,None)

def checkXrootdLink(env, autoadd=0):
    libList = "XrdUtils XrdClient XrdPosix XrdPosixPreload".split()
    header = "XrdPosix/XrdPosixLinkage.hh"

    conf = env.Configure(custom_tests={
            'CheckLibs' : lambda c: checkLibs(c,libList)})

    found = conf.CheckLibs() and conf.CheckCXXHeader(header)
    conf.Finish()
    if not found:
        print >> sys.stderr, "Missing at least one xrootd lib/header"
    return found


def setXrootd(env):
    (found, path) = findXrootdInclude(env)
    if not found :
        print >> sys.stderr, "Missing Xrootd include path"    
    elif found and path: 
        env.Append(CPPPATH=[found[1]])
    return found


def findMeta(env):
    metaFiles = "../../meta/python/lsst/qserv/meta/*py"
    files = env.Glob(metaFiles)
    return files

# --extern root handling
def _addInst(env, root):
    iPath = os.path.join(root, "include")
    if os.path.isdir(iPath): env.Append(CPPPATH=[iPath])
    for e in ["lib", "lib64"]:
        ep = os.path.join(root, e)
        if os.path.isdir(ep): env.Append(LIBPATH=[ep])
        pass

def addExtern(env, externPaths):
    if externPaths:
        for p in externPaths.split(":"):
            _addInst(env, p)
    pass


########################################################################
# custom.py mechanism
########################################################################
def importCustom(env, extraTgts):
    try:
        import custom
    except ImportError, e:
        return # Couldn't find module to import

    print "using custom.py"
    def getExt(ext):
        varNames = filter(lambda s: s.endswith(ext), dir(custom))
        vals = map(lambda i: getattr(custom, i), varNames)
        return vals
    env.Append(LIBPATH=getExt("LIB")) ## *LIB --> LIBPATH
    env.Append(CPPPATH=getExt("INC")) ## *INC --> CPPPATH

    # Automagically steal PYTHONPATH from envvar
    ppDirs = getExt("PYTHONPATH")
    #existPp = os.getenv("PYTHONPATH", None)
    #print "DEBUG : %s %s" % (existPp, ppDirs)
    #if existPp: ppDirs.prepend(existPp)
    pp = env.get("PYTHONPATH", [])
    pp.extend(ppDirs)
    extraTgts["PYTHONPATH"] = ppDirs
    print "Looking for protoc in custom"
    # Import PROTOC
    if "PROTOC" in dir(custom): env['PROTOC'] = custom.PROTOC
    else: print "didn't find protoc in custom."
    return custom

def extractGeometry(custom):
    geomLib = getattr(custom, "GEOMETRY", None)
    if geomLib:
        if os.access(geomLib, os.R_OK):
            return geomLib
        else:
            print "Invalid GEOMETRY entry in custom.py"
            Abort(1)
    pass

def checkTwisted():
    try:
        import twisted.internet
        print "twisted ok!"
        return True
    except ImportError, e:
        return None
    pass

########################################################################
# obsolete
########################################################################

