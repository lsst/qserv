from __future__ import print_function
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

import os
import state


def checkMySql(env):
    """Checks for MySQL includes and libraries in the following directories:
    * each prefix in searchRoots (lib/, lib64/, include/)
    * a built MySQL directory specified by the env var MYSQL_ROOT
    """
    conf = env.Configure()
    state.log.debug("checkMySql():\n" +
                    "\tCPPPATH : %s\n" % env['CPPPATH'] +
                    "\tLIBPATH : %s" % env['LIBPATH'])

    if not conf.CheckCXXHeader('mysql/mysql.h'):
        state.log.fail("Could not locate MySQL headers (mysql/mysql.h)")

    if conf.CheckLibWithHeader("mysqlclient_r", "mysql/mysql.h",
                               language="C++", autoadd=0):
        if conf.CheckDeclaration("mysql_next_result",
                                 "#include <mysql/mysql.h>", "c++"):
            conf.Finish()
            return True
        else:
            state.log.fail("mysqlclient too old")
    else:
        # MySQL support not found or inadequate.
        state.log.fail("Could not locate MySQL headers (mysql/mysql.h)"
                       + " or find multithreaded mysql lib (mysqlclient_r)")

    conf.Finish()
    return None


class BoostChecker:

    def __init__(self, env):
        self.env = env
        self.suffix = None
        # TODO: this list is hard-coded for now, there may be variations in the future
        self.suffixes = ["-gcc41-mt", "-gcc34-mt", "-mt", ""]
        self.cache = {}

    def getLibName(self, libName):
        '''
        For a given generic name (such as 'boost_system') find and return a
        variant with optional suffix. If BOOST_LIB is set in the environment
        then use only that location to check for matching libraries. Otherwise
        ask scons to find it in any accessible location (depends on linker).
        '''

        # check cached name first
        lib = self.cache.get(libName)
        if lib is None:
            lib = self._getLibName(libName)
        self.cache[libName] = lib
        return lib

    def _getLibName(self, libName):
        '''
        Returns suffixed name of the library, if the suffix is not known yet then
        try to guess it by looking at the existing library names. Same suffix is
        reused for all boost libraries.
        '''

        # helper internal method to check for files
        def _libCheckFile(dirname, libname, env):
            ''' Check that library exists in a specified directory, tries all known prefix/suffix combinations '''
            for pfx in env['LIBPREFIXES']:
                for sfx in env['LIBSUFFIXES']:
                    path = os.path.join(env.subst(dirname), env.subst(pfx) + libname + env.subst(sfx))
                    if os.path.exists(path):
                        return True
            return False

        if self.suffix is None:
            # need to guess correct suffix

            if 'BOOST_LIB' in self.env:

                # if BOOST_LIB is set then we only look inside that directory
                for sfx in self.suffixes:
                    if _libCheckFile('$BOOST_LIB', libName + sfx, self.env):
                        self.suffix = sfx
                        break

                if self.suffix is None:
                    state.log.fail(
                        "Failed to find boost library `"+libName+"' in BOOST_LIB="+self.env.subst("$BOOST_LIB"))

            else:

                # we are probably using system-installed boost, just use scons autotools to
                # try to locate correct libraries
                conf = self.env.Configure()

                def checkSuffix(sfx):
                    return conf.CheckLib(libName + sfx, language="C++", autoadd=0)

                for sfx in self.suffixes:
                    if checkSuffix(sfx):
                        self.suffix = sfx
                        break
                conf.Finish()

                if self.suffix is None:
                    state.log.fail("Failed to find boost library "+libName)

        return libName + self.suffix

    pass  # BoostChecker


class AntlrChecker:

    def __init__(self, env):
        self.env = env
        self.suffix = None
        self.suffixes = ["-pic", ""]
        self.cache = {}
        pass

    def getLibName(self, libName):
        if libName in self.cache:
            return self.cache[libName]

        r = self._getLibName(libName)
        self.cache[libName] = r
        return r

    def _getLibName(self, libName):
        if self.suffix is None:
            conf = self.env.Configure()

            def checkSuffix(sfx):
                return conf.CheckLib(libName + sfx, language="C++", autoadd=0)
            for i in self.suffixes:
                if checkSuffix(i):
                    self.suffix = i
                    break
            if self.suffix is None:
                state.log.fail("Failed to find libantlr : "+libName)
            conf.Finish()
            pass
        return libName + self.suffix
    pass  # AntlrChecker


null_source_file = """
int main(int argc, char **argv) {
        return 0;
}
"""


def checkLibs(context, libList):
    lastLIBS = context.env['LIBS']
    state.log.debug("checkLibs() %s" % lastLIBS)
    context.Message('checkLibs() : Checking for %s...' % ",".join(libList))
    context.env.Append(LIBS=libList)
    result = context.TryLink(null_source_file, '.cc')
    context.Result(result)
    context.env.Replace(LIBS=lastLIBS)
    return result


# Look for xrootd headers
def findXrootdInclude(env):
    hdrName = os.path.join("XrdSsi", "XrdSsiErrInfo.hh")
    conf = env.Configure()

    if conf.CheckCXXHeader(hdrName):  # Try std location
        conf.Finish()
        return (True, None)

    pList = env.get("CPPPATH", [])
    pList.append("/usr/include")
    # pList.append("/usr/local/include")
    for p in pList:
        path = p
        if not path.endswith("xrootd"):
            path = os.path.join(p, "xrootd")
        if os.access(os.path.join(path, hdrName), os.R_OK):
            conf.Finish()
            return (True, path)
    conf.Finish()
    return (False, None)


def findXrootdLibPath(libName, pathList):
    fName = "lib%s.so" % (libName)
    for p in pathList:
        path = p
        if os.access(os.path.join(path, fName), os.R_OK):
            return path
    print("Couldn't find " + libName)
    return None


def checkXrootdLink(env, autoadd=0):
    libList = "XrdUtils XrdClient XrdPosix XrdPosixPreload".split()
 #   libList = "XrdPosix".split()
    header = "XrdSsi/XrdSsiErrInfo.hh"

    # print "ldpath", env.Dump("LIBPATH")
    xrdLibPath = findXrootdLibPath("XrdCl", env["LIBPATH"])
    if xrdLibPath:
        env.Append(RPATH=[xrdLibPath])
    conf = env.Configure(custom_tests={
        'CheckLibs': lambda c: checkLibs(c, libList)})

    found = conf.CheckLibs() and conf.CheckCXXHeader(header)
    conf.Finish()
    if not found:
        state.log.fail("Missing at least one xrootd lib or header file")
    return found


def setXrootd(env):
    (found, path) = findXrootdInclude(env)
    if not found:
        state.log.fail("Missing Xrootd include path")
    elif found and path:
        env.Append(CPPPATH=[path])
    return found


# --extern root handling
def _addInst(env, root):
    iPath = os.path.join(root, "include")
    if os.path.isdir(iPath):
        env.Append(CPPPATH=[iPath])
    for e in ["lib", "lib64"]:
        ep = os.path.join(root, e)
        if os.path.isdir(ep):
            env.Append(LIBPATH=[ep])
        pass


def addExtern(env, externPaths):
    if externPaths:
        for p in externPaths.split(":"):
            _addInst(env, p)
    pass


#
# custom.py mechanism
#
def importCustom(env, extraTgts):

    def getExt(ext):
        varNames = [s for s in env.Dictionary() if s.endswith(ext)]
        vals = [env[varName] for varName in varNames]
        state.log.debug("varNames : %s, vals %s" % (varNames, vals))
        return vals

    env.Append(LIBPATH=getExt("_LIB"))  # *LIB --> LIBPATH
    env.Append(CPPPATH=getExt("_INC"))  # *INC --> CPPPATH

    state.log.debug("importCustom():\n" +
                    "\tCPPPATH : %s\n" % env['CPPPATH'] +
                    "\tLIBPATH : %s" % env['LIBPATH'])

    # Automagically steal PYTHONPATH from envvar
    extraTgts["PYTHONPATH"] = env.get("PYTHONPATH", [])
    return None
