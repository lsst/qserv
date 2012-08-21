# 
# LSST Data Management System
# Copyright 2012 LSST Corporation.
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

import os, subprocess, sys

def detectProtobufs():
    """Checks for protobufs support (in the broadest sense)
    If PROTOC, PROTOC_INC and PROTOC_LIB do not exist as environment 
    variables, try to autodetect a system-installed ProtoBufs and set the
    environment variable accordingly. No other values are modified or 
    returned."""

    if not (os.environ.has_key("PROTOC")
            and os.environ["PROTOC_INC"] 
            and os.environ["PROTOC_LIB"]):
        try:
            output = subprocess.Popen(["protoc", "--version"], 
                                      stdout=subprocess.PIPE).communicate()[0]
            guessRoot = "/usr"
            incPath = os.path.join([guessRoot]
                                   + "include/google/protobuf".split("/"))
            testInc = os.path.join(incPath, "message.h")
            libPath = os.path.join(guessRoot, "lib")
            testLib = os.path.join(libPath, "libprotobuf.a")
            assert os.access(testInc, os.R_OK) and os.access(testLib, os.R_OK)
            print "Using guessed protoc and paths."
        except:
            print """
Can't continue without Google Protocol Buffers.
Make sure PROTOC, PROTOC_INC, and PROTOC_LIB env vars are set.
e.g., PROTOC=/usr/local/bin/protoc 
      PROTOC_INC=/usr/local/include 
      PROTOC_LIB=/usr/local/lib"""
            raise StandardError("FATAL ERROR: Can't build protocol without ProtoBufs")
        pass
    # Print what we're using.
    print "Protocol buffers using protoc=%s with lib=%s and include=%s" %(
        os.environ["PROTOC"], os.environ["PROTOC_INC"], 
        os.environ["PROTOC_LIB"])

def composeEnv(env, roots=[], includes=[], libs=[]):
    assert env
    env.Append(CPPPATH=includes)
    env.Append(CPPPATH=[os.path.join(x, "include") for x in roots])
    env.Append(LIBPATH=libs)    
    env.Append(LIBPATH=[os.path.join(x, "lib") for x in roots])
    return env

def checkMySql(env, Configure):
    """Checks for MySQL includes and libraries in the following directories:
    * each prefix in searchRoots (lib/, lib64/, include/)
    * a built MySQL directory specified by the env var MYSQL_ROOT
    Must pass Configure class from SCons
    """
    if os.environ.has_key('MYSQL_ROOT'):
        mysqlRoots = [os.environ['MYSQL_ROOT']]
        env.Prepend(CPPPATH=[os.path.join(mysqlRoots[0], "include")])
        # Look for mysql sub-directories. lib64 is important on RH/Fedora
        searchLibs = filter(os.path.exists, 
                            [os.path.join(r, lb, "mysql") 
                             for r in mysqlRoots for lb in ["lib","lib64"]])
        if searchLibs:
            env.Prepend(LIBPATH=searchLibs)
        pass

    conf = Configure(env)
    if conf.CheckLibWithHeader("mysqlclient_r", "mysql/mysql.h",
                                   language="C++"):
        if conf.CheckDeclaration("mysql_next_result", 
                                 "#include <mysql/mysql.h>","c++" ):
            return conf.Finish()
        else:
            print >> sys.stderr, "mysqlclient too old. (check MYSQL_ROOT)."
    else:
        print >> sys.stderr, "Could not locate MySQL headers (mysql/mysql.h)"\
            + " or find multithreaded mysql lib(mysqlclient_r)"
    # MySQL support not found or inadequate.
    return None

def guessMySQL(env):
    """Guesses the detected mysql dependencies based on the environment.
    Would be nice to reuse conf.CheckLib, but that doesn't report what 
    actually got detected, just that it was available. This solution 
    doesn't actually check beyond simple file existence.
    Returns (includepath, libpath, libname) """
    libName = "mysqlclient_r"
    libp = env["LIBPREFIX"]
    libs = env["SHLIBSUFFIX"]
    foundLibs = filter(lambda (p,f): os.path.exists(f),
                       [(p, os.path.join(p, libp + libName + libs))
                         for p in env["LIBPATH"]])
    assert foundLibs
    foundIncs = filter(os.path.exists, 
                       [os.path.join(p, "mysql/mysql.h") 
                        for p in env["CPPPATH"]])
    assert foundIncs
    return (foundIncs[0], foundLibs[0][0], libName)
    
# Xrootd/Scalla search helper
class XrdHelper:
    def __init__(self, roots):
        self.cands = roots
        if os.environ.has_key('XRD_DIR'):
            self.cands.insert(0, os.environ['XRD_DIR'])

        self.platforms = ["x86_64_linux_26", "x86_64_linux_26_dbg",
                          "i386_linux26", "i386_linux26_dbg"]
        if os.environ.has_key('XRD_PLATFORM'):
            self.platforms.insert(0, os.environ['XRD_PLATFORM'])
        pass

    def getXrdLibInc(self):
        for c in self.cands:
            (inc, lib) = (self._findXrdInc(c), self._findXrdLib(c))
            if inc and lib:
                return (inc, lib)
        return (None, None)

    def _findXrdLib(self, path):
        for p in self.platforms:
            libpath = os.path.join(path, "lib", p)
            if os.path.exists(libpath):
                return libpath
        return None

    def _findXrdInc(self, path):
        paths = map(lambda p: os.path.join(path, p), ["include/xrootd", "src"])
        for p in paths:
            neededFile = os.path.join(p, "XrdPosix/XrdPosix.hh")
            if os.path.exists(neededFile):
                return p
        return None
    pass
XRDFLAGS = ["-D_LARGEFILE_SOURCE", "-D_LARGEFILE64_SOURCE",
            "-D_FILE_OFFSET_BITS=64", "-D_REENTRANT",]

## Boost checker
def checkBoostHeader(conf, pkgList=[]):
    for p in pkgList:
        if not conf.CheckCXXHeader('boost/' + p + '.hpp'):
            return False
    return True
    
def checkAddBoost(conf, lib):
    """Check for a boost lib with various suffixes and add it to a Configure
    if found. (e.g. 'boost_regex' or 'boost_thread')"""
    return (conf.CheckLib(lib + "-gcc34-mt", language="C++") 
            or conf.CheckLib(lib + "-gcc41-mt", language="C++") \
            or conf.CheckLib(lib, language="C++") \
            or conf.CheckLib(lib + "-mt", language="C++"))

def checkAddAntlr(conf):
    found = conf.CheckLibWithHeader("antlr", "antlr/AST.hpp", 
                                    language="C++")
    if not found:
        print >> sys.stderr, "Could not locate libantlr or antler/AST.hpp"
    return found

def composeEnv(env, roots=[], includes=[], libs=[]):
    env.Append(CPPPATH=[os.path.join(x, "include") for x in roots])
    env.Append(CPPPATH=includes)
    env.Append(LIBPATH=[os.path.join(x, "lib") for x in roots])
    env.Append(LIBPATH=libs)
    return env

def checkAddSslCrypto(conf):
    found =  conf.CheckLib("ssl") and conf.CheckLib("crypto")
    if not found:
        print >> sys.stderr, "Could not locate libssl or libcrypto"
    return found

def checkAddMySql(conf):
    found = conf.CheckLibWithHeader("mysqlclient_r", "mysql/mysql.h", 
                                    language="C++")
    if not found:
        print >> sys.stderr, "Could not find  mysql/mysql.h or",
        print >> sys.stderr, "multithreaded MySQL (mysqlclient_r)"
    return found

def checkAddXrdPosix(conf):
    found = conf.CheckLibWithHeader("XrdPosix", 
                                    "XrdPosix/XrdPosixLinkage.hh",
                                    language="C++")
    if not found:
        print >> sys.stderr, "Could not find XrdPosix lib/header"
    return found
########################################################################
# dependency propagation tools
def importDeps(env, f):
    post = {}
    fName = f+".deps"
    if os.access(fName, os.R_OK):
        deps = eval(open(fName).read()) # import dep file directly
        if "LIBS" in deps:
            post["LIBS"] = deps.pop("LIBS")
        #print "imported deps", deps
        env.Append(**deps)
    return post

def mergeDict(d1, d2):
    """Merge list values from d2 to d1"""
    for k in d2:
        if k in d1: d1[k].extend(d2[k])
        else: d1[k] = d2[k]
    return d1

def checkLibsFromDict(conf, depDict, autoadd=1):
    if "LIBS" in depDict:
        for lib in depDict["LIBS"]:
            conf.CheckLib(lib, language="C++", autoadd=autoadd)
    return conf
