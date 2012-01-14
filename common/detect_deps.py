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
    foundLibs = filter(os.path.exists, 
                       [os.path.join(p, libp + libName + libs) 
                         for p in env["LIBPATH"]])
    assert foundLibs
    foundIncs = filter(os.path.exists, 
                       [os.path.join(p, "mysql/mysql.h") 
                        for p in env["CPPPATH"]])
    assert foundIncs
    return (foundIncs[0], foundLibs[0], libName)
    
