# -*- python -*-
#
# Setup our environment
#
# Do not change these
import glob, os, re, sys

env = Environment()

xrd_dir = "/scratch/xrd/xrootd";
if os.environ.has_key('XRD_DIR'):
    xrd_dir = os.environ['XRD_DIR']
if not os.path.exists(xrd_dir):
    xrd_dir = "/home/ktl/xrootd"
if not os.path.exists(xrd_dir):
    print >> sys.stderr, "Could not locate xrootd base directory"
    Exit(1)
xrd_platform = "x86_64_linux_26"
if os.environ.has_key('XRD_PLATFORM'):
    xrd_platform = os.environ['XRD_PLATFORM']
if not os.path.exists(os.path.join(xrd_dir, "lib", xrd_platform)):
    xrd_platform = "i386_linux26"
if not os.path.exists(os.path.join(xrd_dir, "lib", xrd_platform)):
    print >> sys.stderr, "Could not locate xrootd libraries"
    Exit(1)
env.Append(CPPPATH = [os.path.join(xrd_dir, "src")])
env.Append(LIBPATH = [os.path.join(xrd_dir, "lib", xrd_platform)])

boost_dir = "/u1/lsst/stack/Linux64/external/boost/1.37.0"
if os.environ.has_key('BOOST_DIR'):
    boost_dir = os.environ['BOOST_DIR']
if not os.path.exists(boost_dir):
    boost_dir = "/afs/slac/g/ki/lsst/home/DMS/Linux/external/boost/1.37.0"
if not os.path.exists(boost_dir):
    print >> sys.stderr, "Could not locate Boost base directory (BOOST_DIR)"
    Exit(1)
env.Append(CPPPATH = [os.path.join(boost_dir, "include")])
env.Append(LIBPATH = [os.path.join(boost_dir, "lib")])

if os.environ.has_key('SSL_DIR'):
    ssl_dir = os.environ['SSL_DIR']
    if os.path.exists(ssl_dir):
        env.Append(LIBPATH=[ssl_dir])

if os.environ.has_key('MYSQL_DIR'):
    mysql_dir = os.environ['MYSQL_DIR']
    if os.path.exists(mysql_dir):
        env.Prepend(LIBPATH=[os.path.join(mysql_dir,"lib","mysql")])
        env.Prepend(CPPPATH=[os.path.join(mysql_dir,"include")])


conf = Configure(env)

if not conf.CheckLib("ssl"):
    print >> sys.stderr, "Could not locate ssl"
    Exit(1)
if not conf.CheckLib("crypto"):
    print >> sys.stderr, "Could not locate crypto"
    Exit(1)

if not conf.CheckLibWithHeader("mysqlclient", "mysql/mysql.h", "c"):
    print >> sys.stderr, "Could not locate mysqlclient"
    Exit(1)
if not conf.CheckDeclaration("mysql_next_result", "#include <mysql/mysql.h>","c++" ):
    print >> sys.stderr, "mysqlclient too old. (check MYSQL_DIR)."
    Exit(1)
if not conf.CheckLibWithHeader("XrdSys", "XrdSfs/XrdSfsInterface.hh", "C++"):
    print >> sys.stderr, "Could not locate XrdSys"
    Exit(1)
if not conf.CheckCXXHeader("boost/regex.hpp"):
    print >> sys.stderr, "Could not locate Boost headers"
    Exit(1)
if not conf.CheckLib("boost_regex-gcc43-mt", language="C++") \
    and not conf.CheckLib("boost_regex-gcc34-mt", language="C++") \
    and not conf.CheckLib("boost_regex", language="C++"):
    print >> sys.stderr, "Could not locate boost_regex library"
    Exit(1)
if not conf.CheckLib("boost_thread", language="C++"):
    print >> sys.stderr, "Could not locate boost_thread library"
    Exit(1)
env = conf.Finish()

# Describe what your package contains here.
env.Help("""
LSST Query Services worker package
""")

#
# Build/install things
#
for d in Split("lib tests doc"):
    if os.path.isdir(d):
        try:
            SConscript(os.path.join(d, "SConscript"), exports='env')
        except Exception, e:
            print >> sys.stderr, "%s: %s" % (os.path.join(d, "SConscript"), e)
