# -*- python -*-
#
# Setup our environment
#
# 
import glob, os.path, re, sys
import distutils.sysconfig

env = Environment()

def makePythonDist():
    print 'dist', distutils.sysconfig.get_python_inc()
    

if 'install' in COMMAND_LINE_TARGETS:
    makePythonDist()

searchRoots = ['/usr/local/'] # search in /usr/local by default.
if os.environ.has_key('SEARCH_ROOTS'):
    searchRoots = os.environ['SEARCH_ROOTS'].split(":")

# Scalla/xrootd is more complex to find.
class XrdHelper:
    def __init__(self, roots):
        self.cands = roots
        if os.environ.has_key('XRD_DIR'):
            self.cands.insert(0, os.environ['XRD_DIR'])

        self.platforms = ["x86_64_linux_26","i386_linux26","i386_linux26_dbg"]
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

def composeEnv(env, roots=[], includes=[], libs=[]):
    env.Append(CPPPATH=[os.path.join(x, "include") for x in roots])
    env.Append(CPPPATH=includes)
    env.Append(LIBPATH=[os.path.join(x, "lib") for x in roots])
    env.Append(LIBPATH=libs)
    return env

# Start checking deps
# -------------------
hasXrootd = True
canBuild = True

# Find Scalla/xrootd directories
x = XrdHelper(searchRoots)
(xrd_inc, xrd_lib) = x.getXrdLibInc()
if (not xrd_inc) or (not xrd_lib):
    print >>sys.stderr, "Can't find xrootd headers or libraries"
    hasXrootd = False
else:
    print >> sys.stderr, "Using xrootd inc/lib: ", xrd_inc, xrd_lib

# Build the environment
env = Environment()
env.Tool('swig')
env['SWIGFLAGS'] = ['-python', '-c++', '-Iinclude'],
env['CPPPATH'] = [distutils.sysconfig.get_python_inc(), 'include'],
env['SHLIBPREFIX'] = ""

## Allow user-specified swig tool
## SWIG 1.3.29 has bugs which cause the build to fail.
## 1.3.36 is known to work.
if os.environ.has_key('SWIG'):
    env['SWIG'] = os.environ['SWIG']

searchLibs = [xrd_lib]
searchLibs += filter(os.path.exists, 
                     map(lambda r: os.path.join(r,"lib","mysql"),searchRoots))

composeEnv(env, roots=searchRoots, includes=[xrd_inc], libs=searchLibs)
if hasXrootd:
    env.Append(CPPPATH = [xrd_inc])
    env.Append(LIBPATH = [xrd_lib])
env.Append(CPPFLAGS = ["-D_LARGEFILE_SOURCE",
                       "-D_LARGEFILE64_SOURCE",
                       "-D_FILE_OFFSET_BITS=64",
                       "-D_REENTRANT",
                       "-g"])

# Start configuration tests
conf = Configure(env)

# XrdPosix library
if not conf.CheckLib("XrdPosix", language="C++"):
    print >>sys.stderr, "Can't use XrdPosix lib"
    hasXrootd = False

# boost library reqs
def checkBoost(lib):
    if not conf.CheckLib(lib + "-gcc34-mt", language="C++") \
            and not conf.CheckLib(lib + "-gcc41-mt", language="C++") \
            and not conf.CheckLib(lib, language="C++") \
            and not conf.CheckLib(lib + "-mt", language="C++"):
        print >>sys.stderr, "Can't find " + lib
        canBuild = False
checkBoost("boost_thread")
checkBoost("boost_regex")

# libssl
if not conf.CheckLib("ssl"):
    print >> sys.stderr, "Could not locate ssl"
    canBuild = False

# ANTLR
if not conf.CheckCXXHeader("antlr/AST.hpp"):
    print >> sys.stderr, "Could not locate ANTLR headers"
    canBuild = False
if not conf.CheckLib("antlr", language="C++"):
    print >> sys.stderr, "Could not find ANTLR lib"
    canBuild = False

# MySQL
if not conf.CheckCXXHeader("mysql/mysql.h"):
    print >> sys.stderr, "Could not locate MySQL headers"
    canBuild = False
if not conf.CheckLib("mysqlclient_r", language="C++"):
    print >> sys.stderr, "Could not find multithreaded mysql(mysqlclient_r)"
    canBuild = False

    
# Close out configuration
env = conf.Finish()    
if hasXrootd:
    env.Append(LIBS = ["XrdPosix"])
canBuild = canBuild and hasXrootd

parserSrcs = map(lambda x: os.path.join('src', x), 
                 ["parser.cc", "AggregateMgr.cc", 
                  "SqlParseRunner.cc",
                  "Templater.cc",
                  "dbgParse.cc",
                  "SqlSQL2Lexer.cpp", "SqlSQL2Parser.cpp"] )
             

pyPath = 'python/lsst/qserv/master'
pyLib = os.path.join(pyPath, '_masterLib.so')
dispatchSrcs = map(lambda x: os.path.join('src', x), 
                   ["xrdfile.cc", 
                    "thread.cc", "TableMerger.cc",
                    "sql.cc",
                    "dispatcher.cc", "xrootd.cc"])

srcPaths = dispatchSrcs + [os.path.join(pyPath, 'masterLib.i')] + parserSrcs


runTrans = { 'bin' : os.path.join('bin', 'runTransactions'),
             'srcPaths' : dispatchSrcs + [os.path.join('src',
                                                       "runTransactions.cc")]
             }
# Lexer and Parser cpp files should have been generated with
# "antlr -glib DmlSQL2.g SqlSQL2.g"
testParser = { 'bin' : os.path.join('bin', 'testCppParser'),
               'srcPaths' : (parserSrcs +
                             [os.path.join("tests","testCppParser.cc")]),
               }
if canBuild:
    env.SharedLibrary(pyLib, srcPaths)
    env.Program(runTrans['bin'], runTrans["srcPaths"])
    env.Program(testParser['bin'], testParser["srcPaths"])

# Describe what your package contains here.
env.Help("""
LSST Query Services master server package
""")

#
# Build/install things
#
for d in Split("lib examples doc"):
    if os.path.isdir(d):
        try:
            SConscript(os.path.join(d, "SConscript"), exports='env')
        except Exception, e:
            print >> sys.stderr, "%s: %s" % (os.path.join(d, "SConscript"), e)

if not canBuild:
    print >>sys.stderr, "****** Fatal errors. Didn't build anything. ******"
    

# env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"




# Alias("install", [env.Install(env['prefix'], "python"),
#                   env.Install(env['prefix'], "include"),
#                   env.Install(env['prefix'], "lib"),
#                   env.InstallAs(os.path.join(env['prefix'], "doc", "doxygen"),
#                                 os.path.join("doc", "htmlDir")),
#                   env.InstallEups(os.path.join(env['prefix'], "ups"))])

# scons.CleanTree(r"*~ core *.so *.os *.o")

# #
# # Build TAGS files
# #
# files = scons.filesToTag()
# if files:
#     env.Command("TAGS", files, "etags -o $TARGET $SOURCES")

# env.Declare()
