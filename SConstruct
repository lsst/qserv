# -*- python -*-
#
# Setup our environment
#
# 
import glob, os.path, re, sys
import distutils.sysconfig



def makePythonDist():
    print 'dist', distutils.sysconfig.get_python_inc()
    

if 'install' in COMMAND_LINE_TARGETS:
    makePythonDist()


# Manual Xrd dependencies
xrd_cands = ['/usr/local/']
if os.environ.has_key('XRD_DIR'):
    xrd_cands.insert(0, os.environ['XRD_DIR'])

def findXrdLib(path):
    platforms = ["x86_64_linux_26","i386_linux26"]
    if os.environ.has_key('XRD_PLATFORM'):
        platforms.insert(0, os.environ['XRD_PLATFORM'])
    for p in platforms:
        libpath = os.path.join(path, "lib", p)
        if os.path.exists(libpath):
            return libpath
    return None

def findXrdInc(path):
    paths = map(lambda p: os.path.join(path, p), ["include/xrootd", "src"])
    for p in paths:
        neededFile = os.path.join(p, "XrdPosix/XrdPosix.hh")
        if os.path.exists(neededFile):
            return p
    return None

def setXrd(cands):
    for c in cands:
        (inc, lib) = (findXrdInc(c), findXrdLib(c))
        if inc and lib:
            return (inc, lib)
    print >> sys.stderr, "Could not locate xrootd libraries"
    Exit(1)
(xrd_inc, xrd_lib) = setXrd(xrd_cands)
print >> sys.stderr, "Using xrootd inc/lib: ", xrd_inc, xrd_lib

## Setup the SWIG environment
swigEnv = Environment()

swigEnv.Tool('swig')
swigEnv['SWIGFLAGS'] = ['-python', '-c++', '-Iinclude'],
swigEnv['CPPPATH'] = [distutils.sysconfig.get_python_inc(), 'include'],
swigEnv['SHLIBPREFIX'] = ""
## SWIG 1.3.29 has bugs which cause the build to fail.
## 1.3.36 is known to work.
if os.environ.has_key('SWIG'):
    swigEnv['SWIG'] = os.environ['SWIG']



swigEnv.Append(CPPPATH = [xrd_inc])
swigEnv.Append(LIBPATH = [xrd_lib])
swigEnv.Append(CPPFLAGS = ["-D_LARGEFILE_SOURCE",
                           "-D_LARGEFILE64_SOURCE",
                           "-D_FILE_OFFSET_BITS=64",
                           "-D_REENTRANT"])
pyPath = 'python/lsst/qserv/master'
pyLib = os.path.join(pyPath, 'masterLib.so')
srcPaths = [os.path.join('src', 'xrdfile.cc'),
            os.path.join(pyPath, 'masterLib.i')]
swigEnv.SharedLibrary(pyLib, srcPaths)

# Describe what your package contains here.
swigEnv.Help("""
LSST Query Services master server package
""")

#
# Build/install things
#
for d in Split("lib python examples tests doc"):
    if os.path.isdir(d):
        try:
            SConscript(os.path.join(d, "SConscript"), exports='env')
        except Exception, e:
            print >> sys.stderr, "%s: %s" % (os.path.join(d, "SConscript"), e)


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
