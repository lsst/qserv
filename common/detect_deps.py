
import os, subprocess, sys

def detectProtobufs():
    ## Check for protobufs support
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
    
# Xrootd/Scalla search helper
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
XRDFLAGS = ["-D_LARGEFILE_SOURCE", "-D_LARGEFILE64_SOURCE",
            "-D_FILE_OFFSET_BITS=64", "-D_REENTRANT",]

## Boost checker
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
