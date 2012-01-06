
import os, subprocess

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
    
