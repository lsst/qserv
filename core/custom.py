import os
import distutils.sysconfig as ds

XROOTD_DIR=os.getenv("XROOTD_DIR")
MYSQL_DIR=os.getenv("MYSQL_DIR")
PROTOBUF_DIR=os.getenv("PROTOBUF_DIR")
PYTHONPATH=os.getenv("PYTHON_PATH")

INC=[]
INC.append(os.path.join(XROOTD_DIR, "include","xrootd"))
INC.append(os.path.join(MYSQL_DIR,"include"))

LIB=[]
LIB.append(os.path.join(MYSQL_DIR,"lib","mysql"))
LIB.append(os.path.join(XROOTD_DIR,"lib64"))
LIB.append(os.path.join(PROTOBUF_DIR,"lib"))

PROTOC=os.path.join(PROTOBUF_DIR,"bin","protoc")
MYSQL_ROOT=MYSQL_DIR
