import os
import distutils.sysconfig as ds

XROOTD_DIR=os.getenv("XROOTD_DIR")
MYSQL_DIR=os.getenv("MYSQL_DIR")
MYSQLPROXY_DIR=os.getenv("MYSQLPROXY_DIR")
PROTOBUF_DIR=os.getenv("PROTOBUF_DIR")

PYTHONPATH=os.getenv("PYTHONPATH")

XROOTD_INC = os.path.join(XROOTD_DIR, "include","xrootd")
MYSQL_INC = os.path.join(MYSQL_DIR,"include")

MYSQL_LIB = os.path.join(MYSQL_DIR,"lib","mysql")
XROOTD_LIB = os.path.join(XROOTD_DIR,"lib64")
PROTOBUF_LIB = os.path.join(PROTOBUF_DIR,"lib")

PROTOC=os.path.join(PROTOBUF_DIR,"bin","protoc")
