DEFAULT_CONFIG="""
# WARNING : these variables mustn't be changed once the install process is started
# This file relies on Python templating system i.e. each %(variable_name)s will be replaced by the value of variable_name
[DEFAULT]
version = 0.5.1rc3
# Qserv will be installed in base_dir
base_dir = /opt/qserv-%(version)s
# Log file will be stored here
log_dir = %(base_dir)s/var/log
# Temporary files will be stored here
tmp_dir = %(base_dir)s/tmp

[qserv]

# Tree possibles values :
# mono
# master
# worker
node_type=mono

# Port used by Qserv RPC service
# mysql-proxy will connect to Qserv using socket localhost:rpc_port
rpc_port=7080

user = qsmaster

# Qserv master DNS name
master=qserv-master-example-name.mydomain.edu

# OPTIONAL : If not already present in %(base_dir)s/qserv/master/python/lsst/qserv/master/geometry.py, geometry file will be downloaded by default
# from  git master branch :http://dev.lsstcorp.org/cgit/LSST/DMS/geom.git/plain/python/lsst/geom/geometry.py
# but a source directory may be specified  (it could be retrieved for exemple with : git clone git://dev.lsstcorp.org/LSST/DMS/geom)
# geometry will be then copied from this source directory
# geometry_src_dir=/home/user/geom

[qms]
db = QMS
user = qmsuser
pass = changeme
port = 7082

[xrootd]
cmsd_manager_port=2131
xrootd_port=1094

[mysql_proxy]

port=4040

[mysqld]

port=3306

user=root
# Be careful, special characters (',%,\",...) may cause error,
# use %% instead of %
pass=changeme

# socket for local connection
sock=%(base_dir)s/var/lib/mysql/mysql.sock 

#data_dir=/data/$(version)/mysql
data_dir=%(base_dir)s/var/lib/mysql

[lsst]

# OPTIONAL : Where to read LSST data
# Example: PT1.1 data should be in $(data_dir)/pt11/
data_dir=/data/lsst

# Do not edit, unless you know what you are doing
[dependencies]

repository=http://www.slac.stanford.edu/exp/lsst/qserv/download/current

zope_url=%(repository)s/zope.interface-3.8.0.tar.gz
mysql_python_url=%(repository)s/MySQL-python-1.2.3.tar.gz
twisted_url=%(repository)s/Twisted-12.0.0.tar.bz2
expat_url=%(repository)s/expat-2.0.1.tar.gz
libevent_url=%(repository)s/libevent-2.0.16-stable.tar.gz
lua_url=%(repository)s/lua-5.1.4.tar.gz
lua_xlmrpc_url=%(repository)s/lua-xmlrpc-v1.2.1-2.tar.gz
luaexpat_url=%(repository)s/luaexpat-1.1.tar.gz
luasocket_url=%(repository)s/luasocket-2.0.2.tar.gz
mysql_url=%(repository)s/mysql-5.1.61.tar.gz
mysql_proxy_url=%(repository)s/mysql-proxy-0.8.2.tar.gz
protobuf_url=%(repository)s/protobuf-2.4.1.tar.gz
scisql_url=%(repository)s/scisql-0.3.2.tar.bz2
virtualenv_url=%(repository)s/virtualenv-1.7.tar.gz
xrootd_url=%(repository)s/xrootd-qs4.tar.gz
"""


