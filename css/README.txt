#### Dependencies #####

CSS depends on:
a) Zookeeper and Kazoo, for details how to install them, see:
   https://dev.lsstcorp.org/trac/wiki/db/Qserv/ZookeeperNotes
b) db package. Hacky way to get it for now:
    1) git clone git@git.lsstcorp.org:LSST/DMS/db.git db
    2) cd db
    3) git checkout u/jbecla/ticket3133

The eups-based install procedure sets up these dependencies automatically

##### Building  and installing core/modules/css #####

Follow the Qserv install procedure, described in top-level README.txt

##### Starting zookeeper #####

$QSERV_DIR/etc/init.d/zookeeper start

##### Testing C bindings #####

export LD_LIBRARY_PATH=<path>/lib:<path>/lib/mysql
build/css/testCssException
build/css/testKvInterfaceImpl
build/css/dist/testFacade


##### Testing python bindings and qserv-admin_client #####


### set PYTHONPATH
export PYTHONPATH=<basePath>/localPython/lib/python<version>/site-packages:<basePath>/db/python/:<basePath>/qserv/css:<basePath>/qserv/client


### set verbosity of kazoo
# You might want to change kazoo logging level by setting:
export KAZOO_LOGGING=50
# 50-critical, 40-error, 30-warning, 20-info, 10-debug


### clean up everything
echo "drop everything;" | ./admin/bin/qserv-admin.py


### create ~/.my.cnf with connection and credential parameters, eg
[client]
user     = <your mysql user name>
password = <the password>
   # host/port and/or socket
host     = localhost
port     = 3306
socket   = /var/run/mysqld/mysqld.sock


### in one window, start the watcher
  # this is without logging
  ./css/watcher.py
  # this is with logging going to a file
  ./css/watcher.py  -v 10 -f watcher.log


### in second window, run the test:
  # this is without logging:
  ./admin/bin/qserv-admin.py  < ./client/tests/test_qserv-admin
  # this is with logging:
  ./admin/bin/qserv-admin.py -v 10 -f qadm.log < ./admin/tests/test_qserv-admin


  ./admin/tests/test_qserv-admin_impl.py
