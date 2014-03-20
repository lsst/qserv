DEPS="expat libevent lua luaexpat luasocket luaxmlrpc mysql mysqlproxy
mysqlpython protobuf python qserv twisted virtualenv_python xrootd
zopeinterface"

DEPS_DIR=dependencies

rm -rf ${DEPS_DIR}
mkdir ${DEPS_DIR}
cd ${DEPS_DIR}

for product in ${DEPS}
do
    echo "Cloning ${product}"
    git clone ssh://git@dev.lsstcorp.org/contrib/eupspkg/${product}
done
cd -
