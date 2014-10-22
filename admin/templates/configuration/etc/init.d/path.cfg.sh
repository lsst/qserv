#-------------------------------------------------------------------------------
# Handling paths, using eups or meta-config file
# Author: Fabrice Jammes <fabrice.jammes@in2p3.fr> (09.17.2013)
#-------------------------------------------------------------------------------

# Set up a default search path.
if [ -n ${EUPS_DIR} ]; then
    QSERV_ENV="(eups environment)"
else
    QSERV_ENV="(meta-config environment)"

    # all
    export PATH={{PATH}}

    # qserv-czar
    export PYTHONPATH={{PYTHONPATH}}

    # qserv-czar, xrootd
    export LD_LIBRARY_PATH={{LD_LIBRARY_PATH}}

    # mysql
    MYSQL_DIR={{MYSQL_DIR}}

    # mysql-proxy
    QSERV_DIR={{QSERV_DIR}}
    LUA_DIR={{LUA_DIR}}

    # xrootd
    XROOTD_DIR={{XROOTD_DIR}}
fi
