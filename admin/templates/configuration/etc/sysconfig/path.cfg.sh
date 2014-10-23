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

check_existence() {
    # Test syntax.
    if [ "$#" = 0 ] ; then
	    echo $"Usage: check_existence {varname1} ... {varnameN}"
	    return 1
    fi
    for varname in "$@"
    do
        var=${!varname}
        if [ -z "${var}" ]; then
            log_failure_msg "Failure in service $NAME: undefined \$$varname"
            exit 1
        fi
    done
}

if [ $NAME = 'mysqld' ]; then
    check_existence MYSQL_DIR
elif [ $NAME = 'xrootd' ]; then
    check_existence XROOTD_DIR
elif [ $NAME = 'mysql-proxy' ]; then
    check_existence QSERV_DIR LUA_DIR
fi
