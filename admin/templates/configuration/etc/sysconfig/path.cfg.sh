#-------------------------------------------------------------------------------
# Handling paths, using eups or meta-config file
# Author: Fabrice Jammes <fabrice.jammes@in2p3.fr> (09.17.2013)
#-------------------------------------------------------------------------------

# Set up a default search path.
# Using value from QSERV_RUN_DIR/qserv-meta.conf

check_dir_availability() {
    # Test syntax.
    if [ "$#" = 0 ] ; then
	    echo $"Usage: check_dir_availability {varname1} ... {varnameN}"
	    return 1
    fi
    for varname in "$@"
    do
        var=${!varname}
        if [ ! -r "${var}" ]; then
            log_failure_msg "Failure in service $NAME: \$$varname doesn't \
exists or read permission isn't granted"
            exit 1
        elif [ ! -d "${var}" ]; then
            log_failure_msg "Failure in service $NAME: \$$varname isn't a \
directory"
            exit 1
        fi
    done
}

# Used by all services 
export PATH={{PATH}}


if [ $NAME = 'qserv-czar' ]; then
    export PYTHONPATH={{PYTHONPATH}}
    export LD_LIBRARY_PATH={{LD_LIBRARY_PATH}}

elif [ $NAME = 'mysqld' ]; then
    MYSQL_DIR={{MYSQL_DIR}}
    check_dir_availability MYSQL_DIR

elif [ $NAME = 'xrootd' ]; then
    export LD_LIBRARY_PATH={{LD_LIBRARY_PATH}}
    XROOTD_DIR={{XROOTD_DIR}}
    check_dir_availability XROOTD_DIR

elif [ $NAME = 'mysql-proxy' ]; then
    QSERV_DIR={{QSERV_DIR}}
    LUA_DIR={{LUA_DIR}}
    check_dir_availability QSERV_DIR LUA_DIR

fi
