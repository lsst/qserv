[mysqld]

datadir=<DATA_DIR>
socket=<INSTALL_DIR>/var/lib/mysql/mysql.sock
# port=3306
port=<MYSQL_PORT>

# Disabling symbolic-links is recommended to prevent assorted security risks
symbolic-links=0

#
# * Logging and Replication
#
# Both location gets rotated by the cronjob.
# Be aware that this log type is a performance killer.
# general-log=<LOG_DIR>/mysql-queries.log

[mysqld_safe]

log-error=<LOG_DIR>/mysqld.log
pid-file=<INSTALL_DIR>/var/run/mysqld/mysqld.pid

