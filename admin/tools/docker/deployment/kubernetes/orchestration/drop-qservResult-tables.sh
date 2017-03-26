#!/bin/sh

# Drop all result/message tables on Qserv master

# @author  Fabrice Jammes, IN2P3/SLAC

DB=qservResult
USER=root
PASSWORD=changeme

MYSQL_CMD="mysql --socket /qserv/run/var/lib/mysql/mysql.sock --user=$USER --password=$PASSWORD"

kubectl exec master -- bash -c ". /qserv/stack/loadLSST.bash && \
setup mariadbclient && \
TABLES=\$($MYSQL_CMD -N $DB -e 'show tables') && \
for t in \$TABLES; do \
SQL=\"DROP table \$t; \$SQL\"; \
done; \
echo \$SQL; \
$MYSQL_CMD $DB -e \"\$SQL\""
