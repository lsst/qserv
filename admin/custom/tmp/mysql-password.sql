UPDATE mysql.user SET Password = PASSWORD('%(MYSQLD_PASS)s') WHERE User = 'root';
FLUSH PRIVILEGES;
