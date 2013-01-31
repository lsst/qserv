UPDATE mysql.user SET Password = PASSWORD('%(MYSQLD_PASS)') WHERE User = 'root';
FLUSH PRIVILEGES;
