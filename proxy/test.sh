#!/bin/sh



mysql -e "CREATE DATABASE proxyTest; CREATE TABLE proxyTest.Obj (objectId INT, x INT)"


# these all should succeed
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE x>7"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE areaSpec_box (1, 2,3,4)"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE areaSpec_box (1, 2,3,4) AND x>4"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45   AND x>4"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE areaSpec_box (1, 2,3,4)   AND objectId=45"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE areaSpec_box (1, 2,3,4)   AND objectId=45 AND x>3"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45  AND areaSpec_box (1, 2,3,4) AND x>3"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45 aND areaSpec_box (1, 2,3,4)"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45 aND areaSpec_box (1, 2,3,4) AND x>4"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45 aND areaSpec_box (1, 2,3,4) AND x>3"


mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId IN 44"

mysql --port=4040 --protocol=TCP -e "show databases"
mysql --port=4040 --protocol=TCP proxyTest -e "show tables"
mysql --port=4040 --protocol=TCP proxyTest -e "describe Obj"
mysql --port=4040 --protocol=TCP proxyTest -e "desc Obj"

# these all should fail
mysql --port=4040 --protocol=TCP proxyTest -e "insert into Obj values(1, 2)"
mysql --port=4040 --protocol=TCP -e "create database xyz"
mysql --port=4040 --protocol=TCP proxyTest -e "create table tt (i int)"
mysql --port=4040 --protocol=TCP proxyTest -e "explain select * from Obj"


mysql -e "DROP DATABASE proxyTest"
