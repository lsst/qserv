#!/bin/sh



mysql -e "CREATE DATABASE proxyTest; USE proxyTest; CREATE TABLE Obj (objectId INT, x INT); INSERT INTO Obj VALUES (1,11),(2,12),(3,13); CREATE TABLE dummyResults1(f FLOAT); INSERT INTO dummyResults1 VALUES (1.11), (2.22), (3.33), (4.44); CREATE TABLE dummyResults1Lock (i int); CREATE TABLE dummyResults2(f FLOAT); INSERT INTO dummyResults2 VALUES (5.55), (6.66); CREATE TABLE dummyResults2Lock (i int); CREATE TABLE dummyResults3(f FLOAT); INSERT INTO dummyResults3 VALUES (7.77), (8.88), (9.99); CREATE TABLE dummyResults3Lock (i int)"

# these all should succeed
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE x>7"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE qserv_areaSpec_box (1, 2,3,4)"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE qserv_areaSpec_box (1, 2,3,4) AND x>4"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45   AND x>4"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45\n   AND x>4"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE qserv_areaSpec_box (1, 2,3,4)   AND objectId=45"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE qserv_areaSpec_box (1, 2,3,4)   AND objectId=45 AND x>3"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45  AND qserv_areaSpec_box (1, 2,3,4) AND x>3"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45 aND qserv_areaSpec_box (1, 2,3,4)"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45 aND qserv_areaSpec_box (1, 2,3,4) AND x>4"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45 aND qserv_areaSpec_box (1, 2,3,4) AND x>3"



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
