#!/bin/sh


# these all should succeed

mysql -e "CREATE DATABASE proxyTest; CREATE TABLE proxyTest.Obj (objectId INT, x INT)"
mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE x>7"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE areaSpec_box (1, 2,3,4)"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE areaSpec_box( 1,2 , 3,4 ) AND x>6"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId=45 \n  AND x>4"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE areaSpec_box(1,2,3,4) AND objectId=45 AND x>3"

mysql --port=4040 --protocol=TCP proxyTest -e "select * from Obj WHERE objectId IN 44"

mysql -e "DROP DATABASE proxyTest"
