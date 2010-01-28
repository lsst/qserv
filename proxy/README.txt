
# To start mysql proxy
./bin/mysql-proxy --proxy-lua-script=mysqlProxy.lua

# To talk to mysql proxy from mysql client
mysql --port=4040 --protocol=TCP

