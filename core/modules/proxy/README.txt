
# Start mysql proxy and rpcServer using provided scripts

# Talk to mysql proxy from mysql client using
mysql --port=4040 --protocol=TCP


# packages needed for lua (on ubuntu)
# liblua5.1-socket2 liblua5.1-xmlrpc0 lua

# Note: luaxmlrpc 1.0b (dec 2, 2004) has a bug.  
# At minimum, replace "module (arg and arg[1])" with module (...)"
# lua-xmlrpc author suggests using CVS head version.
# We recommend unpacking Ubuntu's liblua5.1-xmlrpc0_1.0b-4_all.deb .
# Put the package's /usr/share/lua/5.1/xmlrpc in $PREFIX/share/lua/5.1/xmlrpc


