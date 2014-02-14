 cd ~/src/misc/eups/; source env.sh; cd -
setup mysql
setup xrootd
setup protobuf
# TODO each python package must deploy locally its eggs
setup python 

# export MYSQL_ROOT=${MYSQL_DIR}
export PROTOC=${PROTOBUF_DIR}/bin/protoc
export PROTOC_INC=${PROTOBUF_DIR}/include
export PROTOC_LIB=${PROTOBUF_DIR}/lib

# TODO : watch for a pb with ldconfig on Ubuntu ? (seems ok on debian) :
sudo -c "cd /usr/lib; ln -s libboost_regex.so.1.49.0 libboost_regex.so"

# TODO : add next file on Debian/Ubuntu
fjammes@clrinfoport09:~/src/qserv/core$ cat /usr/local/bin/antlr 
java -cp /usr/share/java/antlr-2.7.7.jar antlr.Tool $@

scons CPPPATH="${XROOTD_DIR}/include/xrootd:${MYSQL_DIR}/include"
