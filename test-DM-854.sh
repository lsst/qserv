set -e
set -x

~/qserv-run/2015_03/bin/qserv-stop.sh
qserv-configure.py --all
~/qserv-run/2015_03/bin/qserv-start.sh
qserv-check-integration.py --case=01 --load
mysql --host=127.0.0.1 --port=4040 --user=qsmaster qservTest_case01_qserv  -e "SELECT DISTINCT o1.objectId, o2.objectId  FROM   Object o1, Object o2  WHERE scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 0.001 AND  o1.objectId <> o2.objectId;"
# FIX
mysql --host=127.0.0.1 --port=4040 --user=qsmaster qservTest_case01_qserv  -e
"SELECT DISTINCT o1.objectId as obj1, o2.objectId as obj2 FROM Object o1,
Object o2 WHERE scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) <
0.01 AND  o1.objectId <> o2.objectId;"


mysql --host=127.0.0.1 --port=4040 --user=qsmaster qservTest_case01_qserv -e "SELECT objectId, objectId  FROM   Object LIMIT 1;"
