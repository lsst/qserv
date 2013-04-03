#!/bin/sh

echo === destroyMeta ===
./meta/examples/runMetaClientTool.sh destroyMeta

echo === installMeta === 
./meta/examples/runMetaClientTool.sh installMeta

echo === printMeta ===
./meta/examples/runMetaClientTool.sh printMeta

echo === list dbs===
./meta/examples/runMetaClientTool.sh listDbs

echo === create 4 dbs ===
./meta/examples/runMetaClientTool.sh createDb Summer2012 @./meta/examples/dbPartitioned.params
./meta/examples/runMetaClientTool.sh createDb NonPartA @./meta/examples/dbNonPartitioned.params
./meta/examples/runMetaClientTool.sh createDb NonPartB @./meta/examples/dbNonPartitioned.params
./meta/examples/runMetaClientTool.sh createDb Winter2013 defaultOverlap_nearNeighbor=0.25 partitioning=on defaultOverlap_fuzziness=0.0001 partitioningStrategy=sphBox nStripes=10 nSubStripes=23

echo === list dbs===
./meta/examples/runMetaClientTool.sh listDbs

echo === checking if dbs exists. y, n ===
./meta/examples/runMetaClientTool.sh checkDbExists Summer2012
./meta/examples/runMetaClientTool.sh checkDbExists Summer2012xx

echo === 'retrieve dbInfo for 2 dbs (ok, fail)' ===
./meta/examples/runMetaClientTool.sh retrieveDbInfo Summer2012
./meta/examples/runMetaClientTool.sh retrieveDbInfo NonPart

echo === create 5 tables ===
./meta/examples/runMetaClientTool.sh createTable Summer2012 tableName=Object partitioning=on schemaFile=./meta/examples/tbSchema_Object.sql clusteredIndex=IDX_objectId overlap=0.025 phiColName=ra_PS thetaColName=decl_PS logicalPart=2 physChunking=0x0021
./meta/examples/runMetaClientTool.sh createTable Summer2012 partitioning=off tableName=Exposure schemaFile=./meta/examples/tbSchema_Exposure.sql
./meta/examples/runMetaClientTool.sh createTable Summer2012 @./meta/examples/tb_Source.params
./meta/examples/runMetaClientTool.sh createTable Summer2012 @./meta/examples/tb_Exposure.params
./meta/examples/runMetaClientTool.sh createTable Winter2013 @./meta/examples/tb_Source.params

echo === retrieve table info for 2 tables ===
./meta/examples/runMetaClientTool.sh retrieveTableInfo Winter2013 Object
./meta/examples/runMetaClientTool.sh retrieveTableInfo Winter2013 Exposure

echo === print meta ===
./meta/examples/runMetaClientTool.sh printMeta

echo === drop 3 tables. Fail, ok, ok ===
./meta/examples/runMetaClientTool.sh dropTable weirdDb Object
./meta/examples/runMetaClientTool.sh dropTable Summer2012 Object
./meta/examples/runMetaClientTool.sh dropTable Summer2012 Exposure

echo === drop 3 dbs. Ok, ok, fail ===
./meta/examples/runMetaClientTool.sh dropDb Summer2012
./meta/examples/runMetaClientTool.sh dropDb NonPartA
./meta/examples/runMetaClientTool.sh dropDb whatever

echo === checking if db exist ===
./meta/examples/runMetaClientTool.sh checkDbExists Summer2012

echo === list dbs ===
./meta/examples/runMetaClientTool.sh listDbs


#./meta/examples/runMetaClientTool.sh destroyMeta
