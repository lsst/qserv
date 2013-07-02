#!/bin/sh

## Careful with "destroyMeta". It destroys all metadata for all databases
./meta/examples/runMetaClientTool.sh destroyMeta
./meta/examples/runMetaClientTool.sh installMeta

## You might want to consider using the shorter way of passing arguments
## through a config file, see the meta/tests/completeTest.sh for examples,
## look for things like @./meta/examples/tb_Source.params

## The values for overlaps, stripes, substripes etc are for illustrative
## purposes only. Definitely synchronize it with how data was actually
## partitioned and loaded!
./meta/examples/runMetaClientTool.sh createDb LSST partitioning=on partitioningStrategy=sphBox  defaultOverlap_fuzziness=0 defaultOverlap_nearNeighbor=0.025 nStripes=85 nSubStripes=12

./meta/examples/runMetaClientTool.sh createTable LSST tableName=AvgForcedPhot partitioning=on schemaFile=../testdata/case03/data/AvgForcedPhot.sql overlap=0.025 phiColName=ra thetaColName=decl objIdColName=deepSourceId logicalPart=1 physChunking=0x0021

./meta/examples/runMetaClientTool.sh createTable LSST tableName=AvgForcedPhotYearly partitioning=on schemaFile=../testdata/case03/data/AvgForcedPhotYearly.sql overlap=0.025 phiColName=ra thetaColName=decl objIdColName=deepSourceId logicalPart=1 physChunking=0x0021

./meta/examples/runMetaClientTool.sh createTable LSST partitioning=on schemaFile=../testdata/case03/data/RunDeepForcedSource.sql overlap=0.025 phiColName=coord_ra thetaColName=coord_decl objIdColName=objectId logicalPart=1 physChunking=0x0021

## notice that the table that is used by the view must be defined before the view is defined
## this is exactly how you'd do it in mysql: you can't define a view if you don't have the
## underlying table.
./meta/examples/runMetaClientTool.sh createTable LSST partitioning=on schemaFile=../testdata/case03/data/DeepForcedSource.sql overlap=0.025 phiColName=ra thetaColName=decl objIdColName=deepSourceId logicalPart=1 physChunking=0x0021 isView=1

./meta/examples/runMetaClientTool.sh createTable LSST tableName=RunDeepSource partitioning=on schemaFile=../testdata/case03/data/RunDeepSource.sql overlap=0.025 phiColName=coord_ra thetaColName=coord_decl objIdColName=id logicalPart=1 physChunking=0x0021

./meta/examples/runMetaClientTool.sh createTable LSST partitioning=on schemaFile=../testdata/case03/data/DeepSource.sql overlap=0.025 phiColName=ra thetaColName=decl objIdColName=deepSourceId logicalPart=1 physChunking=0x0021 isView=1

./meta/examples/runMetaClientTool.sh createTable LSST partitioning=off schemaFile=../testdata/case03/data/Science_Ccd_Exposure.sql
