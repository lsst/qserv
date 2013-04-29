#!/bin/bash

# Usage: reduce <Object> <Source> <RefObjMatch> <RefSrcMatch>
# Output: Object-reduced.csv ObjectId.txt Source-reduced.csv SourceId.txt RefObjMatch-reduced.csv RefSrcMatch-reduced.csv 
  

OBJECT=$1
SOURCE=$2
REFOBJMATCH=$3
REFSRCMATCH=$4

echo Reducing Object
awk -F"," -f reduce-object.awk ${OBJECT} > Object-reduced.csv
sed -i '/^$/d' Object-reduced.csv  
echo Done

echo Extracting ObjectID 
awk -F"," -f object2id.awk Object-reduced.csv | sort -n > ObjectId.txt
sed -i '/^$/d' ObjectId.txt 
echo Done

echo Reducing Source
awk -F"," -f reduce-source.awk ${SOURCE} > Source-reduced.csv
sed -i '/^$/d' Source-reduced.csv 
echo Done

echo Extracting SourceID 
awk -F"," -f source2id.awk Source-reduced.csv | sort -n > SourceId.txt
sed -i '/^$/d' SourceId.txt 
echo Done

echo Reducing RefObjMatch
awk -F"," -f reduce-refobjmatch.awk ${REFOBJMATCH} > RefObjMatch-reduced.csv
sed -i '/^$/d' RefObjMatch-reduced.csv 
echo Done

echo Reducing RefSrcMatch
awk -F"," -f reduce-refsrcmatch.awk ${REFSRCMATCH} > RefSrcMatch-reduced.csv
sed -i '/^$/d' RefSrcMatch-reduced.csv 
echo Done


# NonVarObject is empty in PT12.
# 
# echo Reducing NonVarObject 
# awk -F"," -f reduce-nonvarobject.awk ${NONVAROBJ} > NonVarObject-reduced.csv
# sed -i '/^$/d' NonVarObject-reduced.csv
# echo Done

