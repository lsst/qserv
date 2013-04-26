#!/bin/bash

# Usage: reduce <Object> <Source>
# Output: Object-reduced.csv ObjectId.txt Source-reduced.csv

OBJECT=$1
SOURCE=$2

echo Reducing Object
awk -F"," -f reduce-object.awk ${OBJECT} > Object-reduced.csv
echo Done

echo Extracting ObjectID 
awk -F"," -f object2id.awk Object-reduced.csv | sort -n > ObjectId.txt
echo Done

echo Reducing Source
awk -F"," -f reduce-source.awk ${SOURCE} > Source-reduced.csv
echo Done

