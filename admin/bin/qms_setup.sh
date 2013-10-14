#!/bin/bash

if [ "$#" != 3 ] ; then
    SCRIPT=`basename $0`
    echo $"Usage: ${SCRIPT} {QSERV_BASE_DIR} {DB_NAME} {TEST_ID}"
    exit 1
fi

BASE_DIR=$1
DB_NAME=$2
TEST_ID=$3

ERR_NO_META=25

DATA_DIR=${BASE_DIR}/qserv/tests/testdata/${TEST_ID}/data

if [ ! -d ${DATA_DIR} ]; then
    echo "Directory containing test dataset (${DATA_DIR}) doesn't exists."
    exit 1
fi

META_CMD="${BASE_DIR}/bin/python ${BASE_DIR}/qserv/meta/bin/metaClientTool.py --auth=${HOME}/.lsst/qmsadm"

die() { echo "$@" 1>&2 ; exit 1; }

run_cmd() {
    if [ "$#" != 2 ] ; then
		echo $"Usage:  {CMD} {MSG}"
		return 1
    fi
    CMD=$1
    ACTION=$2
    echo
    echo "${ACTION}"
    echo "     ${CMD}"
    ${CMD} || die "error (errno:$?) in step : ${ACTION}"

}

ACTION="Checking if meta db ${DB_NAME} exists :";
echo ${ACTION}
CMD="${META_CMD} checkDbExists ${DB_NAME}"
echo "     ${CMD}"
DB_EXISTS=$(${CMD})
RET=$?

echo "Already existing database ${DB_NAME} : ${DB_EXISTS}"

# installing meta if needed
if [ ${RET} -eq ${ERR_NO_META} ]
then
    run_cmd "${META_CMD} installMeta" "Meta not currently installed : installing it"
fi

# creating db if needed
if [ "${DB_EXISTS}" == "yes" ]
then
    run_cmd "${META_CMD} dropDb ${DB_NAME}" "Dropping previous meta db ${DB_NAME}"
fi

run_cmd "${META_CMD} createDb ${DB_NAME} @${DATA_DIR}/db.params" "Creating meta db ${DB_NAME}"

# needed to access schema files, specified with relative path in .params files
cd ${DATA_DIR} && META_FILE_LST=`ls *.params| egrep -v "^db.params$"` || die "Error while looking for tables meta-files in ${DATA_DIR}"

for META_TABLE in $META_FILE_LST
do
    run_cmd "${META_CMD} createTable ${DB_NAME} @${DATA_DIR}/${META_TABLE}" "Creating meta table ${META_TABLE}"
done
