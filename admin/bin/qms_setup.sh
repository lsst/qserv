#!/bin/bash

BASE_DIR=/opt/qserv-dev
DB_NAME=LSST
TEST_ID=01

ERR_NO_META=25

DATA_DIR=${BASE_DIR}/qserv/tests/testdata/case${TEST_ID}/data
META_CMD="${BASE_DIR}/bin/python ${BASE_DIR}/qserv/meta/bin/metaClientTool.py --auth=${HOME}/.lsst/qmsadm"

die() { echo "$@" 1>&2 ; exit 1; }

ACTION="Checking if meta db ${DB_NAME} exists :";
echo ${ACTION}
CMD="${META_CMD} checkDbExists ${DB_NAME}"
echo "     ${CMD}"
DB_EXISTS=$(${CMD})
RET=$?

# installing meta if needed
if [ ${RET} -eq ${ERR_NO_META} ]
then
    DB_EXISTS="no"
    ACTION="Installing meta db";
    echo "No meta db : "${ACTION};
    ${META_CMD} installMeta || die "error while ${ACTION}"
    RET=$?
elif [ ${RET} -ne 0 ]
then
    die "error (errno:${RET}) while ${ACTION}"
fi

echo "Already existing database ${DB_NAME} : ${DB_EXISTS}"

if [ "${DB_EXISTS}" == "yes" ]
then
    ACTION="Dropping previous meta db ${DB_NAME}";
    echo ${ACTION};
    ${META_CMD} dropDb ${DB_NAME} || die "error while ${ACTION}"
fi

echo
ACTION="Creating meta db ${DB_NAME}";
echo ${ACTION};
CMD="${META_CMD} createDb ${DB_NAME} @${DATA_DIR}/db.params"
echo "     ${CMD}"
${CMD} || die "error while ${ACTION}"

# needed to access schema file, specified with relative path in .params files
cd ${DATA_DIR}
echo
ACTION="Creating meta table ${DB_NAME}";
echo ${ACTION};
CMD="${META_CMD} createTable ${DB_NAME} @${DATA_DIR}/Object.params"
echo "     ${CMD}"
${CMD} || die "error while ${ACTION}"
