#!/bin/sh

set -e
set -x

#############################
# CUSTOMIZE NEXT PARAMETERS :
#############################

INSTALL_DIR=root/directory/where/qserv/stack/will/be/installed

# If you are using internet-free mode, please specify the location of the local distribution
# server, if no comment two lines below. 
INTERNET_FREE_DISTSERVER_PARENT_DIR=shared/dir/available/to/all/nodes
INTERNET_FREE_DISTSERVER_DIR=${INTERNET_FREE_DISTSERVER_PARENT_DIR}/distserver

# If you want to remove previous configuration data, please set QSERV_RELEASE,
# if no comment line below
QSERV_RUN_DIR=${HOME}/qserv-run/${QSERV_RELEASE}

#############################

# CAUTION: remove previous configuration data
if [[ ! -z  ${QSERV_RUN_DIR} ]]; then
    rm -rf ${QSERV_RUN_DIR}
fi

if [[ ! -z  ${INTERNET_FREE_DISTSERVER_DIR} ]]; then
    INTERNET_FREE_OPT="-r ${INTERNET_FREE_DISTSERVER_DIR}"
fi
QSERV_RUN_DIR_OPT="-R ${QSERV_RUN_DIR}"

# automated install script is available ether in internet-free distserver, or in Qserv sources :
if [[ ! -z  ${INTERNET_FREE_DISTSERVER_DIR} ]]; then
    INTERNET_FREE_OPT="-r ${INTERNET_FREE_DISTSERVER_DIR}"
    QSERV_INSTALL_SCRIPT=${INTERNET_FREE_DISTSERVER_DIR}/qserv-install.sh
else
    QSERV_INSTALL_SCRIPT=${QSERV_SRC_DIR}/admin/tools/qserv-install.sh
fi
bash ${QSERV_INSTALL_SCRIPT} ${INTERNET_FREE_OPT} ${QSERV_RUN_DIR_OPT} -i ${INSTALL_DIR}
