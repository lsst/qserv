#!/bin/sh

set -e
set -x

#############################
# CUSTOMIZE NEXT PARAMETERS :
#############################

INTERNET_FREE_DISTSERVER_DIR=shared/dir/available/to/all/nodes
INSTALL_DIR=root/directory/where/qserv/stack/will/be/installed

#############################


# CAUTION: remove previous configuration data
rm -rf ~/qserv-run
# CAUTION: previous install will be removed
# The script will ask you two questions, if you use : 
#    - answer "yes" to the first one (i.e. install git with eups)
#    - and "no" to the second (i.e. don't install anaconda with eups)
bash ${INTERNET_FREE_DISTSERVER_DIR}/qserv-install.sh -r ${INTERNET_FREE_DISTSERVER_DIR} -i ${INSTALL_DIR}

