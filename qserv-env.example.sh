export QSERV_VERSION=qserv-dev
export QSERV_BASE=/opt/$QSERV_VERSION

# qserv 
# WARNING : set qserv binary directory before in order to use specific python of /opt/qserv/bin

if [ -z "${QSERV_ENV_SETTED}" ]; then
	export PATH=${QSERV_BASE}/bin:${PATH}
	# in order to load numpy
	export PYTHONPATH=/usr/lib64/python2.6/site-packages/
	export QSERV_ENV_SETTED=1
fi 

alias qserv-start="qserv-admin --start"
alias qserv-stop="qserv-admin --stop"
# TODO : manage MySQL pass correctly
alias qserv-status="qserv-admin --status --dbpass \"${QSERV_MYSQL_PASS}\""

