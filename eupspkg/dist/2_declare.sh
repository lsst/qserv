BASEDIR=$(dirname $0)
source ${BASEDIR}/setup-dist.sh
if [ -n "${SETUP_DIST}" ]
then
    exit 1
fi


eups_install

# If you don't have python >= 2.7 with numpy >= 1.5.1 and
# matplotlib >=1.2.0, use Anaconda python distribution by installing
# it manually, or use the LSST-packaged one.
# eups distrib install anaconda
# setup anaconda

eups distrib install git --repository="http://sw.lsstcorp.org/eupspkg" &&
setup git


eups_remove_all
rm -rf LOCAL_PKGROOT/*
eups declare python system -r none -m none
eups declare numpy system -r none -m none
eups_dist mysql 5.1.65 && 
eups_dist xrootd qs5 &&
eups_dist lua 5.1.4 &&
eups_dist luasocket 2.0.2 &&
eups_dist expat 2.0.1 &&
eups_dist luaexpat 1.1 &&
eups_dist luaxmlrpc v1.2.1-2 &&
eups_dist libevent 2.0.16-stable &&
eups_dist mysqlproxy 0.8.2 &&
eups_dist virtualenv_python 1.10.1 && 
eups_dist mysqlpython 1.2.3 &&
eups_dist protobuf 2.4.1 && 
eups_dist zopeinterface 3.8.0 && 
eups_dist twisted 12.0.0 && 
eups_dist qserv 6.0.0rc1 ||
exit -1

echo "Declaring Qserv packages"
# will allow to use eups distrib install pkg, without version
eups distrib declare --server-dir=${LOCAL_PKGROOT} -t current

# Now check that we can install from the new distserver
echo
echo "REMOVING PACKAGES : $PWD"
echo
# ${INSTALL_DIR}/tmp/${product} will be removed during post processing
cd ${INSTALL_DIR}
# eups_remove_all
