source setup-eups.sh

# If you don't have python >= 2.7 with numpy >= 1.5.1 and
# matplotlib >=1.2.0, use Anaconda python distribution by installing
# it manually, or use the LSST-packaged one.
# eups distrib install anaconda
# setup anaconda

setup git


eups_remove_all
rm -rf EUPS_PKGROOT/*
eups declare python system -r none -m none
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

echo
echo "DECLARING PACKAGES : $PWD"
echo
# will allow to use eups distrib install pkg, without version
eups distrib declare --server-dir=${EUPS_PKGROOT} -t current

# Now check that we can install from the new distserver
echo
echo "REMOVING PACKAGES : $PWD"
echo
# ${INSTALL_DIR}/tmp/${product} will be removed during post processing
cd ${INSTALL_DIR}
# eups_remove_all
echo
echo "INSTALLING PACKAGES : $PWD"
echo
# edit ~/.eups/manifest.remap (try with $EUPS_DIR/site/manifest.remap)
eups declare python system -r none -m none

#eups distrib install virtualenv_python
#setup virtualenv_python

#time eups distrib install qserv 
#eups distrib install mysqlpython

# NOTE : this pkgroot isn't usable as it force to install python2.7 and
# mysqlclient :
#export EUPS_PKGROOT="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs"
