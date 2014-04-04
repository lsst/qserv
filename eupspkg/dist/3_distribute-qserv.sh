BASEDIR=$(dirname $0)
source ${BASEDIR}/setup-dist.sh
if [ -n "${SETUP_DIST}" ]
then
    
    exit 1
fi
 

if [ ! -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
then
    eups_install
fi

cd ${QSERV_SRC_DIR} &&

git tag -f ${VERSION} &&
git push origin -f ${VERSION} &&


echo "INFO : Distributing Qserv" &&
eups distrib install git --repository="${EUPS_PKGROOT_LSST}" &&
setup git &&
export EUPSPKG_REPOSITORY_PATH=${QSERV_REPO} &&
eups_dist_create qserv ${VERSION} ||
{
    echo "ERROR : Unable to distribute Qserv" && 
    exit 1
}
 
cd - &&

# TODO : package in eups ?
mkdir -p ${LOCAL_PKGROOT}/tarballs &&
TESTDATA_ARCHIVE=testdata-${VERSION}.tar.gz &&
if [ ! -f ${LOCAL_PKGROOT}/tarballs/${TESTDATA_ARCHIVE} ]; then
    echo "INFO : Retrieving Qserv tests dataset"
    git archive --remote=${DATA_REPO} --format=tar --prefix=testdata/ ${DATA_BRANCH} | gzip > ${LOCAL_PKGROOT}/tarballs/${TESTDATA_ARCHIVE} || 
    {
        echo "ERROR : Unable to download tests dataset" && 
        exit 1
    }
fi

# TODO : package in eups ?
SCISQL_ARCHIVE=scisql-0.3.2.tar.bz2
if [ ! -f ${LOCAL_PKGROOT}/tarballs/${SCISQL_ARCHIVE} ]; then
echo "INFO : Downloading scisql"
    SCISQL_URL=https://launchpad.net/scisql/trunk/0.3.2/+download/${SCISQL_ARCHIVE}
    wget ${SCISQL_URL} --directory-prefix=${LOCAL_PKGROOT}/tarballs ||
    echo "WARN : unable to download scisql from ${SCISQL_URL}"
fi
 
cp ${QSERV_SRC_DIR}/eupspkg/newinstall-qserv.sh ${LOCAL_PKGROOT}/newinstall-qserv-$VERSION.sh
#ln -s ${LOCAL_PKGROOT}/newinstall-qserv-$VERSION.sh ${LOCAL_PKGROOT}/newinstall-qserv.sh

upload_to_distserver
