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

echo "INFO : Distributing Qserv" &&
eups distrib install git --repository="${EUPS_PKGROOT_LSST}" &&
setup git &&
cd ${QSERV_SRC_DIR} &&

echo "INFO : Creating eups TaP with current Qserv commit"
HASH_COMMIT=$(git rev-parse --verify HEAD)

QSERV_TAP_REPO_PATH=${DEPS_DIR}/qserv
rm -f ${QSERV_TAP_REPO_PATH}/upstream/*
git archive --format=tar --prefix=qserv/ HEAD | gzip > ${QSERV_TAP_REPO_PATH}/upstream/qserv-${VERSIONTAG}.tar.gz ||
{
    echo "ERROR : Unable to build qserv archive" && 
    exit 1
}

rm -rf ${QSERV_TAP_REPO_PATH}/ups/*
git archive --format=tar HEAD ups | tar -x -C ${QSERV_TAP_REPO_PATH} ||
{
    echo "ERROR : Unable to build qserv archive" && 
    exit 1
}

cd ${QSERV_TAP_REPO_PATH} &&

# update repos
git add upstream/qserv-${VERSIONTAG}.tar.gz &&
git add ups/* &&
git commit -m "Packaging qserv-${VERSIONTAG} with ${HASH_COMMIT}" &&
git tag -f ${VERSIONTAG} &&
git push origin -f ${VERSIONTAG} &&

rm -f ${LOCAL_PKGROOT}/tables/qserv-${VERSIONTAG}.table &&
eups_dist qserv ${VERSIONTAG} ||
{
    echo "ERROR : Unable to distribute Qserv" && 
    exit 1
}
eups distrib declare --server-dir=${LOCAL_PKGROOT} -t current
 
# TODO : package in eups ?
mkdir -p ${LOCAL_PKGROOT}/tarballs &&
TESTDATA_ARCHIVE=testdata-${VERSIONTAG}.tar.gz &&
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

sed "s/%VERSIONTAG%/${VERSIONTAG}/g" ${QSERV_SRC_DIR}/eupspkg/newinstall-qserv-template.sh | sed "s/%DISTSERVERNAME%/${DISTSERVERNAME}/g" > ${LOCAL_PKGROOT}/newinstall-qserv-${VERSIONTAG}.sh 
#ln -s ${LOCAL_PKGROOT}/newinstall-qserv-$VERSIONTAG.sh ${LOCAL_PKGROOT}/newinstall-qserv.sh

upload_to_distserver
