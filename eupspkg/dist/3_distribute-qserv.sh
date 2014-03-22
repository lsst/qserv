source ${QSERV_SRC_DIR}/eupspkg/setup-dist.sh

if [ ! -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
then
    eups_install
fi

QSERV_REPO=ssh://git@dev.lsstcorp.org/LSST/DMS/qserv
QSERV_BRANCH=tickets/3100

DATA_REPO=ssh://git@dev.lsstcorp.org/LSST/DMS/testdata/qservdata.git
DATA_BRANCH=master

QSERV_REPO_PATH=${DEPS_DIR}/qserv

rm -f ${LOCAL_PKGROOT}/tables/qserv-${VERSION}.table &&

echo "INFO : Retrieving Qserv archive"
git archive --remote=${QSERV_REPO} --format=tar --prefix=qserv/ ${QSERV_BRANCH} | gzip > ${QSERV_REPO_PATH}/upstream/qserv-${VERSION}.tar.gz ||
{
    echo "ERROR : Unable to download qserv archive" && 
    exit 1
}

cd ${QSERV_REPO_PATH} &&

# update repos
git add upstream/qserv-${VERSION}.tar.gz &&
git commit -m "Packaging qserv-${VERSION}" &&
git tag | head -1 | xargs git tag -f &&
git push origin master -f --tags &&

echo "INFO : Retrieving Qserv tests dataset"
git archive --remote=${DATA_REPO} --format=tar --prefix=testdata/ ${DATA_BRANCH} | gzip > ${LOCAL_PKGROOT}/tarballs/testdata-${VERSION}.tar.gz || 
{
    echo "ERROR : Unable to download tests dataset" && 
    exit 1
}

echo "INFO : Distributing Qserv"
cd - &&
eups distrib install git --repository="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs" &&
setup git &&
eups_dist qserv ${VERSION} ||
{
    echo "ERROR : Unable to distribute Qserv" && 
    exit 1
}
 
echo "INFO : Downloading scisql"
SCISQL_ARCHIVE=scisql-0.3.2.tar.bz2
if [ ! -f ${LOCAL_PKGROOT}/tarballs/${SCISQL_ARCHIVE} ]; then
    SCISQL_URL=https://launchpad.net/scisql/trunk/0.3.2/+download/${SCISQL_ARCHIVE}
    mkdir -p ${LOCAL_PKGROOT}/tarballs &&
    wget ${SCISQL_URL} --directory-prefix=${LOCAL_PKGROOT}/tarballs ||
    echo "WARN : unable to download scisql from ${SCISQL_URL}"
fi
 

upload_to_distserver
