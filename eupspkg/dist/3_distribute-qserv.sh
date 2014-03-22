BASEDIR=$(dirname $0)
cd ${BASEDIR}/..
source ${BASEDIR}/setup-dist.sh
cd -

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

echo "Retrieving Qserv archive"
git archive --remote=${QSERV_REPO} --format=tar --prefix=qserv/ ${QSERV_BRANCH} | gzip > ${QSERV_REPO_PATH}/upstream/qserv-${VERSION}.tar.gz ||
(echo "Unable to download qserv archive"; exit 1)

cd ${QSERV_REPO_PATH} &&

# update repos
git add upstream/qserv-${VERSION}.tar.gz &&
git commit -m "Packaging qserv-${VERSION}" &&
git tag | head -1 | xargs git tag -f &&
git push origin master -f --tags &&

echo "Retrieving Qserv tests dataset"
git archive --remote=${DATA_REPO} --format=tar --prefix=testdata/ ${DATA_BRANCH} | gzip > ${LOCAL_PKGROOT}/tarballs/testdata-${VERSION}.tar.gz || 
(echo "Unable to download tests dataset"; exit 1)

echo "Distributing Qserv"
cd - &&
eups distrib install git --repository="http://lsst-web.ncsa.illinois.edu/~mjuric/pkgs" &&
setup git &&
eups_dist qserv ${VERSION} ||
(echo "Unable to distribute Qserv"; exit 1)
 
echo "Downloading scisql"
SCISQL_ARCHIVE=scisql-0.3.2.tar.bz2
if [ ! -f ${LOCAL_PKGROOT}/tarballs/${SCISQL_ARCHIVE} ]; then
    mkdir -p ${LOCAL_PKGROOT}/tarballs
    wget https://launchpad.net/scisql/trunk/0.3.2/+download/scisql-0.3.2.tar.bz2 --directory-prefix=${LOCAL_PKGROOT}/tarballs
fi
 

upload_to_distserver
