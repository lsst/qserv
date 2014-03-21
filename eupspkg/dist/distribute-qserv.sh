BASEDIR=$(dirname $0)
cd ${BASEDIR}/..
source ${BASEDIR}/setup-dist.sh
cd -

QSERV_REPO=ssh://git@dev.lsstcorp.org/LSST/DMS/qserv
QSERV_BRANCH=tickets/3100

QSERV_REPO_PATH=${DEPS_DIR}/qserv

rm -f ${LOCAL_PKGROOT}/tables/qserv-${VERSION}.table

# retrieve package
git archive --remote=${QSERV_REPO} --format=tar --prefix=qserv/ ${QSERV_BRANCH} | gzip > ${QSERV_REPO_PATH}/upstream/qserv-${VERSION}.tar.gz 

cd ${QSERV_REPO_PATH}

# update repos
git add upstream/qserv-${VERSION}.tar.gz
git commit -m "Packaging qserv-${VERSION}"
git tag | head -1 | xargs git tag -f
git push origin master -f --tags

# distribute
cd -
setup git
eups_dist qserv ${VERSION} 

${BASEDIR}/upload-to-distserver.sh 
