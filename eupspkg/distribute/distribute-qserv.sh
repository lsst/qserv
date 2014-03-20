QSERV_REPO_PATH=${HOME}/eupspkg/contrib/qserv

rm /opt/eupspkg-qserv/distserver/tables/qserv-6.0.0rc1.table

# retrieve package
cp ~fjammes/tmp/qserv-6.0.0rc1.tar.gz ${QSERV_REPO_PATH}/upstream/
cd ${QSERV_REPO_PATH}

# update repos
git add upstream/qserv-6.0.0rc1.tar.gz
git commit --amend
git tag | head -1 | xargs git tag -f
git pom -f --tags

# distribute
cd -
source setup-eups.sh
setup git
eups_dist qserv 6.0.0rc1

./upload-to-distserver.sh 
