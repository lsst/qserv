DISTSERVER=/opt/eupspkg-qserv/distserver
mkdir -p ${DISTSERVER}/tarballs
rm ${DISTSERVER}/tarballs/scisql-0.3.2.tar.bz2*
wget https://launchpad.net/scisql/trunk/0.3.2/+download/scisql-0.3.2.tar.bz2 --directory-prefix=${DISTSERVER}/tarballs
cp .htaccess ${DISTSERVER}
cp ~/tarballs/testdata.tar.gz ${DISTSERVER}/tarballs
lftp -e lftp -e "mirror -R ${DISTSERVER} ~/htdocs/qserv/distserver" sftp://datasky:od39yW0e@datasky.in2p3.fr/
