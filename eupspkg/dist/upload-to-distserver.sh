SCISQL_ARCHIVE=scisql-0.3.2.tar.bz2

if [ ! -f ${LOCAL_PKGROOT}/tarballs/${SCISQL_ARCHIVE} ]; then
    mkdir -p ${LOCAL_PKGROOT}/tarballs
    wget https://launchpad.net/scisql/trunk/0.3.2/+download/scisql-0.3.2.tar.bz2 --directory-prefix=${LOCAL_PKGROOT}/tarballs
fi

cp .htaccess ${LOCAL_PKGROOT}
lftp -e lftp -e "mirror -R ${LOCAL_PKGROOT} ~/htdocs/qserv/distserver" sftp://datasky:od39yW0e@datasky.in2p3.fr/
