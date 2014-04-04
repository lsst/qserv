eups_install() {

    echo  "Installing eups using : '$EUPS_GIT_CLONE_CMD'"

    # cleaning
    if [ -e "${INSTALL_DIR}/eups/bin/setups.sh" ]
    then   
        echo "Removing previous install"
        source "${INSTALL_DIR}/eups/bin/setups.sh"
        eups_unsetup_all
        eups_remove_all
    fi

    rm -rf ~/.eups/ups_db ~/.eups/_caches_

    # installing eups latest version
    cd ${INSTALL_DIR}
    rm -rf sources &&
    mkdir sources &&
    cd sources &&
    ${EUPS_GIT_CLONE_CMD} &&
    cd eups/ &&
    ${EUPS_GIT_CHECKOUT_CMD} &&
    ./configure --prefix="${INSTALL_DIR}/eups" \
    --with-eups="${INSTALL_DIR}/stack"&&
    make &&
    make install ||
    {
        echo "Failed to install eups" >&2
        exit 1
    }
}

eups_dist() {
    if [ -z "$1" -o -z "$2" ]; then
        echo "eups_dist requires at least two arguments"
        exit 1
    fi
    local product=$1 &&
    local version=$2

    if [ -z "$3" ]; then
        gitrepo=${REPOSITORY_BASE_CONTRIB}
    else
        gitrepo=$3
    fi

    CWD=${PWD} &&
    TMP_DIR=${INSTALL_DIR}/tmp &&
    mkdir -p ${TMP_DIR} &&
    cd ${TMP_DIR} &&
    rm -rf ${product} &&
    git clone $gitrepo/${product} &&
    cd ${product} &&
    git checkout -q ${version} -- ups &&
    eups_dist_create $product $version
}

eups_dist_create () {
    if [ -z "$1" -o -z "$2" ]; then
        echo "eups_dist_create requires two arguments"
        exit 1
    fi
    local product=$1 &&
    local version=$2
    cmd="eups declare ${product} ${version} -r ." &&
    echo "CMD : $cmd" &&
    $cmd &&
    cmd="eups distrib create --nodepend --server-dir=${LOCAL_PKGROOT} -f generic -d eupspkg -t current ${product} ${version}"
    echo "Running : $cmd" &&
    $cmd &&
    # for debug purpose only : build file generation
    # eups expandbuild -V ${version} ups/${product}.build >
    # ${product}-${version}.build
    cd ${CWD} ||
    {
        echo "ERROR : while creating package $product, $version"
    }
}

eups_remove_all() {
    echo "INFO : removing all packages except git"
    eups list  | grep -v git | cut -f1 |  awk '{print "eups remove -t current --force "$1}' | bash
}

eups_unsetup_all() {
    echo "INFO : unsetup of all packages"
    eups list | grep -w setup | cut -f1 |  awk '{print "unsetup "$1}' | bash
}

upload_to_distserver() {
    cp ${QSERV_SRC_DIR}/eupspkg/dist/.htaccess ${LOCAL_PKGROOT}
    lftp -e "mirror -Re ${LOCAL_PKGROOT} www/htdocs/qserv/; quit" sftp://datasky:od39yW0e@datasky.in2p3.fr/
}
