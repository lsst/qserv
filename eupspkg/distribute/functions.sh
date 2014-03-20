eups_dist() {
    if [ -z "$1" -o -z "$2" ]; then
        echo "eups_dist requires two arguments"
        exit 1
    fi
    product=$1
    version=$2
    CWD=${PWD} &&
    cd ${INSTALL_DIR}/tmp &&
    rm -rf ${product} &&
    git clone ${REPOSITORY_BASE}/${product} &&
    cd ${product} &&
    git checkout -q ${version} -- ups &&
    cmd="eups declare ${product} ${version} -r ." &&
    echo "CMD : $cmd" &&
    $cmd &&
    cmd="eups distrib create --server-dir=${INSTALL_DIR}/distserver -f generic -d eupspkg -t current -S SOURCE=git -S REPOSITORY_PATH=$REPOSITORY_PATH ${product}" &&
    echo "CMD : $cmd" &&
    $cmd &&
    # for debug purpose only : build file generation
    # eups expandbuild -V ${version} ups/${product}.build > ${product}-${version}.build
    cd ${CWD}
}

eups_remove_all() {
    echo "INFO : removing all packages except git"
    eups list  | grep -v git | cut -f1 |  awk '{print "eups remove -t current --force "$1}' | bash
}

eups_unsetup_all() {
    echo "INFO : unsetup of all packages"
    eups list | grep -w setup | cut -f1 |  awk '{print "unsetup "$1}' | bash
}
