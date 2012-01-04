#!/bin/sh
# 
# Description: Quick install script to put Python files in the right place
#
# Purpose: Copy python modules into a target place.  Puts in 
# the boilerplate __init__.py files where necessary.
#
# (Please migrate into SCons if you can do so elegantly.)

if ! [[ -d $1 ]] ; then
    echo "Refusing to install to: $1"
    echo "Please specify a target directory."
    exit 1
fi

if ! [[ -d "python" ]] ; then
    echo "Please run from the qserv top-level directory, which should"
    echo "contain a python subdirectory."
    exit 1
fi


TARGET=$1
CP=/usr/bin/cp

echo "Installing to $1"
copyContentsWithBoilerplate () {
    local src=$1
    local tar=$2
    for f in `ls $src` ; do
	if [[ -d $src/$f ]] ; then
	    mkdir -p $tar/$f
	    local moduleFile="$tar/$f/__init__.py"
	    copyContentsWithBoilerplate $src/$f $tar/$f
	    if ! [[ -f $moduleFile ]] ; then
		echo "Touching $moduleFile as boilerplate."
		touch $moduleFile
	    fi
	else
	    echo "Copying $src/$f $tar"
	    cp $src/$f $tar
	fi
	
    done
}

copyContentsWithBoilerplate python $TARGET

    