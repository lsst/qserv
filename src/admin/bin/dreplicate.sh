#!/bin/sh

# LSST Data Management System
# Copyright 2014 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.


# Replicate directories to a number of servers. This is a temporary
# solution for data replication on small-scale cluster (or mosly 
# for testing simple multi-node setup). This will likely be replaced
# with some cluster management-based solution as we prepare for
# actual deployment.

# @author  Andy Salnikov, SLAC

set -e

usage() {
  cat << EOD

Usage: `basename $0` [options] path host [host ...]

  Available options:
    -h          this message
    -d path     use different location on remote hosts
    -u name     use different remote user name

  Copies specified path which can be a single file or a directory to one 
or more remote hosts. path argument must specify absolute path name (start 
with slash). Directories are replicated recursively.

EOD
}

dest=''
user=''

# get the options
while getopts hd:u: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    d) dest="$OPTARG" ;;
	    u) user="${OPTARG}@" ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -lt 2 ] ; then
    usage
    exit 2
fi

path=$1
shift
hosts="$@"

case "$path" in
    /*) ;;
    *) echo "expect absolute path" ; exit 2 ;;
esac      

# strip trailing slash
path=$(echo $path | sed 's%\(.*[^/]\)/*%\1%')

test "$dest" || dest=$(dirname "$path")
 
for host in $hosts; do
    echo "Syncronizing $path to remote $host:$dest"
    rsync --rsh=ssh -a --delete "$path" "$user$host:$dest"
done
