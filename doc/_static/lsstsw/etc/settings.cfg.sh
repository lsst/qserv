#
# Config file with environment variables used by ~lsstsw builder
#

# top-level products
export PRODUCTS=${PRODUCTS:-"lsst_sims lsst_distrib qserv git anaconda"}

# set it to nonempty to prevent versiondb from being pushed upstream
# unless you're the automated LSST software account
if [[ $USER != 'lsstsw' || $(hostname) != 'lsst-dev.ncsa.illinois.edu' ]]; then
        export NOPUSH=1
fi

#
# the settings below should rarely need changing
#

# where are we? default to $HOME if $LSSTSW hasn't been defined
export LSSTSW=${LSSTSW:-$HOME}

# use 'package' for public releases, use 'git' for development releases
export EUPSPKG_SOURCE=${EUPSPKG_SOURCE:-git}

# the location of the distribution server
export EUPS_PKGROOT=$LSSTSW/distserver/production

# the location of source repositories
BASE='git://git.lsstcorp.org/LSST'
SBASE='https://stash.lsstcorp.org/scm'
export REPOSITORY_PATTERN="$BASE/sims/%(product)s.git|$BASE/DMS/%(product)s.git|$BASE/DMS/devenv/%(product)s.git|$BASE/DMS/testdata/%(product)s.git|$BASE/external/%(product)s.git"

# location of the build directory
export BUILDDIR=$LSSTSW/build

# repository path for 'eups distrib create'
export EUPSPKG_REPOSITORY_PATH="$BUILDDIR"/'$PRODUCT'

# location of the EUPS stack
export EUPS_PATH=$LSSTSW/stack

# location of the version repository (it should be a clone of git@git.lsstcorp.org/LSST/DMS/devenv/versiondb.git)
export VERSIONDB=$LSSTSW/versiondb

# location of exclusions.txt file for 'lsst-build prepare' command
export EXCLUSIONS=$LSSTSW/etc/exclusions.txt
