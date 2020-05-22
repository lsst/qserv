# shellcheck shell=bash

export LIBCURL_DIR=$CONDA_PREFIX
export LOG4CXX_DIR=$CONDA_PREFIX
export LUA_DIR=$CONDA_PREFIX
export PYBIND11_DIR=$CONDA_PREFIX


build() {
  scons -j"$NJOBS" prefix="$PREFIX" version="$VERSION" CXX="$CXX" \
    --verbose
}

# scons rechecks the compiler even when installing...
install() {
  scons -j"$NJOBS" prefix="$PREFIX" version="$VERSION" CXX="$CXX" install \
    --verbose

  if [[ -d "ups" && ! -d "$PREFIX/ups" ]]; then
    install_ups
  fi
}
