# shellcheck shell=bash

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
