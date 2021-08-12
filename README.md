# Qserv: petascale distributed database

## Master branch status

Continuous integration server launches Qserv build and also multi-node integration tests:

[![Build Status](https://travis-ci.org/lsst/qserv.svg?branch=master)](https://travis-ci.org/lsst/qserv)

## Documentation

[Documentation for master branch](https://qserv.lsst.io/)

## How to publish a new release

```
RELEASE="2021.8.1-rc1"
./publish-release.sh -t "$RELEASE"
