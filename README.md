# Qserv: petascale distributed database

## Continuous integration for master branch

Build Qserv and run multi-node integration tests.

| CI       | Status                                                                                                                                         | Image build  | e2e tests   | Documentation generation  | Static code analysis  | Image security scan |
|----------|------------------------------------------------------------------------------------------------------------------------------------------------|--------------|-------------|---------------------------|-----------------------|---------------------|
| Gihub    | [![Qserv CI](https://github.com/lsst/qserv/workflows/CI/badge.svg?branch=master)](https://github.com/lsst/qserv/actions?query=workflow%3A"CI") | Yes          | No          | https://qserv.lsst.io/    | No                    | Yes                 |
| Travis   | [![Build Status](https://travis-ci.org/lsst/qserv.svg?branch=master)](https://travis-ci.org/lsst/qserv)                                        | Yes          | Yes (shmux) | No                        | No                    | No                  |

## Documentation

Access to [Qserv documentation at https://qserv.lsst.io](https://qserv.lsst.io/)

# Code analysis

[Security overview](https://github.com/lsst/qserv/security)