# Qserv: petascale distributed database

Build Qserv and run Qserv multi-node integration tests in k8s (using a fixed qserv-operator version)

| CI       | Status                                                                                                                                                           | Image build  | e2e tests | Documentation generation        | Static code analysis  | Image security scan |
|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|-----------|---------------------------------|-----------------------|---------------------|
| Gihub    | [![Qserv CI](https://github.com/lsst/qserv/workflows/CI/badge.svg?branch=master)](https://github.com/lsst/qserv/actions?query=workflow%3A"CI") | Yes          | No        | https://qserv.lsst.io/ (obsolete) | Yes                   | Yes                 |


## Documentation

[Documentation for master branch](https://qserv.lsst.io/)

## How to publish a new release

```
RELEASE="2021.8.1-rc1"
./publish-release.sh "$RELEASE"
