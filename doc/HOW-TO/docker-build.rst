#############################
Build a Qserv container image
#############################

.. note::

   This procedure is launched for each new commit inside `Travis CI <https://travis-ci.com/github/lsst/qserv>`_.
   It possible possible to retrieve the tag of the produced Qserv image on top of build log,
   the log line start with `Building and testing qserv/qserv:<tag>`.

Choose a Qserv dependencies image
=================================

Qserv dependencies images are built by script :file:`admin/tools/docker/lsst-dm-ci/dev_images.sh`.
This script can be ran or a workstation or inside `LSST-ci <https://ci.lsst.codes/blue/organizations/jenkins/dax%2Fdocker%2Fbuild-dev/activity>`_ in order to easilly
build a new Qserv dependencies image.

- Build or select a Qserv dependencies image inside the `Qserv container repository <https://hub.docker.com/r/qserv/qserv>`_.
  Qserv dependencies image start with a tag prefixed with ``deps_``.
  For example, for image `qserv/qserv:deps_20200130_1236`, the image tag is `deps_20200130_1236`.
- Replace `DEPS_TAG_DEFAULT` value with the image tag above in `admin/tools/docker/conf.sh`

Build a Qserv container image
=============================

Simply run:

.. code-block:: bash

   ./admin/tools/docker/2_build-git-image.sh <path-to-qserv-source>
