# Sample data

The dataset is available in repository:
git://git.lsstcorp.org/LSST/DMS/testdata/qserv_testdata.git
See datasets/case<id>/data directories

# MySQL permissions

See ../../admin/templates/configuration/tmp/configure/sql/qserv-czar.sql
and ../../admin/templates/configuration/tmp/configure/sql/qserv-worker.sql
to check required permissions on Qserv czar and worker node

# Qserv configuration

- *.lsp.cf files contains xrootd configuration examples
- see ../../admin/templates/configuration/etc/lsp.cf to check default
  configuration template for xrootd (on the worker side)
-  see ../../admin/templates/configuration/etc/qserv-czar.cnf to check
  default configuration template for Qserv Czar.

