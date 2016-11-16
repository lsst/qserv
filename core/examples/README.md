# Sample data

The dataset is available in repository:
git://git.lsstcorp.org/LSST/DMS/testdata/qserv_testdata.git
See datasets/case<id>/data directories

# MySQL permissions

See ../../admin/templates/configuration/tmp/configure/sql/qserv-czar.sql
and ../../admin/templates/configuration/tmp/configure/sql/qserv-worker.sql
to check required permissions on Qserv czar and worker node

# Qserv configuration

Default configuration template for Qserv Czar:
     ../../admin/templates/configuration/etc/qserv-czar.cnf

Default configuration for xrootd (manager and server side):
    ../../admin/templates/configuration/etc/lsp.cf

Default configuration for XrdSsi plugin:
    ../../admin/templates/configuration/etc/xrdssi.cnf

