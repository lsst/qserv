.. _admin-upgrade:

Instructins for upgrading Qserv to newer releases
=================================================

.. _admin-upgrade-mariadb-11-4-4:

Upgrading Qserv to MariaDB 11.4.4
---------------------------------

MariaDB 11.4.4 is a major release that introduces a number of new features and
improvements. If your instance of Qserv is still based on MariaDB 10.6.8, you
need to upgrade your data to the new version.

To upgrade your Qserv installation to MariaDB 11.4.4, follow these steps:

- Read the `MariaDB 11.4.4 release notes <https://mariadb.com/kb/en/mariadb-11-4-4-release-notes>`_ to learn about the new features and improvements.
- Read the guide `Upgrading from MariaDB 10.6 to MariaDB 10.11 <https://mariadb.com/kb/en/upgrading-from-mariadb-10-6-to-mariadb-10-11>`_ to learn how to upgrade your databases.
- Turn Qserv into the database-only mode by ensuring that all but the MariaDB services are being run.
- Log into the MariaDB containers and run the database upgrade command as shown below:

  .. code-block:: bash

    mariadb-upgrade -P3306 -h127.0.0.1 --protocol=tcp -uroot -p******

  Where ``-P`` specifies the port number, ``-h`` specifies the host, ``-u`` specifies the user, and ``-p`` specifies the password.
  Note that the port number may be different in your installation.

Experiments with the production databases at USDF (SLAC) have shown that the upgrade process is safe and reliable.
However, it is still recommended to make a backup of your data before proceeding with the upgrade.
