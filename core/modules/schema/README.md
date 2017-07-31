Qserv tools for schema migration
================================

This package contains code for managing of the schema migration process
for Qserv databases (e.g. QMeta or CSS). The code in this package is a
generic framework, specific details of migration process are implemented
in the packages which define schema and provide migration scripts for the
schema.

Main item in this framework is a `qserv-smig.py` application, this
application does not have direct dependency on any specific schema, instead
it is designed to discover and load schema-specific code at run time. Here
is how it works:
- application takes the name of the module that is supposed to provide
  migration module for its schema
- given that name `smig` imports a module with the name
  `lsst.qserv.<name>.schema_migration`, this module is supposed to
  implement a factory method with the name `make_migration_manager`.
- `smig` calls this method with a bunch of parameters, and method should
  return an instance of the special _migration manager_ class which
  implements interface defined by `schema.SchemaMigMgr` class.
- `smig` call various methods of this instance to either obtain
  information about schema or perform schema migration.

The modules that need to support schema migration for the databases defined
in these modules typically need to implement following:
- define a Python module with the name `schema_migration`, this typically
  goes into `python` sub-directory of the module and is installed in
  `lib/python/lsst/qserv/<module>` folder to be imported by `smig`.
- that module has to implement single method `make_migration_manager`
  which takes few parameters (check `qmeta` module for example) and returns
  new instance of `schema.SchemaMigMgr` class.

The methods in `schema.SchemaMigMgr` sub-classes can be implemented in
any reasonable way. Typical implementation can, for example, be defined in
terms of separate SQL scripts which are installed in `share/qmeta/schema`
sub-directory (this is default location used by `smig`) and named to
reflect version numbers of the schema (e.g. `migrate-0-to-1.sql`).

There is an implementation of these concepts in `qmeta` module, presently
it supports migration of `QMeta` schema from version 0 to version 1. That
module can be used as an example for implementing migration for other modules.

Performing migration
====================

`qserv-smig.py` is the main tool for all migration-related tasks. Because
it can run migration for any module it needs a bunch of parameters to
identify particular module and its database. The module name s passed as a
positional parameter to the script. Database connection can be specified in
couple different ways:
- as a sqlalchemy-style URL using -c option: `-c mysql://user:pass@host:port/dbName`
- as a reference to a configuration section in some INI file:
  `-f etc/qserv-czar.cnf -s css`.

Latter case can be used when database connection parameters are stores in
existing configuration files (e.g. files in existing qserv installation),
though in many cases schema migration will require administrator-level
access so URL-style connection would be more convenient in that case.

Here are few use cases for `qserv-smig` script.

Dump schema information
-----------------------

Running script without any option will dump some useful info, for example:

    $  qserv-smig.py -c mysql://root:***@127.0.0.1:13306/qservMeta qmeta
    Current schema version: 0
    Latest schema version: 1
    Known migrations:
      0 -> 1 : migrate-0-to-1.sql (X)
    Database would be migrated to version 1

The information displayed here includes:
- schema version currently defined by database,
- latest schema version which is determined as highest schema number in
  migration scripts,
- migration steps - starting version, final version and corresponding script,
  `(X)` marks scripts that would be applied to current setup,
- final version that would be active after migration

Check schema version
--------------------

To check that migration is needed for current setup `--check` option
could be used:

    $  qserv-smig.py --check -c mysql://root:***@127.0.0.1:13306/qservMeta qmeta

This will print the same info but will also return code to the shell,
code 0 means that schema does not need migration, code 1 means that schema
needs update.

Migrating schema
----------------

Actual migration is only performed if option `-m` appears on command line:

    $  qserv-smig.py -m -c mysql://root:***@127.0.0.1:13306/qservMeta qmeta
    Current schema version: 0
    Latest schema version: 1
    Known migrations:
      0 -> 1 : migrate-0-to-1.sql (X)
    Database was migrated to version 1

It prints the same info but last line now says that schema was updated. Use
`-v` option to also see extra info during migration process.

Normally `smig` tries to update schema to the latest version, but it is
possible to stop migration process at specific version by using `-n` option:

    $  qserv-smig.py -m -n 100 -c mysql://root:***@127.0.0.1:13306/qservMeta qmeta

this will stop migration after version 100 even if there migration scripts
for later versions.
