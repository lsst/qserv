"""Module defining SchemaMigMgr abstract base class.
"""

__all__ = ["SchemaMigMgr", "Uninitialized"]


from abc import ABCMeta, abstractmethod
import backoff
from contextlib import closing
import jinja2
import logging
import mysql.connector
# MySQLInterfaceError can get thrown, we need to catch it.
# It's not exposed as a public python object but *is* used in mysql.connector unit tests.
from _mysql_connector import MySQLInterfaceError
import os
import re
from sqlalchemy.engine.url import make_url

from lsst.qserv.admin.template import get_template_cfg
from lsst.qserv.admin.qserv_backoff import max_backoff_sec, on_backoff


_log = logging.getLogger(__name__)
database_error_connection_refused_code = 2003


class SchemaUpdateRequired(RuntimeError):
    """Error that indicates that a schema update is required.

    It may be raised when the update could not be performed because necessary
    arguments (e.g. update=True) were not passed.
    """
    pass;


class Uninitialized:
    """Represents an uninitialized database, which has no integer version
    number.
    """

    def __init__(self):
        raise TypeError("UninitializedType is not callable")


class Version:
    """Extend numeric versions to support an ``Uninitialized`` value that
    is less than any integer value."""

    def __init__(self, value):
        if isinstance(value, Version):
            self.value = value.value
            return
        if not isinstance(value, int) and value is not Uninitialized:
            raise RuntimeError(
                f"Version value must be an int or Uninitialized, not {value}"
            )
        self.value = value

    @staticmethod
    def cast(other):
        """Ensure that the passed-in object is a Version instance; if it is
        just return it, if not create a Version and use it as the value.
        """
        if isinstance(other, Version):
            return other
        return Version(other)

    @staticmethod
    def _otherVal(other):
        """Get the value of the other value. May be a ``Version`` instance,
        ``Uninitialized``, or an ``int``.
        """
        if type(other) is int or other is Uninitialized:
            return other
        if type(other) is Version:
            return other.value
        raise TypeError(f"Unsupported value {other}.")

    def __lt__(self, other):
        otherVal = self._otherVal(other)
        return (
            self.value is Uninitialized
            and otherVal is not Uninitialized
            or (
                self.value is not Uninitialized
                and otherVal is not Uninitialized
                and self.value < otherVal
            )
        )

    def __gt__(self, other):
        return not self.__lt__(other) and not self.__eq__(other)

    def __ge__(self, other):
        return not self < other

    def __eq__(self, other):
        other = self._otherVal(other)
        if self.value is Uninitialized and other is Uninitialized:
            return True
        if self.value is Uninitialized or other is Uninitialized:
            return False
        return self.value == other

    def __hash__(self):
        return hash(self.value)

    def __repr__(self):
        return str(self.value)

    def __str__(self):
        return "Uninitialized" if self.value is Uninitialized else str(self.value)


class Migration:
    """Contains information about a schema migration; the "from" and "to"
    versions, the name of the migration file, and the full path of the
    migration file.
    """

    @property
    def dirname(self):
        return os.path.dirname(self.filepath)

    def __init__(self, from_version, to_version, name, filepath):
        self.from_version = Version(from_version)
        self.to_version = Version(to_version)
        self.name = name
        self.filepath = filepath  # path and filename

    def __eq__(self, other):
        return (self.from_version, self.to_version, self.name, self.filepath) == (
            other.from_version,
            other.to_version,
            other.name,
            other.filepath,
        )

    def __lt__(self, other):
        return (self.from_version, self.to_version, self.name, self.filepath) < (
            other.from_version,
            other.to_version,
            other.name,
            other.filepath,
        )

    def __hash__(self):
        return hash((self.from_version, self.to_version, self.name, self.filepath))

    def __repr__(self):
        return (
            f"Migration(from_version={self.from_version}, to_version={self.to_version}, "
            f"name={self.name}, filepath={self.filepath})"
        )


class SchemaMigMgr(metaclass=ABCMeta):

    """Abstract base class for schema migration managers.

    Parameters
    ----------
    scripts_dir : `str`
        The folder in which to look for migration scripts.
    connection : `str`
        The uri to the module database.
    """

    @abstractmethod
    def current_version(self):
        """Returns current schema version.

        Returns
        -------
        version : `int` or ``Uninitialized``
            The current schema version.
        """
        pass

    # mig_name_re describes a common migration file name pattern. Subclasses
    # can override this. This pattern looks for name "migrate-N-to-M.sql" or
    # "migrate-N-to-M-something.sql", and it can have a ".jinja" suffix. N can
    # be an integer or "None" (without quotes). Note that to use a different re
    # with the SchemaMigMgr implementation of current_version, the re must
    # include named capture groups for the "from" and "to" migration versions.
    mig_name_re = re.compile(
        r"migrate-(?P<from>\d+|None)-to-(?P<to>\d+)(-.*)?.sql(\.jinja)?\Z"
    )

    def __init__(self, scripts_dir, connection=None):
        # find all migration scripts, add full script path to each
        self.connection = None
        if connection:
            self._connect(connection)
        self.migrations = self.find_scripts(scripts_dir, self.script_match)

    def close(self):
        if self.connection:
            self.connection.close()

    @property
    def max_migration_version(self):
        """Get the migration with the highest 'to' version."""
        if not self.migrations:
            return None
        return max(m.to_version for m in self.migrations)

    @backoff.on_exception(
        exception=(
            mysql.connector.errors.DatabaseError,
            MySQLInterfaceError,
            mysql.connector.errors.InterfaceError,
            mysql.connector.errors.ProgrammingError,
        ),
        wait_gen=backoff.expo,
        on_backoff=on_backoff(log=_log),
        max_time=max_backoff_sec,
    )
    def _connect(self, connection):
        url = make_url(connection)
        # The database is not always guaranteed to exist yet (sometimes we connect and then create it)
        # so cache it, and it can be set in the connection before use when it is known to exist.
        self.database = url.database
        kwargs = dict(
            user=url.username,
            password=url.password,
        )
        if "socket" in url.query:
            kwargs["unix_socket"] = url.query["socket"]
        else:
            kwargs.update(host=url.host, port=url.port)
        self.connection = mysql.connector.connect(**kwargs)

    @backoff.on_exception(
        exception=mysql.connector.errors.DatabaseError,
        wait_gen=backoff.expo,
        on_backoff=on_backoff(log=_log),
        # Do give up, unless error is that the connection was refused (assume db is starting up)
        max_time=max_backoff_sec,
        giveup=lambda e: e.errno != database_error_connection_refused_code,
    )
    @backoff.on_exception(
        exception=mysql.connector.errors.OperationalError,
        wait_gen=backoff.expo,
        on_backoff=on_backoff(log=_log),
        max_time=max_backoff_sec,
        # Can not check errno for the failure reason, no codes are published in
        # docs. Assuming it's because the connection was rejected, and hoping that
        # retry will succeed.
    )
    def apply_migrations(self, migrations):
        """Apply migrations.

        Parameters
        ----------
        migrations : `list` [``Migrations``]
            Migrations to apply, in order.

        Returns
        -------
        version : `int`
            The current version number after applying migrations.
        """
        with closing(self.connection.cursor()) as cursor:
            for migration in migrations:
                _log.info("--- Executing migration script %s", migration.name)
                stmt = None
                if migration.filepath.endswith(".jinja"):
                    env = jinja2.Environment(
                        loader=jinja2.FileSystemLoader(migration.dirname),
                        autoescape=jinja2.select_autoescape("sql"),
                        undefined=jinja2.StrictUndefined,
                    )
                    try:
                        stmt = env.get_template(migration.name).render(get_template_cfg())
                    except jinja2.exceptions.UndefinedError as e:
                        raise RuntimeError(
                            f"A template parameter is missing from the configuration for {migration.filepath}: {e.message}"
                        ) from e
                else:
                    with open(migration.filepath, "r") as f:
                        stmt = f.read()
                _log.debug(f"Migration statment: {stmt}")
                if stmt:
                    for result in cursor.execute(stmt, multi=True):
                        pass
                else:
                    _log.warn("Migration statement was empty, nothing to execute.") # empty migration files are used for testing.
                self.connection.commit()
                _log.info("+++ Script %s completed successfully", migration.name)
        return migration.to_version

    def migrate(self, to_version=None, do_migrate=False):
        """Perform schema migration from current version to given version.

        Parameters
        ----------
        to_version : int or None
            If None then migrate to latest known version, otherwise only
            migrate to given version.
        do_migrate : bool
            If True perform the migration, otherwise only print steps that
            should be performed.

        Returns
        -------
        None if no migrations were performed or the version number at
        which migration has stopped.

        Raises
        ------
        Exception is raised for any migration errors. The state of the
        database is not guaranteed to be consistent after exception.
        """
        from_version = Version.cast(self.current_version())
        to_version = Version.cast(self.max_migration_version if to_version is None else to_version)
        # checks for requested to_version
        if from_version >= to_version:
            _log.debug(
                "current version (%s) is already same or newer as requested (%s)",
                from_version,
                to_version,
            )
            return None
        migrations = self.migration_path(from_version, to_version, self.migrations)
        if not migrations:
            raise ValueError(
                "No known scripts to migrate from version "
                f"{from_version} to version {to_version}."
            )
        if do_migrate:
            return self.apply_migrations(migrations)
        else:
            # Todo can probably format migrations more nicely.
            print(f"Would apply migrations: {migrations}")

    @staticmethod
    def migration_path(from_version, to_version, migrations):
        """Look in migrations and find the shortest possible path between
        versions.

        Parameters
        ----------
        from_version : ``Version`` or one of its supported types.
            The version to migrate from.
        to_version : ``Version`` or one of its supported types.
            The version to migrate to.
        migrations : `list` [`Migration`]
            The available migrations.

        Returns
        -------
        migrations: `list` [``Migration``] or `None`
            The shortest possible list of migrations from the two versions,
            `None` if there is not a possible way to migrate from the given
            versions.
        """
        # First see if any migrations exactly match our from and to versions.
        for migration in migrations:
            if (
                migration.from_version == from_version
                and migration.to_version == to_version
            ):
                return [migration]
        # See if there are intermedate migrations that go to our to_version and
        # start at a from_version that is higher than our desired from_version
        # (we have already checked for versions from our desired from_version).
        # If there are multiple paths from from_version to to_version, choose
        # the shortest one.
        mig = None
        for migration in migrations:
            if (
                migration.to_version == to_version
                and migration.from_version > from_version
            ):
                intermediate_mig = SchemaMigMgr.migration_path(
                    from_version, migration.from_version, migrations
                )
                if intermediate_mig and (
                    mig is None or len(intermediate_mig) + 1 < len(mig)
                ):
                    mig = intermediate_mig
                    mig.append(migration)
        return mig or []

    @classmethod
    def script_match(cls, fname):
        """Match script name against pattern.

        Returns
        -------
        versions : `None` or `tuple` [`int` or `None`, `int`]
            None if no match. Pair of versions if there is a match, the first
            item (the "from" value) may be `None` to indicate "from an
            uninitialized database", or an integer to indicate the version the
            script can upgrade from, and the second value will be an integer
            indicating the version the script will upgrade to.
        """
        match = cls.mig_name_re.match(fname)
        if match:
            v_from = match.group("from")
            v_from = Uninitialized if v_from == "None" else int(v_from)
            v_to = int(match.group("to"))
            return (v_from, v_to)
        return None

    def latest_version(self):
        """Returns latest known schema version.

        Returns
        -------
        version : `int`
            The latest known schema version.
        """
        version = self.max_migration_version
        _log.debug("latest version from migration scripts: %s", version)
        if version is None:
            # no migration scripts - current version is latest
            version = self.current_version()
            _log.debug("no migration scripts, returning current version %s", version)
        return version

    @classmethod
    def find_scripts(cls, scripts_dir, match_fun):
        """Helper function for finding migration scripts.

        Parameters
        ----------
        scripts_dir : str
            Path to a directory where scripts are located.
        match_fun : function
            Function which takes one argument which is a script name
            (without directory) and returns None if script is not useful
            or 2-tuple (from_version, to_version) with version numbers.
            If versions are identical then script is ignored.

        Returns
        -------
        migrations : `list` [``Migration``]
            List of Migration instances that describe possible migrations.

        Raises
        ------
        Exception is raised if directory does not exist or if `match_fun`
        raises exception or returns unexpected value.
        """
        migrations = []
        for fname in os.listdir(scripts_dir):
            _log.debug("checking script %s", fname)
            match = match_fun(fname)
            if match is not None:
                v_from, v_to = match
                _log.debug("matching script %s: %s -> %s", fname, v_from, v_to)
                if v_from == v_to:
                    _log.warn("script %s has identical versions, skipping", os.path.join(scripts_dir, fname))
                else:
                    migrations.append(
                        Migration(v_from, v_to, fname, os.path.join(scripts_dir, fname))
                    )
        return migrations

    def databaseExists(self, dbName):
        """Determine if a database exists

        Parameters
        ----------
        dbName : `str`
            The name of the database.

        Returns
        -------
        exists : `bool`
            True if the database exists, else False.
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("SHOW DATABASES")
            if (dbName,) in cursor.fetchall():
                return True
        return False
