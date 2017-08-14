"""Module defining SchemaMigMgr abstract base class.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["SchemaMigMgr"]

#--------------------------------
#  Imports of standard modules --
#--------------------------------
from abc import ABCMeta, abstractmethod
import logging
import os

#-----------------------------
# Imports for other modules --
#-----------------------------

#----------------------------------
# Local non-exported definitions --
#----------------------------------

_log = logging.getLogger(__name__)

#------------------------
# Exported definitions --
#------------------------


class SchemaMigMgr:
    """Abstract base class for schema migration managers.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def current_version(self):
        """Returns current schema version.

        Returns
        -------
        Integer number
        """
        pass

    @abstractmethod
    def latest_version(self):
        """Returns latest known schema version.

        Returns
        -------
        Integer number
        """
        pass

    @abstractmethod
    def migrations(self):
        """Returns all known migrations.

        Returns
        -------
        List of tuples, each tuple contains three elements:
        - starting schema version (int)
        - final schema version (usually starting + 1)
        - migration script name (or any arbitrary string)
        """
        pass

    @abstractmethod
    def migrate(self, version=None, do_migrate=False):
        """Perform schema migration from current version to given version.

        Parameters
        ----------
        version : int or None
            If None then migrate to latest known version, otherwise only
            migrate to given version.
        do_migrate : bool
            If True performa migration, otherwise only print steps that
            should be performed

        Returns
        -------
        None if no migrations were performed or the version number at
        which migration has stopped.

        Raises
        ------
        Exception is raised for any migration errors. The state of the
        database is not guaranteed to be consistent after exception.
        """
        pass

    @classmethod
    def findScripts(cls, scripts_dir, match_fun):
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
        List of tuples, one tuple per script, each tuple contains three items:
            from_version : int
                Schema version number to apply this script to
            to_version : int
                Schema version number for resulting schema
            name : str
                Script name (without directory)

        Raises
        ------
        Exception is raised if directory does not exist or if `match_fun`
        raises exception or returns unexpected value.
        """
        scripts = []
        for fname in os.listdir(scripts_dir):
            _log.debug("checking script %s", fname)
            match = match_fun(fname)
            if match is not None:
                v_from, v_to = match
                _log.debug("matching script %s: %s -> %s", fname, v_from, v_to)
                if v_from == v_to:
                    _log.warn("script %s has identical versions, skipping", path)
                else:
                    scripts.append((v_from, v_to, fname))
        return scripts
