"""
Module defining methods and classes used by watcher.

@author Andy Salnikov
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
from past.builtins import basestring
from future.utils import with_metaclass
from abc import ABCMeta, abstractmethod
import logging
import time
import warnings

# -----------------------------
# Imports for other modules --
# -----------------------------
from lsst.qserv import css
from lsst.qserv import qmeta
from lsst.qserv.admin import nodeMgmt
from lsst.qserv.admin import nodeAdmin
from lsst.qserv.css import cssConfig
from lsst.qserv.wmgr.client import ServerError

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)

# ------------------------
# Exported definitions --
# ------------------------


class WatcherCss(object):
    """
    Class which implements high-level CSS operations for watcher.
    """

    # status values to ignore when searching for items to work on,
    # currently we look at all possible values so it's empty
    _skipStatus = []

    def __init__(self, config, wmgrSecretFile=None, czar=None):
        """
        @param config: Either string specifying URL for CSS connection or
                       dictionary specifying configuration for CssAccess
        @param wmgrSecretFile: path to a file with wmgr secret
        @param czar: optional czar wmgr config, tuple with host name and
                     port number, use when czar node is not registered in CSS
        """

        _LOG.debug('connecting to css: %s', config)
        if isinstance(config, basestring):
            config = cssConfig.configFromUrl(config)
        self.css = css.CssAccess.createFromConfig(config, "")
        self.wmgrSecretFile = wmgrSecretFile
        self.czar = czar

    def getDbs(self):
        """
        Get the set of databases that (might) need watcher attention.

        @return: mapping where database name is key and status is value
        """
        try:
            res = self.css.getDbStatus()
        except css.NoSuchKey as exc:
            # usually means CSS not initialized (/DBS key missing)
            warnings.warn(str(exc), RuntimeWarning)
            return {}
        except css.CssError as exc:
            _LOG.error("Failed to retrieve database status from CSS: %s", exc)
            return {}
        res = dict((k, v) for k, v in res.items() if v not in self._skipStatus)
        return res

    def getTables(self):
        """
        Get the set of tables that (might) need watcher attention.

        @return: mapping where tuple of names (db, table) is key  and status is value
        """
        res = {}
        try:
            dbNames = self.css.getDbNames()
        except css.NoSuchKey as exc:
            # usually means CSS not initialized (/DBS key missing)
            warnings.warn(str(exc), RuntimeWarning)
            return {}
        except css.CssError as exc:
            _LOG.error("Failed to retrieve database names from CSS: %s", exc)
            return {}

        for db in dbNames:
            try:
                for tbl, stat in self.css.getTableStatus(db).items():
                    if stat not in self._skipStatus:
                        res[(db, tbl)] = stat
            except css.NoSuchDb:
                # database may disappear so catch it
                pass
            except css.CssError as exc:
                # for other errors make a noise but try to continue
                _LOG.error("Failed to retrieve table status from CSS: %s", exc)
                return {}
        return res

    def setDbStatus(self, dbName, newStatus):
        """
        Update status of database in CSS.
        """
        self.css.setDbStatus(dbName, newStatus)

    def dropDb(self, dbName):
        """
        Drop database in CSS.
        """
        self.css.dropDb(dbName)

    def setTableStatus(self, dbName, tableName, newStatus):
        """
        Update status of table in CSS.
        """
        self.css.setTableStatus(dbName, tableName, newStatus)

    def dropTable(self, dbName, tableName):
        """
        Drop table in CSS.
        """
        self.css.dropTable(dbName, tableName)

    def getNodes(self):
        """
        Return tuple with two lists of nodes (NodeAdmin instances) for all
        active nodes. First item in the returned tuple is a list of czar nodes,
        second item is a list of worker nodes.
        """

        mgmt = nodeMgmt.NodeMgmt(self.css, self.wmgrSecretFile)

        czars = []
        workers = []
        try:
            # try get node(s) from CSS
            czars = mgmt.select(css.NODE_STATE_ACTIVE, 'czar')
            workers = mgmt.select(css.NODE_STATE_ACTIVE, 'worker')
        except css.CssError as exc:
            _LOG.error('Failed to get nodes from CSS: %s', exc)

        # add explicit car node if there was no czar in CSS
        if not czars and self.czar:
            host, port = self.czar
            czars.append(nodeAdmin.NodeAdmin(host=host, port=port, wmgrSecretFile=self.wmgrSecretFile))

        return czars, workers


class IExecutor(with_metaclass(ABCMeta, object)):
    """
    Class defining interface for executor.
    """

    @abstractmethod
    def createDb(self, dbName, options):
        """
        Create new database, returns nothing, throws exceptions on errors.

        @param dbName: database name
        @param options: options string, anything that appears after colon in
                      CSS status
        @return True on success, false if operation cannot be performed
                immediately (CSS should not be modified)
        @raise Exception:  if operation failed
        """
        pass

    @abstractmethod
    def dropDb(self, dbName, options):
        """
        Drop a database, returns nothing, throws exceptions on errors.

        @param dbName: database name
        @param options: options string, anything that appears after colon in
                      CSS status
        @return True on success, false if operation cannot be performed
                immediately (CSS should not be modified)
        @raise Exception:  if operation failed
        """
        pass

    @abstractmethod
    def createTable(self, dbName, tableName, options):
        """
        Create new table, returns nothing, throws exceptions on errors.

        @param dbName: database name
        @param tableName: table name
        @param options: options string, anything that appears after colon in
                      CSS status
        @return True on success, false if operation cannot be performed
                immediately (CSS should not be modified)
        @raise Exception:  if operation failed
        """
        pass

    @abstractmethod
    def dropTable(self, dbName, tableName, options):
        """
        Drop a table, returns nothing, throws exceptions on errors.

        @param dbName: database name
        @param tableName: table name
        @param options: options string, anything that appears after colon in
                      CSS status
        @return True on success, false if operation cannot be performed
                immediately (CSS should not be modified)
        @raise Exception:  if operation failed
        """
        pass


class Watcher(object):
    """
    This class encapsulates main logic of watcher.
    """

    def __init__(self, wcss, executor, interval=3.):
        """
        @param  wcss:     instance of WatcherCss class
        @param executor:  instance of a IExecutor (subclass)
        @param interval:  interval in seconds between CSS polling
        """
        self.wcss = wcss
        self.executor = executor
        self.interval = interval

    def run(self, once=False):
        """
        Main loop which never returns.

        Watches for changes in CSS and calls corresponding methods in executor.

        @param once:  If true then stop after one iteration, mostly for testing
        """

        while True:

            actions = []

            lastIterTime = time.time()
            # To avoid races in reading CSS order for processing is "DROP TABLE",
            # "DROP DB", "CREATE DB", "CREATE TABLE", and we re-read CSS in each case
            for (db, table), status in self.wcss.getTables().items():
                if status.startswith(css.KEY_STATUS_DROP_PFX):
                    options = status.split(':', 1)[1]
                    _LOG.info("Found DROP TABLE for %s.%s (options: %s)",
                              db, table, options)
                    actions.append((self._dropTable, db, table, options))

            for db, status in self.wcss.getDbs().items():
                if status.startswith(css.KEY_STATUS_DROP_PFX):
                    options = status.split(':', 1)[1]
                    _LOG.info("Found DROP DB for %s (options: %s)", db, options)
                    # DROP DB implies DROP TABLE for all tables, so we do not care about table status
                    actions.append((self._dropDb, db, options))
                if status.startswith(css.KEY_STATUS_CREATE_PFX):
                    options = status.split(':', 1)[1]
                    _LOG.info("Found CREATE DB for %s (options: %s)", db, options)
                    actions.append((self._createDb, db, options))

            for (db, table), status in self.wcss.getTables().items():
                if status.startswith(css.KEY_STATUS_CREATE_PFX):
                    options = status.split(':', 1)[1]
                    _LOG.info("Found CREATE TABLE for %s.%s (options: %s)", db, table, options)

                    #  check database status, it must be either READY or PENDING_CREATE
                    dbStat = self.wcss.getDbs().get(db)
                    if dbStat != css.KEY_STATUS_READY and not dbStat.startswith(css.KEY_STATUS_CREATE_PFX):
                        _LOG.error("inconsistent CSS data, table status = %s and db status = %s for %s.%s",
                                   dbStat, status, db, table)
                    else:
                        actions.append((self._createTable, db, table, options))

            # process everything
            for action in actions:
                method = action[0]
                try:
                    method(*action[1:])
                except Exception as exc:
                    _LOG.error("Failure while processing action: %s", exc, exc_info=True)

            # stop if requested
            if once:
                break

            # rest for a little while
            sleep = lastIterTime + self.interval - time.time()
            if sleep > 0:
                time.sleep(sleep)

    def _createDb(self, dbName, options):
        """
        Create new database on all nodes.
        """

        _LOG.info('Creating database %s', dbName)

        # call executor to perform action
        try:
            if self.executor.createDb(dbName, options):
                status = css.KEY_STATUS_READY
            else:
                status = None
        except Exception as exc:
            _LOG.error('Failure in executor: %s', exc)
            status = css.KEY_STATUS_FAILED_PFX + str(exc)
            raise
        finally:
            # update CSS
            if status is not None:
                _LOG.info('Set CSS status for database %s = %s', dbName, status)
                self.wcss.setDbStatus(dbName, status)

    def _dropDb(self, dbName, options):
        """
        Drop existing database on all nodes.
        """

        _LOG.info('Dropping database %s', dbName)

        # call executor to perform action
        try:
            if self.executor.dropDb(dbName, options):
                status = True
            else:
                status = None
        except Exception as exc:
            _LOG.error('Failure in executor: %s', exc)
            status = css.KEY_STATUS_FAILED_PFX + str(exc)
            raise
        finally:
            if status is True:
                # update CSS
                _LOG.info('Drop database %s from CSS', dbName)
                self.wcss.dropDb(dbName)
            elif status is not None:
                _LOG.info('Set CSS status for database %s = %s', dbName, status)
                self.wcss.setDbStatus(dbName, status)

    def _createTable(self, dbName, tableName, options):
        """
        Create new database on all nodes.
        """

        _LOG.info('Creating table %s.%s', dbName, tableName)

        # call executor to perform action
        try:
            if self.executor.createTable(dbName, tableName, options):
                status = css.KEY_STATUS_READY
            else:
                status = None
        except Exception as exc:
            _LOG.error('Failure in executor: %s', exc)
            status = css.KEY_STATUS_FAILED_PFX + str(exc)
            raise
        finally:
            # update CSS
            if status is not None:
                _LOG.info('Set CSS status for table %s.%s = %s', dbName, tableName, status)
                self.wcss.setTableStatus(dbName, tableName, status)

    def _dropTable(self, dbName, tableName, options):
        """
        Drop existing database on all nodes.
        """

        _LOG.info('Dropping table %s.%s', dbName, tableName)

        # call executor to perform action
        try:
            if self.executor.dropTable(dbName, tableName, options):
                status = True
            else:
                status = None
        except Exception as exc:
            _LOG.error('Failure in executor: %s', exc)
            status = css.KEY_STATUS_FAILED_PFX + str(exc)
            raise
        finally:
            if status is True:
                # update CSS
                _LOG.info('Drop table %s.%s from CSS', dbName, tableName)
                self.wcss.dropTable(dbName, tableName)
            elif status is not None:
                _LOG.info('Set CSS status for table %s.%s = %s', dbName, tableName, status)
                self.wcss.setTableStatus(dbName, tableName, status)


class QservExecutor(IExecutor):
    """
    Standard implementation of IExecutor to work with qserv cluster.
    """

    def __init__(self, wcss, qmetaConfig):
        """
        @param wcss:     instance of WatcherCss class
        @param qmetaConfig: dictionary specifying configuration for QMeta
        """

        IExecutor.__init__(self)

        self.wcss = wcss
        self.qmeta = qmeta.QMeta.createFromConfig(qmetaConfig)

    def createDb(self, dbName, options):
        """
        Create new database, returns nothing, throws exceptions on errors.
        """

        _LOG.info('Creating database %s (options: %s)', dbName, options)

        czars, workers = self.wcss.getNodes()
        nodes = czars + workers
        if not nodes:
            _LOG.error('Could not find any nodes to work on')
            return False

        # TODO we should do it in parallel for better scaling
        nCreated = 0
        for node in nodes:

            wmgr = node.wmgrClient()
            if dbName in wmgr.databases():
                _LOG.info('Database %r already exists on node %r', dbName, node.name())
            else:
                try:
                    wmgr.createDb(dbName)
                    _LOG.info('Created database %r on node %r', dbName, node.name())
                    nCreated += 1
                except ServerError as exc:
                    if exc.code == 409:
                        _LOG.info('Database %r already exists on node %r', dbName, node.name())
                    else:
                        raise

        _LOG.info('Created databases on %d nodes out of %d', nCreated, len(nodes))
        return True

    def dropDb(self, dbName, options):
        """
        Drop a database, returns nothing, throws exceptions on errors.
        """

        _LOG.info('Dropping database %s (options: %s)', dbName, options)

        # before we drop database we need to make sure that no queries are using it
        qids = self.qmeta.getQueriesForDb(dbName)
        if qids:
            # some queries are still using it, try next time
            _LOG.info('Database %s is in use by %d queries', dbName, len(qids))
            return False

        czars, workers = self.wcss.getNodes()
        nodes = [(node, 'czar') for node in czars] + [(node, 'worker') for node in workers]
        if not nodes:
            _LOG.error('Could not find any nodes to work on')
            return False

        # TODO we should do it in parallel for better scaling
        nDropped = 0
        for node, nodeType in nodes:

            wmgr = node.wmgrClient()
            if dbName not in wmgr.databases():
                _LOG.info('Database %r does not exist on node %r', dbName, node.name())
            else:
                if nodeType == 'worker':
                    # Try to remove this database from chunk inventory
                    try:
                        # Do not restart xrootd
                        restart = False
                        wmgr.xrootdUnregisterDb(dbName, restart)
                        _LOG.info('Removed database %r from chunk registry on node %r', dbName, node.name())
                    except ServerError as exc:
                        if exc.code == 409:
                            _LOG.info('Database %r is not in chunk registry on node %r', dbName, node.name())
                        else:
                            raise
                try:
                    wmgr.dropDb(dbName)
                    _LOG.info('Dropped database %r on node %r', dbName, node.name())
                    nDropped += 1
                except ServerError as exc:
                    if exc.code == 404:
                        _LOG.info('Database %r does not exist on node %r', dbName, node.name())
                    else:
                        raise

        _LOG.info('Dropped databases on %d nodes out of %d', nDropped, len(nodes))
        return True

    def createTable(self, dbName, tableName, options):
        """
        Create new table, returns nothing, throws exceptions on errors.
        """

        _LOG.info('Creating table %s.%s (options: %s)', dbName, tableName, options)

        czars, workers = self.wcss.getNodes()
        nodes = czars + workers
        if not nodes:
            _LOG.error('Could not find any nodes to work on')
            return False

        # TODO we should do it in parallel for better scaling
        nCreated = 0
        for node in nodes:

            wmgr = node.wmgrClient()
            if tableName in wmgr.tables(dbName):
                _LOG.info('Table %r.%r already exists on node %r', dbName, tableName, node.name())
            else:
                try:
                    wmgr.createTable(dbName, tableName)
                    _LOG.info('Created table %r.%r on node %r', dbName, tableName, node.name())
                    nCreated += 1
                except ServerError as exc:
                    if exc.code == 409:
                        _LOG.info('Table %r.%r already exists on node %r', dbName, tableName, node.name())
                    else:
                        raise

        _LOG.info('Created tables on %d nodes out of %d', nCreated, len(nodes))
        return True

    def dropTable(self, dbName, tableName, options):
        """
        Drop a table, returns nothing, throws exceptions on errors.
        """

        _LOG.info('Dropping table %s.%s (options: %s)', dbName, tableName, options)

        # before we drop table we need to make sure that no queries are using it
        qids = self.qmeta.getQueriesForTable(dbName, tableName)
        if qids:
            # some queries are still using it, try next time
            _LOG.info('Table %s.%s is in use by %d queries', dbName, tableName, len(qids))
            return False

        czars, workers = self.wcss.getNodes()
        nodes = czars + workers
        if not nodes:
            _LOG.error('Could not find any nodes to work on')
            return False

        # TODO we should do it in parallel for better scaling
        nDropped = 0
        for node in nodes:

            wmgr = node.wmgrClient()
            if tableName not in wmgr.tables(dbName):
                _LOG.info('Table %r.%r does not exist on node %r', dbName, tableName, node.name())
            else:
                try:
                    wmgr.dropTable(dbName, tableName)
                    _LOG.info('Dropped table %r.%r on node %r', dbName, tableName, node.name())
                    nDropped += 1
                except ServerError as exc:
                    if exc.code == 409:
                        _LOG.info('Table %r.%r does not exist on node %r', dbName, tableName, node.name())
                    else:
                        raise

        _LOG.info('Dropped tables on %d nodes out of %d', nDropped, len(nodes))

        # if there is a QMeta info for this DROP TABLE query then mark it as done
        for option in options.split(':'):
            if option.startswith('qid='):
                try:
                    qid = int(option[4:])
                except ValueError as ex:
                    qid = 0
                # qid=0 means there is no QMeta info
                if qid:
                    try:
                        _LOG.debug('Updating QMeta status for qid=%s', qid)
                        self.qmeta.completeQuery(qid, qmeta.QInfo.COMPLETED)
                        self.qmeta.finishQuery(qid)
                    except qmeta.QMetaError as ex:
                        # should go on
                        _LOG.warning('Failed to update QMeta status for DROP TABLE: %s', ex)

        return True
