#
# LSST Data Management System
# Copyright 2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""
Module defining NodeMgmt class and related methods.

NodeMgmt class responsibility is to support operations on a set of
qserv nodes, for example creating/deleting databases or tables.

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging
import types

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.db.exception import produceExceptionClass
from .nodeAdmin import NodeAdmin

#----------------------------------
# Local non-exported definitions --
#----------------------------------

_Exception = produceExceptionClass('WorkerMgmtException', [
    (100, "TABLE_SCHEMA_ERR", "Invalid table schema specification"),
    ])

#------------------------
# Exported definitions --
#------------------------

#---------------------
#  Class definition --
#---------------------

class NodeMgmt(object):
    """
    NodeMgmt class main purpose is to facilitate management operations on
    a set of nodes. It's two main responsibities are:
      1. selecting a corresponding set of nodes
      2. doing some operation on the selected set of nodes

    For most operations responsibility is delegated to NodeAdmin/WmgrClient
    classes, e.g. one can do:

        mgr = NodeMgmt(...)
        for node in mgr.select(nodeType='worker', state='active'):
            node.wmgrClient().dropTable(tableName)
    """

    #----------------
    #  Constructor --
    #----------------
    def __init__(self, qservAdmin, wmgrSecretFile=None):
        """
        Constructor needs an instance of QservAdmin type which provides access to
        CSS worker node information.

        @param qservAdmin:  QservAdmin instance.
        @param wmgrSecretFile:  Path to a file with wmgr secret, this argument is
                    only used when actual communication with remote nodes happen, but
                    it's not required if you only use selectDict()
        """

        self.css = qservAdmin
        self.wmgrSecretFile = wmgrSecretFile
        self._log = logging.getLogger(__name__)

    #-------------------
    #  Public methods --
    #-------------------

    def select(self, state=None, nodeType=None):
        """
        Returns set of NodeAdmin instances based on supplied selection criteria.

        @param state:   string or list of strings from a list defined in  QservAdmin.NodeState.STATES
                        (e.g. QservAdmin.NodeState.ACTIVE). If provided then only nodes with
                        the state that match any item in the list (or the state if state is a string)
                        are returned.
        @param nodeType: string or list of strings, if provided then only nodes that have the specified
                        node type are returned

        @return: Sequence (list) of NodeAdmin objects.
        """

        nodes = self.selectDict(state, nodeType)

        # convert to instances
        return [NodeAdmin(name=key, qservAdmin=self.css, wmgrSecretFile=self.wmgrSecretFile)
                for key, _ in nodes.items()]


    def selectDict(self, state=None, nodeType=None):
        """
        Returns set of nodes based on supplied selection criteria. Nodes are returned
        as a dictionary with node name as key and node data as values. See select()
        for parameter description.
        """

        # if state is a string make a list out of it
        if isinstance(state, types.StringTypes):
            state = [state]

        # if nodeType is a string make a list out of it
        if isinstance(nodeType, types.StringTypes):
            nodeType = [nodeType]

        # get all nodes as a sequence of (node_name, node_data)
        nodes = self.css.getNodes().items()

        # filter out those that don't match
        if state is not None:
            nodes = [item for item in nodes if item[1].get('state') in state]
        if nodeType is not None:
            nodes = [item for item in nodes if item[1].get('type') in nodeType]

        # make dict
        return dict(nodes)


    def createDb(self, dbName, grantUser=None, state=None, nodeType=None, **kwargs):
        """
        Create database on a set of nodes.

        Method takes arguments that select the list of nodes (same arguments
        as defined for select() method) and additional keyword arguments that
        are passed to NodeAdmin.mysqlConn() method. Note that in the future
        we will likely replaces those positional arguments with something else.

        If database already exists on some nodes it will not be re-created.

        @param dbName:    database name to be created
        @param grantUser: optional user name, if specified then this user is granted
                          "ALL" privileges on created database
        @param state:     same as in select() method
        @param nodeType:  same as in select() method
        @param kwargs:    optional keyword arguments passed to NodeAdmin.mysqlConn()
                          method
        @return: tuple of two integers, first integer is the number of workers selected,
                 second is the number of workers where database did not exist and was created
        @raise DbException: when database operations fail (e.g. failed to connect to database)
        """

        # get a bunch of nodes to work with
        nodes = self.select(state, nodeType)

        # make database on each of them if does not exist
        nCreated = 0
        for worker in nodes:
            db = worker.mysqlConn(**kwargs)
            if not db.dbExists(dbName):

                self._log.debug('Creating database %s on node %s', dbName, worker.name())

                # race condition here obviously, so we set mayExist=True to suppress
                # exception in case someone else managed to create database at this instant
                db.createDb(dbName, mayExist=True)
                nCreated += 1

                if grantUser:
                    self._log.debug('Granting permissions to user %s', grantUser)
                    grant = "GRANT ALL ON {0}.* to '{1}'@'{2}'"
                    # TODO: grant ALL to user@localhost, we may also want to extend this
                    # with user@127.0.0.1, or user@%, need to look at his again when I
                    # have better understanding of deployment model
                    hostmatch = ['localhost']
                    for host in hostmatch:
                        query = grant.format(dbName, grantUser, host)
                        self._log.debug('query: %s', query)
                        db.execCommand0(grant.format(dbName, grantUser, host))

            else:
                self._log.debug('Database %s already exists on node %s', dbName, worker.name())

        self._log.debug('Created databases on %d nodes out of %d', nCreated, len(nodes))
        return len(nodes), nCreated


    def createTable(self, dbName, tableName, state=None, nodeType=None, **kwargs):
        """
        Create table on a set of nodes. Table schema must already be defined
        in CSS.

        Method takes arguments that select the list of nodes (same arguments
        as defined for select() method) and additional keyword arguments that
        are passed to NodeAdmin.mysqlConn() method. Note that in the future
        we will likely replaces those positional arguments with something else.

        If table already exists on some nodes it will not be re-created.

        @param dbName:    database name for new table
        @param tableName: table name to be created
        @param state:     same as in select() method
        @param nodeType:  same as in select() method
        @param kwargs:    optional keyword arguments passed to NodeAdmin.mysqlConn()
                          method
        @return: tuple of two integers, first integer is the number of workers selected,
                 second is the number of workers where table did not exist and was created
        @raise DbException: when database operations fail (e.g. failed to connect to database)
        @raise Exception: when table schema is invalid.
        """

        # get schema from CSS
        tableSchema = self.css.getTableSchema(dbName, tableName)

        # some pre-validation, check that schema does not include "CREATE ..."
        # and only starts with opening parenthesis
        schemaWords = tableSchema.lower().split()
        if not schemaWords:
            raise _Exception(_Exception.TABLE_SCHEMA_ERR, "schema is empty")
        if schemaWords[0] == 'create':
            raise _Exception(_Exception.TABLE_SCHEMA_ERR, "CREATE TABLE present in schema")
        if schemaWords[0][0] != '(':
            raise _Exception(_Exception.TABLE_SCHEMA_ERR, "schema does not start with parenthesis")

        # get a bunch of nodes to work with
        nodes = self.select(state, nodeType)

        # make database on each of them if does not exist
        nCreated = 0
        for worker in nodes:
            dbi = worker.mysqlConn(**kwargs)
            if not dbi.tableExists(tableName, dbName):

                self._log.debug('Creating table %s.%s on node %s', dbName, tableName, worker.name())

                # race condition here obviously, so we set mayExist=True to suppress
                # exception in case someone else managed to create table at this instant
                dbi.createTable(tableName, tableSchema, dbName, mayExist=True)
                nCreated += 1

            else:
                self._log.debug('Table %s.%s already exists on node %s', dbName, tableName, worker.name())

        self._log.debug('Created tables on %d nodes out of %d', nCreated, len(nodes))
        return len(nodes), nCreated
