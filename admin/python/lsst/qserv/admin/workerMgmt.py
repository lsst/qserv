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
Module defining WorkerMgmt class and related methods.

WorkerMgmt class responsibility is to support operations on a set of
worker nodes, for example creating/deleting databases or tables.

@version $Id$

@author Andy Salnikov - salnikov@slac.stanford.edu
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import types

#-----------------------------
# Imports for other modules --
#-----------------------------
from .workerAdmin import WorkerAdmin

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

#---------------------
#  Class definition --
#---------------------

class WorkerMgmt(object):
    """
    WorkerMgmt class main purpose is to facilitate management operations on
    a set of workers. It's two main responsibities are:
      1. selecting a corresponding set of worker nodes
      2. doing some operation on the selected set of nodes

    For some operations latter responsibility can possibly be delegated to some
    other entity, e.g. WorkerAdmin class.
    """

    #----------------
    #  Constructor --
    #----------------
    def __init__(self, qservAdmin):
        """
        Constructor needs an instance of QservAdmin type which provides access to
        CSS worker node information.

        @param qservAdmin:  QservAdmin instance.
        """

        self.css = qservAdmin

    #-------------------
    #  Public methods --
    #-------------------

    def select(self, state=None, nodeType=None):
        """
        Returns set of WorkerAdmin instances based on supplied selection criteria.

        @param state:   string or list of strings from a list defined in  QservAdmin.NodeState.STATES
                        (e.g. QservAdmin.NodeState.ACTIVE). If provided then only nodes with
                        the state that match any item in the list (or the state if state is a string)
                        are returned.
        @param nodeType: string or list of strings, if provided then only nodes that have the specified
                        node type are returned
        """

        nodes = self.selectDict(state, nodeType)

        # convert to instances
        return [WorkerAdmin(name=key, qservAdmin=self.css) for key, _ in nodes.items()]


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
        are passed to WorkerAdmin.mysqlConn() method. Note that in the future
        we will likely replaces those positional arguments with something else.

        If database already exists on some it will not be re-created.

        @param dbName:    database name to be created
        @param grantUser: optional user name, if specified then this user is granted
                          "ALL" privileges on created database
        @param state:     same as in select() method
        @param nodeType:  same as in select() method
        @param kwargs:    optional keyword arguments passed to WorkerAdmin.mysqlConn()
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
                # race condition here obviously, so we set mayExist=True to suppress
                # exception in case someone else managed to create database at this instant
                db.createDb(dbName, mayExist=True)
                nCreated += 1

                if grantUser:
                    grant = "GRANT ALL ON {0}.* to '{1}'@'{2}'"
                    # TODO: grant ALL to user@localhost, we may also want to extend this
                    # with user@127.0.0.1, or user@%, need to look at his again when I
                    # have better understanding of deployment model
                    hostmatch = ['localhost']
                    for host in hostmatch:
                        db.execCommand0(grant.format(dbName, grantUser, host))

        return len(nodes), nCreated
