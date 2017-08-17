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

"""
Module defining NodeAdmin class and related methods.

NodeAdmin is a primary interface for qserv node administration. Currently
NodeAdmin is a factory for WmgrClient instances which hides complexity
of configuring WmgrClient from various sources (CSS or anything else).
There may be additional methods added to this class in the future.

NodeAdmin uses information about worker nodes defined in Qserv CSS, for
details see https://dev.lsstcorp.org/trac/wiki/db/Qserv/CSS#Node-related.
For testing purposes it it also possible to provide worker information as
a set of parameters to constructor.

@author  Andy Salnikov, SLAC
"""

from __future__ import absolute_import, division, print_function

# --------------------------------
#  Imports of standard modules --
# --------------------------------
import logging

# -----------------------------
# Imports for other modules --
# -----------------------------
from lsst.db.exception import produceExceptionClass
from lsst.qserv.wmgr.client import WmgrClient

# ----------------------------------
# Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)

_Exception = produceExceptionClass('WorkerAdminException', [
    (100, "ARG_ERR", "Missing or inconsistent arguments"),
    (110, "CSS_HOST_NAME_MISS", "Missing host name is in CSS"),
])

# ------------------------
# Exported definitions --
# ------------------------


class NodeAdmin(object):
    """
    Class representing administration/communication endpoint for qserv worker.
    """

    def __init__(self, name=None, css=None, host=None, port=None, wmgrSecretFile=None):
        """
        Make new endpoint for remote worker. Worker can be specified
        either by its name or by the complete set of parameters. If name
        is given then host and port other parameters are taken from CSS
        and css has to be specified as well. If name is not given
        then host and port parameters (except css) need to be specified.
        wmgrSecretFile needs to be provided if connections to wmgr service
        is protected (which means almost always).

        This will throw if node with given name is not defined in CSS.

        @param name:        Name of the worker as defined in CSS.
        @param css:         CssAccess instance, required if name is provided.
        @param host:        Node host name, required if name is not provided.
        @param port:        Port number for wmgr service running on node, required
                            if name is not provided.
        @param wmgrSecretFile:  path to a file with wmgr secret
        """

        if name:

            # need CSS
            if not css:
                raise ValueError('css has to be specified if name is used')

            params = css.getNodeParams(name)
            self.host = params.host
            if not self.host:
                raise _Exception(_Exception.CSS_HOST_NAME_MISS, "node=" + name)
            self.port = params.port
            self._name = name

        else:

            if not (host and port):
                raise _Exception(_Exception.ARG_ERR, 'either name or host/port has to be provided')

            self.host = host
            self.port = port
            self._name = host + ':' + str(port)

        self.wmgrSecretFile = wmgrSecretFile
        self.client = None

    def name(self):
        """
        Returns worker name as defined in CSS, if instance was not made from
        CSS returns some arbitrary unique name.
        """
        return self._name

    def wmgrClient(self):
        """
        Returns an instance ow wmgr.client.WmgrClient class for the node.
        """
        if self.client is None:
            self.client = WmgrClient(self.host, self.port, secretFile=self.wmgrSecretFile)
        return self.client
