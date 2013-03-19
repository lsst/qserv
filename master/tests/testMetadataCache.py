#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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

# testAppInterface.py : A module with Python unittest code for testing
# functionality available through the appInterface module.  Currently
# only includes minimal fuzz testing and (unfinished) query replaying.


# Standard Python imports
import unittest
import sys
import tempfile
import time

# Package imports
import lsst.qserv.master
from lsst.qserv.master.app import MetadataCacheInterface
from lsst.qserv.master import config
from lsst.qserv.master.config import load
from lsst.qserv.meta.status import Status, QmsException

try:
    tf = tempfile.NamedTemporaryFile(delete=True)
    load(tf.name) # silly, but must provide config, or it will fail
    mcI = MetadataCacheInterface()
    sessionId = mcI.newSession()
    mcI.printSession(sessionId)
    mcI.discardSession(sessionId)
except QmsException as qe:
    print qe.getErrMsg()
