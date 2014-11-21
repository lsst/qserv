#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014 LSST/AURA.
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
Top level Central State System (CSS) module

"""

# standard library imports
# import logging
# import sys
# import time

# third-party software imports
# from kazoo.client import KazooClient
# from kazoo.exceptions import NodeExistsError, NoNodeError

# local imports
import cssLib
from cssLib import *

import snapshot
import kvInterface


def getSnapshot(kvi):
    s = snapshot.Snapshot(kvi)
    return s.snapshot

getKvi = kvInterface.KvInterface.newImpl


