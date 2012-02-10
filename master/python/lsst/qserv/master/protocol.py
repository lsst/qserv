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

# protocol module for lsst.qserv.master
# Manages wire-protocol-related operations for qserv.
import worker_pb2

class TaskMsgFactory:
    def __init__(self, session, db):
        msg = worker_pb2.TaskMsg()
        msg.session = session
        msg.db = db
        self.msg = msg

    def newChunk(self, resulttable, chunkid):
        msg = self.msg
        msg.chunkid = chunkid
        self.resulttable = resulttable
        del msg.fragment[:] # clear out fragments
        
    def fillFragment(self, query, subchunks):
        frag = self.msg.fragment.add()
        frag.query = query
        frag.resulttable = self.resulttable
        if subchunks:
            frag.subchunk.extend(subchunks)
        pass

    def getBytes(self):
        s = self.msg.SerializeToString()
        return s

