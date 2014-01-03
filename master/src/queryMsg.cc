/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
// queryMsg.cc houses the implementation of
// queryMsg.h (SWIG-exported functions for accessing QueryMessages)

#include <iostream>
#include "lsst/qserv/master/SessionManagerAsync.h"
#include "lsst/qserv/master/AsyncQueryManager.h"
#include "lsst/qserv/master/MessageStore.h"
#include "lsst/qserv/master/queryMsg.h"

namespace qMaster=lsst::qserv::master;

int qMaster::queryMsgGetCount(int session) {
    qMaster::AsyncQueryManager& qm = qMaster::getAsyncManager(session);
    boost::shared_ptr<MessageStore> ms = qm.getMessageStore();
    return ms->messageCount();
}

// Python call: msg, chunkId, code, timestamp = queryMsgGetMsg(session, idx)
std::string qMaster::queryMsgGetMsg(int session, int idx, int* chunkId, int* code, time_t* timestamp) {
    qMaster::AsyncQueryManager& qm = qMaster::getAsyncManager(session);
    boost::shared_ptr<MessageStore> ms = qm.getMessageStore();
    QueryMessage msg = ms->getMessage(idx);
    *chunkId = msg.chunkId;
    *code = msg.code;
    *timestamp = msg.timestamp;
    return msg.description;
}

void qMaster::queryMsgAddMsg(int session, int chunkId, int code,
                            std::string const& message) {
    qMaster::AsyncQueryManager& qm = qMaster::getAsyncManager(session);
    boost::shared_ptr<MessageStore> ms = qm.getMessageStore();
    ms->addMessage(chunkId, code, message);
}
