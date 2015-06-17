// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2015 AURA/LSST.
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

// Class header
#include "ccontrol/queryMsg.h"

// System headers
#include <iostream>

// Qserv headers
#include "ccontrol/userQueryProxy.h"
#include "ccontrol/UserQuery.h"
#include "qdisp/MessageStore.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

int queryMsgGetCount(int session) {
    return UserQuery_get(session).getMessageStore()->messageCount();
}

// Python call: msg, chunkId, code, timestamp = queryMsgGetMsg(session, idx)
std::string queryMsgGetMsg(int session, int idx, int* chunkId, int* code, std::string* severity, time_t* timestamp) {

    qdisp::QueryMessage msg = UserQuery_get(session).getMessageStore()->getMessage(idx);
    *chunkId = msg.chunkId;
    *code = msg.code;
    *timestamp = msg.timestamp;
    *severity = qdisp::to_string(msg.severity);
    return msg.description;
}

void queryMsgAddMsg(int session, int chunkId, int code, std::string const& message,
					std::string const& severity /* = "INFO" */) {

    UserQuery_get(session).getMessageStore()->addMessage(chunkId,
                                                         code,
                                                         message,
                                                         qdisp::to_severity(severity));
}

}}} // namespace lsst::qserv::ccontrol
