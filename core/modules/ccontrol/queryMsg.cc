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
#include "global/constants.h"
#include "qdisp/MessageStore.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

/** Return the number of messages in the message store
 */
int queryMsgGetCount(int session) {
    return UserQuery_get(session).getMessageStore()->messageCount();
}

/** Get a message from the message store
 */
std::string queryMsgGetMsg(int session, int idx, int* chunkId, int* code, MessageSeverity* severity,
                           time_t* timestamp) {

    qdisp::QueryMessage msg = UserQuery_get(session).getMessageStore()->getMessage(idx);
    *chunkId = msg.chunkId;
    *code = msg.code;
    *timestamp = msg.timestamp;
    *severity = msg.severity;
    return msg.description;
}

/** Add a message to the message store
 */
void queryMsgAddMsg(int session, int chunkId, int code, std::string const& message,
                    MessageSeverity const& severity) {

    UserQuery_get(session).getMessageStore()->addMessage(chunkId,
                                                         code,
                                                         message,
                                                         severity);
}

}}} // namespace lsst::qserv::ccontrol
