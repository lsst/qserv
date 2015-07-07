// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 LSST Corporation.
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
/**
 * @file
 *
 * @ingroup ccontrol
 *
 * @brief Declare an interface for the query messaging mechanism for exporting
 * via SWIG to the python layer of Qserv.
 *
 * @author Daniel Wang, SLAC
 * @author Fabrice Jammes, IN2P3/SLAC
 */


#ifndef LSST_QSERV_CCONTROL_QUERYMSG_H
#define LSST_QSERV_CCONTROL_QUERYMSG_H

// System headers
#include <string>

// Qserv headers
#include "global/constants.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

using lsst::qserv::MessageSeverity;

/** Return the number of messages in the message store
 *
 * @param session session which owns the message store
 *
 * @return number of messages in the message store
 */
int queryMsgGetCount(int session);


/** Get a message from the message store
 *
 *  Messages are used for error reporting or logging
 *  Python call: msg, chunkId, code, timestamp = queryMsgGetMsg(session, idx)
 *  int* chunkId, int* code, time_t* timestamp matches with %apply directive to help SWIG
 *
 *  @param session which owns the message store
 *  @param idx position of the message in the message store vector
 *  @param chunkId returns the chunkId related to the message, -1 if not available
 *  @param code returns the code of the message
 *  @param message returns the text of the message
 *  @param severity returns the message severity level, default to MSG_INFO
 *  @param timestamp returns the timestamp of the message
 *  @return text of the message
 *
 */
std::string queryMsgGetMsg(int session, int idx, int* chunkId, int* code, MessageSeverity* severity,
                           time_t* timestamp);

/** Add a message to the message store
 *
 *  Messages are used for error reporting or logging
 *
 *  @param session which owns the message store
 *  @param chunkId chunkId related to the message, -1 if not available
 *  @param code code of the message
 *  @param message text of the message
 *  @param severity message severity level, default to MSG_INFO
 *
 */
void queryMsgAddMsg(int session, int chunkId, int code, std::string const& message,
                    MessageSeverity const& severity = MessageSeverity::MSG_INFO);

}}} // namespace lsst::qserv::ccontrol

#endif // LSST_QSERV_CCONTROL_QUERYMSG_H
