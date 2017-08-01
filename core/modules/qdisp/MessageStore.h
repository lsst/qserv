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

/// MessageStore.h declares:
///
/// struct QueryMessage
/// class MessageStore

/// The MessageStore classes are responsible for maintaining status and
/// error messages associated with a query.

#ifndef LSST_QSERV_QDISP_MESSAGESTORE_H
#define LSST_QSERV_QDISP_MESSAGESTORE_H

// System headers
#include <ctime>
#include <mutex>
#include <string>
#include <vector>

// Qserv headers
#include "global/constants.h"


namespace lsst {
namespace qserv {
namespace qdisp {

struct QueryMessage {

    QueryMessage(int chunkId_,
                 int code_,
                 std::string description_,
                 std::time_t timestamp_,
                 MessageSeverity severity_
                 )
        :  chunkId(chunkId_),
           code(code_),
           description(description_),
           timestamp(timestamp_),
           severity(severity_) {
    }

    int chunkId;
    int code;
    std::string description;
    std::time_t timestamp;
    MessageSeverity severity;
};

/** Store messages issued by Qserv workers and czar
 *
 * For each SQL query, these messages are stored in a MySQL message table
 * so that mysql-proxy can retrieve it and log it or send error messages
 * to client.
 */
class MessageStore {
public:

    /** Add a message to this MessageStore
     *
     * This message will be sent to proxy via message table, in order to be
     * displayed in mysql-proxy logs.
     *
     * @param chunkId chunkId related to the message, -1 if not available
     * @param code code of the message
     * @param description text of the message
     * @param severity_ message severity level, default to MSG_INFO
     */
    void addMessage(int chunkId, int code, std::string const& description,
                    MessageSeverity severity_ = MessageSeverity::MSG_INFO,
                    std::time_t timestamp = std::time_t(0));

    /** Add an error message to this MessageStore
     *
     * This message will be sent to mysql-proxy via message table, in order
     * display an error in mysql client console. chunkId and code are set
     * to NOTSET because this message may aggregate multiple error messages
     * in multiple files. Indeed, mysql-client can only display
     * one error message per query.
     *
     * @param description text of the message
     */
    void addErrorMessage(std::string const& description);
    const QueryMessage getMessage(int idx);
    const int messageCount();
    const int messageCount(int code);

private:
    std::mutex _storeMutex;
    std::vector<QueryMessage> _queryMessages;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_MESSAGESTORE_H
