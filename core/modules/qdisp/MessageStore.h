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
#include <string>

// Third-party headers
#include <mutex> // for mutex.

namespace lsst {
namespace qserv {
namespace qdisp {

struct QueryMessage {
    QueryMessage(int chunkId_,
                 int code_,
                 std::string description_,
                 std::time_t timestamp_)
        :  chunkId(chunkId_),
           code(code_),
           description(description_),
           timestamp(timestamp_) {
    }

    int chunkId;
    int code;
    std::string description;
    std::time_t timestamp;
};

class MessageStore {
public:
    void addMessage(int chunkId, int code, std::string const& description);
    const QueryMessage getMessage(int idx);
    const int messageCount();
    const int messageCount(int code);

private:
    std::mutex _storeMutex;
    std::vector<QueryMessage> _queryMessages;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_MESSAGESTORE_H
