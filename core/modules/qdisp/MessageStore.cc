// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2016 LSST Corporation.
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
// See MessageStore.h

// Class header
#include "qdisp/MessageStore.h"

// System headers
#include <iostream>

// Third-party headers
#include "boost/format.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/constants.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.MessageStore");
}

namespace lsst {
namespace qserv {
namespace qdisp {

////////////////////////////////////////////////////////////////////////
// public
////////////////////////////////////////////////////////////////////////

void MessageStore::addMessage(int chunkId, int code,
                              std::string const& description,
                              MessageSeverity severity,
                              std::time_t timestamp) {
    if (timestamp == std::time_t(0)) {
        timestamp = std::time(nullptr);
    }
    auto level = code < 0 ? LOG_LVL_ERROR : LOG_LVL_DEBUG;
    LOGS(_log, level, "Add msg: " << chunkId << " " << code << " " << description);
    {
        std::lock_guard<std::mutex> lock(_storeMutex);
        _queryMessages.insert(_queryMessages.end(),
                              QueryMessage(chunkId, code, description, timestamp, severity));
    }
}

void MessageStore::addErrorMessage(std::string const& description) {
    addMessage(NOTSET, NOTSET, description, MessageSeverity::MSG_ERROR);
}

const QueryMessage MessageStore::getMessage(int idx) {
    return _queryMessages.at(idx);
}

const int MessageStore::messageCount() {
    return _queryMessages.size();
}

const int MessageStore::messageCount(int code) {
    int count = 0;
    for (std::vector<QueryMessage>::const_iterator i = _queryMessages.begin();
         i != _queryMessages.end(); ++i) {
        if (i->code == code) count++;
    }
    return count;
}

}}} // namespace lsst::qserv::qdisp
