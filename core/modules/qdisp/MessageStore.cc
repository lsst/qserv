// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2014 LSST Corporation.
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

#include "qdisp/MessageStore.h"

// System headers
#include <iostream>

// Third-party headers
#include "boost/format.hpp"

// LSST headers
#include "lsst/log/Log.h"


namespace lsst {
namespace qserv {
namespace qdisp {

////////////////////////////////////////////////////////////////////////
// public
////////////////////////////////////////////////////////////////////////

void MessageStore::addMessage(int chunkId, int code, std::string const& description) {
    if (code < 0) {
        LOGF_ERROR("Msg: %1% %2% %3%" % chunkId % code % description);
    } else {
        LOGF_DEBUG("Msg: %1% %2% %3%" % chunkId % code % description);
    }
    {
        std::lock_guard<std::mutex> lock(_storeMutex);
        _queryMessages.insert(_queryMessages.end(),
            QueryMessage(chunkId, code, description, std::time(0)));
    }
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
