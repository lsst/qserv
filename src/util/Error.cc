// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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

// Class header
#include "util/Error.h"

// System headers
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {  // File-scope helpers

LOG_LOGGER _log = LOG_GET("lsst.qserv.util.Error");

}  // namespace

namespace lsst::qserv::util {

Error::Error(int code, int subCode, string const& msg, bool logLvlErr)
        : _code(code), _subCode(subCode), _msg(msg) {
    if (_code != NONE || _msg != "") {
        // Flushing output as it is likely that this exception will not be caught.
        int logLvl = (logLvlErr) ? LOG_LVL_ERROR : LOG_LVL_TRACE;
        LOGS(_log, logLvl, "Error " << *this << std::endl);
    }
}

Error::Error(int code, int subCode, set<int> const& chunkIds, set<int> const& jobIds, string const& msg,
             bool logLvlErr)
        : _code(code), _subCode(subCode), _msg(msg) {
    _chunkIds.insert(chunkIds.begin(), chunkIds.end());
    _jobIds.insert(jobIds.begin(), jobIds.end());
    if (_code != NONE || _msg != "") {
        // Flushing output as it is likely that this exception will not be caught.
        int logLvl = (logLvlErr) ? LOG_LVL_ERROR : LOG_LVL_TRACE;
        LOGS(_log, logLvl, "Error " << *this << std::endl);
    }
}

vector<int> Error::getChunkIdsVect() const {
    vector<int> res(_chunkIds.begin(), _chunkIds.end());
    return res;
}

vector<int> Error::getJobIdsVect() const {
    vector<int> res(_jobIds.begin(), _jobIds.end());
    return res;
}

string Error::dump() const {
    stringstream os;
    dump(os);
    return os.str();
}

ostream& Error::dump(ostream& os, bool showJobs) const {
    os << "[count=" << _count << "][code=" << _code << "] " << _msg;
    if (_subCode != 0) {
        os << "[subCode=" << _subCode << "]";
    }
    if (_chunkIds.size() > 0) {
        unsigned int const maxPrint = 10;  // There could be tens of thousands of these
        auto iter = _chunkIds.begin();

        if (_chunkIds.size() > maxPrint) {
            os << "[chunkIds(first 10 of " << _chunkIds.size() << ")=" << *iter++;
        } else {
            os << "[chunkIds=" << *iter++;
        }
        for (unsigned int j = 1; j < _chunkIds.size() && j < maxPrint; ++j) {
            os << ", " << *iter++;
        }
        os << "]";
    }
    if (showJobs && _jobIds.size() > 0) {
        unsigned int const maxPrint = 10;  // There could be tens of thousands of these
        auto iter = _jobIds.begin();

        if (_jobIds.size() > maxPrint) {
            os << "[jobIds(first 10 of " << _jobIds.size() << ")=" << *iter++;
        } else {
            os << "[jobIds=" << *iter++;
        }
        for (unsigned int j = 1; j < _jobIds.size() && j < maxPrint; ++j) {
            os << ", " << *iter++;
        }
        os << "]";
    }
    return os;
}

ostream& operator<<(ostream& out, Error const& error) {
    error.dump(out);
    return out;
}

}  // namespace lsst::qserv::util
