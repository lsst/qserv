// -*- LSST-C++ -*-
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
// Generic timer class

#include "wbase/UserQueryWInfo.h"

// System headers

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wbase.UserQueryInfo");
}

namespace lsst::qserv::wbase {

// Static members of UserQueryWInfo
UserQueryWInfo::UserQueryWInfoMap UserQueryWInfo::_uqwiMap;
queue<UserQueryWInfo::Ptr> UserQueryWInfo::_startTimes;
mutex UserQueryWInfo::_mtx;

UserQueryWInfo::Ptr UserQueryWInfo::getUQWI(QueryId queryId_) {
    lock_guard<mutex> lkg(_mtx);
    auto iter = _uqwiMap.find(queryId);
    if (iter != _uqwiMap.end()) {
        return iter->second;
    }
    Ptr newElem = make_shared<UserQueryWInfo>(queryId);
    _uqwiMap.insert(make_pair(queryId, newElem));
    _startTimes.push(newElem);
    return newElem;
}

UserQueryWInfo::Ptr UserQueryWInfo::erase(QueryId queryId_) {
    lock_guard<mutex> lkg(_mtx);
    return _erase(queryId_);
}

UserQueryWInfo::Ptr UserQueryWInfo::_erase(QueryId queryId_) {
    auto iter = _uqwiMap.find(queryId);
    if (iter == _uqwiMap.end()) {
        return nullptr;
    }
    Ptr oldElem = iter->second;
    _uqwiMap.erase(iter);
    return oldElem;
}

int UserQueryWInfo::removeOld() {
    vector<QueryId> toBeRemoved;
    lock_guard<mutex> lkg(_mtx);
    auto now = CLOCK::now();
    auto maxAge = std::chrono::milliseconds(25h);

    while (!_startTimes.empty()) {
        auto item = _startTimes.front();
        // If the item at the front is too old and the only things using it are
        // item, _startTimes, and _uqwiMap, then delete it.
        // Also delete it if it is really old.
        if (((item->creationTime + maxAge) > now && item.use_count() <= 3) ||
            (item->creationTime + 2 * maxAge > now)) {
            toBeRemoved.emplace_back(item->queryId);
            _startTimes.pop();
        } else {
            break;
        }
    }

    for (auto const& qid : toBeRemoved) {
        _erase(qid);
    }
}

UserQueryWInfo::UserQueryWInfo(QueryId qId_) : queryId(qId_), creationTime(CLOCK::now()) {
    /// For all of the histograms, all entries should be kept at least until the work is finished.
    string qidStr = to_string(queryId);
    std::chrono::milliseconds maxAge(100h);  // essentially forever
    _histSizePerChunk = util::Histogram::Ptr(new util::Histogram(
            string("SizePerChunk_") + qidStr, {1'000, 10'0000, 1'000'000, 10'000'000, 100'000'000}, maxAge,
            200'000));  ///< &&&
    _histRowsPerChunk = util::Histogram::Ptr(new util::Histogram(string("RowsPerChunk_") + qidStr,
                                                                 {1, 100, 1'000, 10'000, 100'000, 1'000'000},
                                                                 maxAge, 200'000));  ///< &&&
    _histTimeRunningPerChunk = util::Histogram::Ptr(
            new util::Histogram(string("TimeRunningPerChunk_") + qidStr,
                                {0.1, 1, 10, 30, 60, 120, 300, 600, 1200, 10000}, maxAge, 200'000));  ///< &&&
    _histTimeTransmittingPerChunk = util::Histogram::Ptr(
            new util::Histogram(string("TimeTransmittingPerChunk_") + qidStr,
                                {0.1, 1, 10, 30, 60, 120, 300, 600, 1200, 10000}, maxAge, 200'000));
}

}  // namespace lsst::qserv::wbase
