// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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
#ifndef LSST_QSERV_WBASE_USERQUERYWINFO_H
#define LSST_QSERV_WBASE_USERQUERYWINFO_H

// System headers
#include <chrono>
#include <map>
#include <memory>

#include "../util/Histogram.h"
#include "global/intTypes.h"

namespace lsst::qserv::wbase {
#if 0   // &&&
/// This class is used to store information about the part of a
/// user query running on this particular worker.
class UserQueryWInfo {
public:
    using Ptr = std::shared_ptr<UserQueryWInfo>;
    using UserQueryWInfoMap = std::map<QueryId, Ptr>;

    using CLOCK = std::chrono::system_clock;
    using TIMEPOINT = std::chrono::time_point<CLOCK>;

    UserQueryWInfo() = delete;
    UserQueryWInfo(UserQueryWInfo const&) = delete;
    UserQueryWInfo(UserQueryWInfo&&) = delete;
    UserQueryWInfo& operator=(UserQueryWInfo const&) = delete;

    ~UserQueryWInfo() = default;

    /// Return a UserQueryWInfo instance for the given `queryId`, creating a new one if needed.
    static Ptr getUQWI(QueryId queryId_);  //&&& replace with QueriesAndChunks::getStats()

    /// Return the UserQueryWInfo instance for the given `queryId` and remove it from the map.
    /// Returns nullptr if it wasn't found.
    static Ptr erase(QueryId queryId_);

    /// Remove entries that are too old and only have 1 reference count.
    static int removeOld();

    TIMEPOINT const creationTime;  ///< Time this instance was created.
    QueryId const queryId;

    util::Histogram::Ptr getHistTimeRunningPerChunk() const { return _histTimeRunningPerChunk; }  ///< &&&
    util::Histogram::Ptr getHistTimeTransmittingPerChunk() const {
        return _histTimeTransmittingPerChunk;
    }  ///< &&&
    util::Histogram::Ptr getHistSizePerChunk() const {
        return _histSizePerChunk;
    }  ///< Store information about bytes per chunk.
    util::Histogram::Ptr getHistRowsPerChunk() const {
        return _histRowsPerChunk;
    }  ///< Store information about rows per chunk.

private:
    /// _mtx must be locked, see erase(QueryId).
    static Ptr _erase(QueryId queryId_);

    /// Map of all UserQueryWInfo instances.
    static UserQueryWInfoMap _uqwiMap;

    /// Queue of all queries ordered by when this worker started them.
    static std::queue<Ptr> _startTimes;

    UserQueryWInfo(QueryId qId_);

    util::Histogram::Ptr _histTimeRunningPerChunk;       ///< &&&
    util::Histogram::Ptr _histTimeTransmittingPerChunk;  ///< &&&
    util::Histogram::Ptr _histSizePerChunk;              ///< Store information about bytes per chunk.
    util::Histogram::Ptr _histRowsPerChunk;              ///< Store information about rows per chunk.

    static std::mutex _mtx;  ///< Protects the map `_uqwiMap`.
};
#endif  //&&&
}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_USERQUERYWINFO_H
