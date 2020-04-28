// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
#ifndef LSST_QSERV_INTTYPES_H
#define LSST_QSERV_INTTYPES_H
 /**
  * @brief  Global int types
  *
  */
#include <stdint.h>
#include <set>
#include <vector>

namespace lsst {
namespace qserv {
typedef std::set<int> IntSet;
typedef std::vector<int> IntVector;
typedef std::vector<int32_t> Int32Vector;

/// Typedef for Query ID in query metadata.
typedef std::uint64_t QueryId;

/// Class to provide a consistent format for QueryIds in the log file
class QueryIdHelper {
public:
    /// Returns a standardized user query id string.
    /// @parameter qid - query id number.
    /// @parameter invalid - true, qid is not a valid user query id.
    static std::string makeIdStr(QueryId qid, bool invalid=false) {
        if (invalid) return "QI=?:";
        return "QI=" + std::to_string(qid) + ":";
    }

    /// Returns a standardized user query id string with jobId.
    /// @parameter qid - query id number.
    /// @parameter jobId - the job id number.
    /// @parameter invalid - true, qid is not a valid user query id.
    static std::string makeIdStr(QueryId qid, int jobId, bool invalid=false) {
        if (invalid) return makeIdStr(qid, true) + "?;";
        return makeIdStr(qid) + std::to_string(jobId) + ";";
    }
};


}}
#endif // LSST_QSERV_INTTYPES_H
