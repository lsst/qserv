/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_QMETA_TYPES_H
#define LSST_QSERV_QMETA_TYPES_H

// System headers
#include <cstdint>
#include <string>

// Third-party headers

// Qserv headers


namespace lsst {
namespace qserv {
namespace qmeta {

/*
 * Several typedefs for commonly used types.
 */

/// Typedef for Czar ID in query metadata.
typedef std::uint32_t CzarId;

/// Typedef for Query ID in query metadata.
typedef std::uint64_t QueryId;

/// Class to provide a consistent format for QueryIds in the log file
class QueryIdHelper {
public:

    static std::string makeIdStr(qmeta::QueryId qid, bool unknown=false) {
        if (unknown) return "QI=?:";
        return "QI=" + std::to_string(qid) + ":";
    }

    static std::string makeIdStr(qmeta::QueryId qid, int jobId, bool unknown=false) {
        if (unknown) return makeIdStr(qid, true) + "?;";
        return makeIdStr(qid) + std::to_string(jobId) + ";";
    }
};

}}} // namespace lsst::qserv::qmeta

#endif // LSST_QSERV_QMETA_TYPES_H
