/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
#ifndef LSST_QSERV_QMETA_QPROGRESSDATA_H
#define LSST_QSERV_QMETA_QPROGRESSDATA_H

// System headers
#include <ctime>

// Qserv headers
#include "global/intTypes.h"

namespace lsst::qserv::qmeta {

/**
 * Class QProgressData stores the transient state of the query progress counters
 * retrieved from the Qserv's metadata database.
 */
class QProgressData {
public:
    QProgressData() = default;
    QProgressData(QProgressData const&) = default;
    QProgressData(QueryId const& queryId_, int totalChunks_, int completedChunks_, std::time_t begin_,
                  std::time_t lastUpdate_)
            : queryId(queryId_),
              totalChunks(totalChunks_),
              completedChunks(completedChunks_),
              begin(begin_),
              lastUpdate(lastUpdate_) {}

    QueryId queryId{0};         ///< Query Id
    int totalChunks{0};         ///< Total number of chunks to be searched by the query.
    int completedChunks{0};     ///< Number of chunks that have been searched.
    std::time_t begin{0};       ///< Time the query was started.
    std::time_t lastUpdate{0};  ///< Last time this row was updated.
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_QPROGRESSDATA_H
