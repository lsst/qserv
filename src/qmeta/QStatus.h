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
#ifndef LSST_QSERV_QMETA_QSTATUS_H
#define LSST_QSERV_QMETA_QSTATUS_H

// System headers
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

// Qserv headers
#include "qmeta/QStats.h"
#include "qmeta/types.h"

namespace lsst::qserv::qmeta {

/// This class is used to track temporary status information.
/// Currently, this only consists of how many chunks have been completed
/// out of how many total chunks there are in the query.
class QStatus {
public:
    typedef std::shared_ptr<QStatus> Ptr;

    QStatus(QStatus const&) = delete;
    QStatus& operator=(QStatus const&) = delete;

    virtual ~QStatus() = default;

    /// Insert a row for tracking chunksCompleted vs totalChunks of a query.
    /// @return true if successful.
    /// @throw SqlError
    virtual void queryStatsTmpRegister(QueryId queryId, int totalChunks) = 0;

    /// Update the number of completed chunks
    /// @return true if successful.
    /// @throw SqlError
    virtual void queryStatsTmpChunkUpdate(QueryId queryId, int completedChunks) = 0;

    /// Get statistics for queryId
    /// @return QStats object containing query completion information.
    /// @throw QueryIdError, SqlError
    virtual QStats queryStatsTmpGet(QueryId queryId) = 0;

    /// Remove row for completion status when the query is done.
    /// @return true if successful.
    /// @throw SqlError
    virtual void queryStatsTmpRemove(QueryId queryId) = 0;

protected:
    QStatus() = default;
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_QSTATUS_H
