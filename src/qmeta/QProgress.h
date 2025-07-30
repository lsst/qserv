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
#ifndef LSST_QSERV_QMETA_QPROGRESS_H
#define LSST_QSERV_QMETA_QPROGRESS_H

// System headers
#include <memory>
#include <mutex>

// Qserv headers
#include "global/intTypes.h"
#include "mysql/MySqlConfig.h"

// Forward declarations
namespace lsst::qserv::qmeta {
class QProgressData;
}  // namespace lsst::qserv::qmeta

namespace lsst::qserv::sql {
class SqlConnection;
}  // namespace lsst::qserv::sql

// This header declarations
namespace lsst::qserv::qmeta {

/**
 * Class QProgress is manages the query progress information in the metadata database.
 */
class QProgress {
public:
    typedef std::shared_ptr<QProgress> Ptr;

    QProgress(mysql::MySqlConfig const& mysqlConf);
    QProgress() = delete;
    QProgress(QProgress const&) = delete;
    QProgress& operator=(QProgress const&) = delete;

    ~QProgress() = default;

    /**
     * Add a new query to the progress table.
     * @param queryId The query id.
     * @param totalChunks The total number of chunks to be processed by the query.
     * @return true if successful.
     * @throw SqlError
     */
    void insert(QueryId queryId, int totalChunks) const;

    /**
     * Update the number of completed chunks.
     * @param queryId The query id.
     * @param completedChunks The number of chunks that have been processed by the query.
     * @return true if successful.
     * @throw SqlError
     */
    void update(QueryId queryId, int completedChunks) const;

    /**
     * Get query progress status.
     * @param queryId The query id.
     * @return QProgressData object containing query progress counters
     * @throw QueryIdError, SqlError
     */
    QProgressData get(QueryId queryId) const;

    /**
     * Remove the query from the table.
     * @note This is a cleanup operation that should be called after the query is completed.
     * @return true if successful.
     * @throw SqlError
     */
    void remove(QueryId queryId) const;

private:
    std::shared_ptr<sql::SqlConnection> const _conn;
    mutable std::mutex _dbMutex;  ///< Synchronizes access to certain DB operations
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_QPROGRESS_H
