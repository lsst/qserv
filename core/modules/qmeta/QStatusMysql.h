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
#ifndef LSST_QSERV_QMETA_QSTATUSMYSQL_H
#define LSST_QSERV_QMETA_QSTATUSMYSQL_H

// System headers
#include <mutex>

// Third-party headers

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "qmeta/QStatus.h"
#include "sql/SqlConnection.h"

namespace lsst {
namespace qserv {
namespace qmeta {

class QStatusMysql : public QStatus {
public:
    typedef std::shared_ptr<QStatusMysql> Ptr;

    QStatusMysql(mysql::MySqlConfig const& mysqlConf);

    QStatusMysql(QStatusMysql const&) = delete;
    QStatusMysql& operator=(QStatusMysql const&) = delete;

    virtual ~QStatusMysql() = default;

    /// @see QStatus::createQueryStatsTable()
    void createQueryStatsTmpTable() override;

    /// @see QStatus::queryStatsTmpRegister(QueryId queryId, int totalChunks)
    void queryStatsTmpRegister(QueryId queryId, int totalChunks) override;

    /// @see QStatus::queryStatsTmpChunkUpdate(QueryId queryId, int completedChunks)
    void queryStatsTmpChunkUpdate(QueryId queryId, int completedChunks) override;

    /// @see QStatus::queryStatsTmpGet(QueryId queryId)
    QStats queryStatsTmpGet(QueryId queryId) override;

    /// @see QStatus::queryStatsTmpRemove(QueryId queryId)
    void queryStatsTmpRemove(QueryId queryId) override;

private:
    sql::SqlConnection _conn;
    std::mutex _dbMutex;    ///< Synchronizes access to certain DB operations

};

}}} // namespace lsst::qserv::qmeta

#endif // LSST_QSERV_QMETA_QSTATUSMYSQL_H
