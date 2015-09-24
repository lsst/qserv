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
#ifndef LSST_QSERV_QMETA_QMETAMYSQL_H
#define LSST_QSERV_QMETA_QMETAMYSQL_H

// System headers
#include <mutex>

// Third-party headers

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "qmeta/QMeta.h"
#include "sql/SqlConnection.h"

namespace lsst {
namespace qserv {
namespace qmeta {

/// @addtogroup qmeta

/**
 *  @ingroup qmeta
 *
 *  @brief Mysql-based implementation of qserv metadata.
 */

class QMetaMysql : public QMeta {
public:

    /**
     *  @param mysqlConf: Configuration object for mysql connection
     */
    QMetaMysql(mysql::MySqlConfig const& mysqlConf);

    // Instances cannot be copied
    QMetaMysql(QMetaMysql const&) = delete;
    QMetaMysql& operator=(QMetaMysql const&) = delete;

    // Destructor
    virtual ~QMetaMysql();

    /**
     *  @brief Return czar ID given czar "name".
     *
     *  Negative number is returned if czar does not exist.
     *
     *  @param name:  Czar name, arbitrary string.
     *  @return: Czar ID, zero if czar does not exist.
     */
    virtual CzarId getCzarID(std::string const& name);

    /**
     *  @brief Register new czar, return czar ID.
     *
     *  If czar with the same name is already registered then its ID
     *  will be returned, otherwise new record will be created.
     *  In both cases the czar will be active after this call.
     *
     *  @param name:  Czar name, arbitrary string.
     *  @return: Czar ID, non-negative number.
     */
    virtual CzarId registerCzar(std::string const& name);

    /**
     *  @brief Mark specified czar as active or inactive.
     *
     *  This method will throw if czar ID is not known.
     *
     *  @param name:  Czar ID, non-negative number.
     *  @param active:  new value if active flag.
     */
    virtual void setCzarActive(CzarId czarId, bool active);

    /**
     *  @brief Register new query.
     *
     *  This method will throw if czar ID is not known.
     *
     *  @param qInfo:  Query info instance, time members (submitted/completed) and query status
     *                 are ignored.
     *  @param tables: Set of tables used by the query, may be empty if tables are not needed
     *                 (e.g. for interactive queries).
     *  @return: Query ID, non-negative number
     */
    virtual QueryId registerQuery(QInfo const& qInfo,
                                  TableNames const& tables);

    /**
     *  @brief Add list of chunks to query.
     *
     *  This method will throw if query ID is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @param chunks:    Set of chunk numbers.
     */
    virtual void addChunks(QueryId queryId, std::vector<int> const& chunks);

    /**
     *  @brief Assign or re-assign chunk to a worker.
     *
     *  This method will throw if query ID or chunk number is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @param chunk:     Chunk number.
     *  @param xrdEndpoint:  Worker xrootd communication endpoint ("host:port").
     */
    virtual void assignChunk(QueryId queryId,
                             int chunk,
                             std::string const& xrdEndpoint);

    /**
     *  @brief Mark chunk as completed.
     *
     *  This method will throw if query ID or chunk number is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @param chunk:     Sequence of chunk numbers.
     */
    virtual void finishChunk(QueryId queryId, int chunk);

    /**
     *  @brief Mark query as completed or failed.
     *
     *  This should be called when all data is collected in the result table or
     *  when failure/abort is detected.
     *  This method will throw if query ID is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @param qStatus:   Query completion status, one of COMPLETED, FAILED, or ABORTED.
     */
    virtual void completeQuery(QueryId queryId, QInfo::QStatus qStatus);

    /**
     *  @brief Mark query as finished and returned to client.
     *
     *  This should be called after query result is sent back to client.
     *
     *  This method will throw if query ID is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     */
    virtual void finishQuery(QueryId queryId);

    /**
     *  @brief Generic interface for finding queries.
     *
     *  Returns the set of query IDs which satisfy all selections specified in parameters.
     *
     *  Setting "completed" to true is equivalent to setting "status" to a set of
     *  (COMPLETED, FAILED, ABORTED) but is based on different QInfo attribute,
     *  it uses "completed" instead of "status". Similarly setting "completed"
     *  to false is equivalent to setting "status" to (EXECUTING).
     *
     *  @param czarId:    Czar ID, non-negative number, if zero then
     *                    queries for all czars are returned
     *  @param qType:     Query type, if ANY then all query types are returned.
     *  @param user:      User name, if empty then queries for all users are returned.
     *  @param status:    Set of QInfo::QStatus values, only queries with status
     *                    that match any value in the list are returned, if set is
     *                    empty then all queries are returned.
     *  @param completed: If set to true/1 then select only completed queries (or
     *                    failed/aborted), if set to false/0 then return queries that
     *                    are still executing, if set to -1 (default) return all queries.
     *  @param returned:  If set to true/1 then select only queries with results already
     *                    returned to client, if set to false/0 then return queries with
     *                    result waiting to be returned or still executing, if set to -1
     *                    (default) return all queries.
     *  @return: List of query IDs.
     */
    virtual std::vector<QueryId> findQueries(CzarId czarId=0,
                                             QInfo::QType qType=QInfo::ANY,
                                             std::string const& user=std::string(),
                                             std::vector<QInfo::QStatus> status=std::vector<QInfo::QStatus>(),
                                             int completed=-1,
                                             int returned=-1);

    /**
     *  @brief Find all pending queries for given czar.
     *
     *  Pending queries are queries which are either executing or
     *  have their result ready but not sent to client yet.
     *
     *  This method will throw if czar ID is not known.
     *
     *  @param czarId:   Czar ID, non-negative number.
     *  @return: List of query IDs.
     */
    virtual std::vector<QueryId> getPendingQueries(CzarId czarId);

    /**
     *  @brief Get full query information.
     *
     *  This method will throw if specified query ID does not exist.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @return: Object with query information.
     */
    virtual QInfo getQueryInfo(QueryId queryId);

    /**
     *  @brief Get queries which use specified table.
     *
     *  Only currently executing queries are returned.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @return: List of query IDs.
     */
    virtual std::vector<QueryId> getQueriesForTable(std::string const& dbName,
                                                    std::string const& tableName);

protected:

    ///  Check that all necessary tables exist
    void _checkDb();

private:

    sql::SqlConnection _conn;
    std::mutex _dbMutex;    ///< Synchronizes access to certain DB operations

};

}}} // namespace lsst::qserv::qmeta

#endif // LSST_QSERV_QMETA_QMETAMYSQL_H
