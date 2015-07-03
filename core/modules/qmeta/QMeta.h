/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef QMETA_QMETA_H
#define QMETA_QMETA_H

// System headers
#include <string>
#include <utility>
#include <vector>

// Qserv headers
#include "qmeta/QInfo.h"


namespace lsst {
namespace qserv {
namespace qmeta {

/// @addtogroup qmeta

/**
 *  @ingroup qmeta
 *
 *  @brief Interface for query metadata.
 */

class QMeta  {
public:

    /**
     *  Type for representing the list of tables, first item in pair is
     *  database name, second is table name.
     */
    typedef std::vector<std::pair<std::string, std::string> > TableNames;

    // Instances cannot be copied
    QMeta(QMeta const&) = delete;
    QMeta& operator=(QMeta const&) = delete;

    // Destructor
    virtual ~QMeta() {}

    /**
     *  @brief Return czar ID given czar "name".
     *
     *  Negative number is returned if czar does not exist.
     *
     *  @param name:  Czar name, arbitrary string.
     *  @return: Car ID, negative if czar does not exist.
     */
    virtual int getCzarID(std::string const& name) = 0;

    /**
     *  @brief Register new czar, return czar ID.
     *
     *  If czar with the same name is already registered then its ID
     *  will be returned, otherwise new record will be created.
     *  In both cases the czar will be active after this call.
     *
     *  @param name:  Czar name, arbitrary string.
     *  @return: Car ID, non-negative number.
     */
    virtual int registerCzar(std::string const& name) = 0;

    /**
     *  @brief Mark specified czar as active or inactive.
     *
     *  This method will throw if czar ID is not known.
     *
     *  @param name:  Czar ID, non-negative number.
     *  @param active:  new value if active flag.
     */
    virtual void setCzarActive(int czarId, bool active) = 0;

    /**
     *  @brief Register new query.
     *
     *  This method will throw if czar ID is not known.
     *
     *  @param qInfo:  Query info instance, time members (submitted/collected) are ignored.
     *  @param tables: Set of tables used by the query, may be empty if tables are not needed
     *                 (e.g. for interactive queries).
     *  @return: Query ID, non-negative number
     */
    virtual int registerQuery(QInfo const& qInfo,
                              TableNames const& tables) = 0;

    /**
     *  @brief Add list of chunks to query.
     *
     *  This method will throw if query ID is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @param chunks:    Set of chunk numbers.
     */
    virtual void addChunks(int queryId, std::vector<int> const& chunks) = 0;

    /**
     *  @brief Assign or re-assign chunk to a worker.
     *
     *  This method will throw if query ID or chunk number is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @param chunk:     Chunk number.
     *  @param xrdEndpoint:  Worker xrootd communication endpoint ("host:port").
     */
    virtual void assignChunk(int queryId,
                             int chunk,
                             std::string const& xrdEndpoint) = 0;

    /**
     *  @brief Mark chunk as completed.
     *
     *  This method will throw if query ID or chunk number is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @param chunk:     Sequence of chunk numbers.
     */
    virtual void finishChunk(int queryId, int chunk) = 0;

    /**
     *  @brief Mark query as collected.
     *
     *  This should be called when all data is collected in the result table.
     *  This method will throw if query ID is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     */
    virtual void markQueryCollected(int queryId) = 0;

    /**
     *  @brief Mark query as completed.
     *
     *  This should be called after query result is sent back to client.
     *
     *  This method will throw if query ID is not known.
     *
     *  @param queryId:   Query ID, non-negative number.
     */
    virtual void finishQuery(int queryId) = 0;

    /**
     *  @brief Find all queries currently executing.
     *
     *  This method will throw if czar ID is not known.
     *
     *  @param czarId:   Czar ID, non-negative number.
     *  @return: List of query IDs.
     */
    virtual std::vector<int> getExecutingQueries(int czarId) = 0;

    /**
     *  @brief Get full query information.
     *
     *  This method will throw if specified query ID does not exist.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @return: Object with query information.
     */
    virtual QInfo getQueryInfo(int queryId) = 0;

    /**
     *  @brief Get queries which use specified table.
     *
     *  Only currently executing queries are returned.
     *
     *  @param queryId:   Query ID, non-negative number.
     *  @return: List of query IDs.
     */
    virtual std::vector<int> getQueriesForTable(std::string const& dbName,
                                                std::string const& tableName) = 0;

protected:

    // Default constructor
    QMeta() {}

};

}}} // namespace lsst::qserv::qmeta

#endif // QMETA_QMETA_H
