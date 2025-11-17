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
#ifndef LSST_QSERV_QMETA_QINFO_H
#define LSST_QSERV_QMETA_QINFO_H

// System headers
#include <ctime>
#include <string>

// Qserv headers

namespace lsst::qserv::qmeta {

/// @addtogroup qmeta

/**
 *  @ingroup qmeta
 *
 *  @brief Class containing information about query metadata.
 */

class QInfo {
public:
    /**
     *  Constants for query types.
     */
    enum QType {
        SYNC,   ///< Synchronous query
        ASYNC,  ///< Asynchronous query
        ANY     ///< ANY is only used in finding queries in database
    };

    /**
     *  Constants for query status.
     */
    enum QStatus {
        EXECUTING,  ///< Query is currently executing (or being prepared)
        COMPLETED,  ///< Query execution completed successfully
        FAILED,     ///< Query execution failed
        FAILED_LR,  ///< Query execution failed due to large result set
        ABORTED     ///< Query execution was intentionally aborted
    };

    /// Default constructor
    QInfo() : _qType(ANY), _qStatus(EXECUTING), _czarId(-1), _submitted(0), _completed(0), _returned(0) {}

    /**
     *  @brief Make new instance.
     *
     *  @param qType:  Query type, one of QType constants.
     *  @param czarId: Czar ID, non-negative number.
     *  @param user:   User name for user who issued the query.
     *  @param qText:  Original query text as given by user.
     *  @param qTemplate:  Query template used to build per-chunk queries.
     *  @param qMerge: Aggregate query to be executed on results table, possibly empty.
     *  @param resultLoc: Location of the query result.
     *  @param msgTableName: Name of the message table.
     *  @param resultQuery: Result query.
     *  @param chunkCount: Number of chunks need by the query.
     *  @param qStatus: Query processing status.
     *  @param submitted: Time when query was submitted (seconds since epoch).
     *  @param completed: Time when query finished execution, 0 if not finished.
     *  @param returned: Time when query result was sent to client, 0 if not sent yet.
     */
    QInfo(QType qType, CzarId czarId, std::string const& user, std::string const& qText,
          std::string const& qTemplate, std::string const& qMerge, std::string const& resultLoc,
          std::string const& msgTableName, std::string const& resultQuery, int chunkCount,
          QStatus qStatus = EXECUTING, std::time_t submitted = std::time_t(0),
          std::time_t completed = std::time_t(0), std::time_t returned = std::time_t(0))
            : _qType(qType),
              _qStatus(qStatus),
              _czarId(czarId),
              _user(user),
              _qText(qText),
              _qTemplate(qTemplate),
              _qMerge(qMerge),
              _resultLoc(resultLoc),
              _msgTableName(msgTableName),
              _resultQuery(resultQuery),
              _chunkCount(chunkCount),
              _submitted(submitted),
              _completed(completed),
              _returned(returned) {}

    // This class requires copying. Defaults are currently sufficient.
    QInfo(QInfo const&) = default;
    QInfo& operator=(QInfo const&) = default;

    /// Returns query type
    QType queryType() const { return _qType; }

    /// Returns query processing status
    QStatus queryStatus() const { return _qStatus; }

    /// Returns czar Id
    CzarId czarId() const { return _czarId; }

    /// Returns user name
    std::string const& user() const { return _user; }

    /// Returns original query text
    std::string const& queryText() const { return _qText; }

    /// Returns query template
    std::string const& queryTemplate() const { return _qTemplate; }

    /// Returns query for result (aggregate) which may be empty
    std::string const& mergeQuery() const { return _qMerge; }

    /// Returns location of query result
    std::string const& resultLocation() const { return _resultLoc; }

    /// Returns message table name
    std::string const& msgTableName() const { return _msgTableName; }

    /// Returns the query to get results, used by mysql-proxy.
    std::string const& resultQuery() const { return _resultQuery; }

    /// Return time when query was submitted
    std::time_t submitted() const { return _submitted; }

    /// Return time when query was completed
    std::time_t completed() const { return _completed; }

    /// Return time when query result was returned to client
    std::time_t returned() const { return _returned; }

    /// Return query execution time in seconds
    std::time_t duration() const { return _completed != 0 ? _completed - _submitted : 0; }

    /// Return the number of chunks to be used to answer the query.
    int chunkCount() const { return _chunkCount; }

private:
    QType _qType;                ///< Query type, one of QType constants
    QStatus _qStatus;            ///< Query processing status
    CzarId _czarId;              ///< Czar ID, non-negative number.
    std::string _user;           ///< User name for user who issued the query.
    std::string _qText;          ///< Original query text as given by user.
    std::string _qTemplate;      ///< Query template used to build per-chunk queries.
    std::string _qMerge;         ///< Aggregate query to be executed on results table, possibly empty.
    std::string _qProxyOrderBy;  ///< ORDER BY clause for proxy-side SELECT statement, possibly empty.
    std::string _resultLoc;      ///< Location of query result, e.g. table:result_12345
    std::string _msgTableName;   ///< Name of the message table for this query
    std::string _resultQuery;    ///< The query for qserv-proxy to execute to get results.
    int _chunkCount = 0;         ///< Number of chunks in the query.
    std::time_t _submitted;      ///< Time when query was submitted (seconds since epoch).
    std::time_t _completed;      ///< Time when query finished execution, 0 if not finished.
    std::time_t _returned;       ///< Time when query result was sent to client, 0 if not sent yet.
};

}  // namespace lsst::qserv::qmeta

#endif  // LSST_QSERV_QMETA_QINFO_H
