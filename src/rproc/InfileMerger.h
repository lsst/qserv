// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_RPROC_INFILEMERGER_H
#define LSST_QSERV_RPROC_INFILEMERGER_H
/// InfileMerger.h declares:
///
/// class InfileMergerConfig
/// class InfileMerger
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <mutex>
#include <set>
#include <string>

// Qserv headers
#include "mysql/LocalInfile.h"
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "sql/SqlConnection.h"
#include "util/Error.h"
#include "util/EventThread.h"
#include "util/SemaMgr.h"

#include "util/InstanceCount.h"

// Forward declarations
namespace lsst::qserv {
namespace mysql {
class MysqlConfig;
}
namespace proto {
class ResponseData;
class ResponseSummary;
}  // namespace proto
namespace qdisp {
class MessageStore;
class UberJob;
}  // namespace qdisp
namespace QMeta {
class MessageStore;
}
namespace qproc {
class DatabaseModels;
}
namespace query {
class SelectStmt;
}
namespace sql {
class Schema;
class SqlConnection;
class SqlResults;
}  // namespace sql
}  // namespace lsst::qserv

namespace lsst::qserv::rproc {

/// class InfileMergerConfig - value class for configuring a InfileMerger
class InfileMergerConfig {
public:
    InfileMergerConfig() = delete;
    InfileMergerConfig(mysql::MySqlConfig const& mySqlConfig_) : mySqlConfig(mySqlConfig_) {}

    // for final result, and imported result
    mysql::MySqlConfig const mySqlConfig;
    std::string targetTable;
    std::shared_ptr<query::SelectStmt> mergeStmt;
    bool debugNoMerge = false;
};

/// InfileMerger is a row-based merger that imports rows from result messages
/// and inserts them into a MySQL table, as specified during construction by
/// InfileMergerConfig.
///
/// To use, construct a configured instance, then call merge() to kick off the
/// merging process, and finalize() to wait for outstanding merging processes
/// and perform the appropriate post-processing before returning.  merge() right
/// now expects a parsed ResponseData message.
/// At present, Result messages are not chained.
class InfileMerger {
public:
    explicit InfileMerger(InfileMergerConfig const& c, std::shared_ptr<qproc::DatabaseModels> const& dm,
                          std::shared_ptr<util::SemaMgr> const& semaMgrConn);
    InfileMerger() = delete;
    InfileMerger(InfileMerger const&) = delete;
    InfileMerger& operator=(InfileMerger const&) = delete;
    ~InfileMerger() = default;

    /// Merge the result data collected over Http.
    bool mergeHttp(std::shared_ptr<qdisp::UberJob> const& uberJob, proto::ResponseData const& responseData);

    /// Merge the result data collected over Http.
    bool mergeHttp(std::shared_ptr<qdisp::UberJob> const& uberJob, proto::ResponseData const& responseData);

    /// Indicate the merge for the job is complete.
    void mergeCompleteFor(int jobId);

    /// @return error details if finalize() returns false
    util::Error const& getError() const { return _error; }
    /// @return final target table name  storing results after post processing
    std::string getTargetTable() const { return _config.targetTable; }

    /// Finalize a "merge" and perform postprocessing.
    /// `collectedBytes` is the number of bytes collected in worker results for
    ///    this user query. Its value is set by this function.
    /// `rowCount` is the number of rows in the final result, as determined by this
    ///    function. A negative value indicates that its value is meaningless,
    ///    which happens when there is no postprocessing of the result table.
    bool finalize(size_t& collectedBytes, int64_t& rowCount);
    /// Check if the object has completed all processing.
    bool isFinished() const;

    void setMergeStmtFromList(std::shared_ptr<query::SelectStmt> const& mergeStmt) const;

    /**
     * @brief Make a schema that matches the results of the given query.
     *
     * @param stmt The statement to make a schema for.
     * @param schema The schema object to hold the schema.
     * @return true If a schema was created.
     * @return false If a schema was NOT created. Call InfileMerger::getError() to get specific information
     *               about the failure.
     */
    bool getSchemaForQueryResults(query::SelectStmt const& stmt, sql::Schema& schema);

    /**
     * @brief Make the results table for the given query.
     *
     * Calculates the schema of the results table for the given query, and makes the results table for that
     * query.
     *
     * @param stmt The statment to make a results table for.
     * @return true If the results table was created.
     * @return false If the results table was NOT created. Call InfileMerger::getError() to get specific
     *               information about the failure.
     */
    bool makeResultsTableForQuery(query::SelectStmt const& stmt);

    int sqlConnectionAttempts() { return _maxSqlConnectionAttempts; }
    size_t getTotalResultSize() const;

private:
    bool _applyMysqlMyIsam(std::string const& query, size_t resultSize);
    void _setupRow();
    bool _applySql(std::string const& sql);
    bool _applySqlLocal(std::string const& sql, std::string const& logMsg, sql::SqlResults& results);
    bool _applySqlLocal(std::string const& sql, std::string const& logMsg);
    bool _applySqlLocal(std::string const& sql, sql::SqlResults& results);
    bool _applySqlLocal(std::string const& sql, sql::SqlResults& results, sql::SqlErrorObject& errObj);
    bool _sqlConnect(sql::SqlErrorObject& errObj);

    util::InstanceCount const _icIm{"InfileMerger"};
    std::string _getQueryIdStr();
    void _setQueryIdStr(std::string const& qIdStr);
    void _fixupTargetName();
    bool _setupConnectionMyIsam();

    InfileMergerConfig _config;                    ///< Configuration
    std::shared_ptr<sql::SqlConnection> _sqlConn;  ///< SQL connection
    std::string _mergeTable;                       ///< Table for result loading
    util::Error _error;                            ///< Error state
    bool _isFinished = false;                      ///< Completed?
    std::mutex _sqlMutex;                          ///< Protection for SQL connection
    mysql::MySqlConnection _mysqlConn;
    std::mutex _mysqlMutex;
    mysql::LocalInfile::Mgr _infileMgr;
    std::shared_ptr<qproc::DatabaseModels> _databaseModels;  ///< Used to create result table.
    std::mutex _queryIdStrMtx;                               ///< protects _queryIdStr
    std::atomic<bool> _queryIdStrSet{false};
    std::string _queryIdStr{"QI=?"};  ///< Unknown until results start coming back from workers.
    int const _maxSqlConnectionAttempts =
            10;  ///< maximum number of times to retry connecting to the SQL database.

    /// Variable to track result size. Each
    size_t const _maxResultTableSizeBytes;    ///< Max result table size in bytes.
    size_t _totalResultSize = 0;              ///< Size of result so far in bytes.
    std::map<int, size_t> _perJobResultSize;  ///< Result size for each job
    std::mutex _mtxResultSizeMtx;             ///< Protects _perJobResultSize and _totalResultSize.

    std::shared_ptr<util::SemaMgr> _semaMgrConn;  ///< Used to limit the number of open mysql connections.
};

}  // namespace lsst::qserv::rproc

#endif  // LSST_QSERV_RPROC_INFILEMERGER_H
