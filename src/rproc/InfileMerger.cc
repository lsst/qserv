// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2019 LSST.
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
/**
 * @file
 *
 * @brief InfileMerger implementation
 *
 * InfileMerger is a class that is responsible for the organized
 * merging of query results into a single table that can be returned
 * to the user. The current strategy loads dumped chunk result tables
 * from workers into a single table, followed by a
 * merging/aggregation query (as needed) to produce the final user
 * result table.
 *
 * @author Daniel L. Wang, SLAC
 */

// Class header
#include "rproc/InfileMerger.h"

// System headers
#include <chrono>
#include <cstddef>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <thread>

// Third-party headers
#include "boost/format.hpp"
#include "boost/regex.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "cconfig/CzarConfig.h"
#include "global/intTypes.h"
#include "proto/worker.pb.h"
#include "qdisp/CzarStats.h"
#include "qproc/DatabaseModels.h"
#include "query/ColumnRef.h"
#include "query/SelectStmt.h"
#include "rproc/ProtoRowBuffer.h"
#include "sql/Schema.h"
#include "sql/SqlConnection.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"
#include "sql/SqlErrorObject.h"
#include "sql/statement.h"
#include "util/Bug.h"
#include "util/Error.h"
#include "util/IterableFormatter.h"
#include "util/Timer.h"

using namespace std;
namespace util = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.rproc.InfileMerger");

/// @return a timestamp id for use in generating temporary result table names.
std::string getTimeStampId() {
    struct timeval now;
    int rc = gettimeofday(&now, nullptr);
    if (rc != 0) {
        throw util::Error(util::ErrorCode::INTERNAL, "Failed to get timestamp.");
    }
    std::ostringstream s;
    s << (now.tv_sec % 10000) << now.tv_usec;
    return s.str();
    // Use the lower digits as pseudo-unique (usec, sec % 10000)
    // Alternative (for production?) Use boost::uuid to construct ids that are
    // guaranteed to be unique.
}

size_t const MB_SIZE_BYTES = 1024 * 1024;

/// @return Error info on the last operation with MySQL
string lastMysqlError(MYSQL* mysql) {
    return "error: " + string(mysql_error(mysql)) + ", errno: " + to_string(mysql_errno(mysql));
}

/**
 * Instances of this class are used to update statistic counter on starting
 * and finishing operations with merging results into the database.
 */
class ResultMergeTracker {
public:
    ResultMergeTracker() { lsst::qserv::qdisp::CzarStats::get()->addResultMerge(); }
    ~ResultMergeTracker() { lsst::qserv::qdisp::CzarStats::get()->deleteResultMerge(); }
};

lsst::qserv::TimeCountTracker<double>::CALLBACKFUNC const reportMergeRate =
        [](lsst::qserv::TIMEPOINT start, lsst::qserv::TIMEPOINT end, double bytes, bool success) {
            if (!success) return;
            if (chrono::duration<double> const seconds = end - start; seconds.count() > 0) {
                lsst::qserv::qdisp::CzarStats::get()->addMergeRate(bytes / seconds.count());
            }
        };

}  // anonymous namespace

namespace lsst::qserv::rproc {

////////////////////////////////////////////////////////////////////////
// InfileMerger public
////////////////////////////////////////////////////////////////////////
InfileMerger::InfileMerger(rproc::InfileMergerConfig const& c,
                           std::shared_ptr<qproc::DatabaseModels> const& dm,
                           util::SemaMgr::Ptr const& semaMgrConn)
        : _config(c),
          _mysqlConn(_config.mySqlConfig),
          _databaseModels(dm),
          _maxSqlConnectionAttempts(cconfig::CzarConfig::instance()->getMaxSqlConnectionAttempts()),
          _maxResultTableSizeBytes(cconfig::CzarConfig::instance()->getMaxTableSizeMB() * MB_SIZE_BYTES),
          _semaMgrConn(semaMgrConn) {
    _fixupTargetName();
    if (!_setupConnectionMyIsam()) {
        throw util::Error(util::ErrorCode::MYSQLCONNECT, "InfileMerger mysql connect failure.");
    }

    // The DEBUG level is good here since this report will be made onces per query,
    // not per each chunk.
    LOGS(_log, LOG_LVL_DEBUG,
         "InfileMerger maxResultTableSizeBytes=" << _maxResultTableSizeBytes
                                                 << " maxSqlConnexctionAttempts=" << _maxSqlConnectionAttempts
                                                 << " debugNoMerge=" << (_config.debugNoMerge ? "1" : " 0"));
}

std::string InfileMerger::_getQueryIdStr() {
    std::lock_guard<std::mutex> lk(_queryIdStrMtx);
    return _queryIdStr;
}

void InfileMerger::_setQueryIdStr(std::string const& qIdStr) {
    std::lock_guard<std::mutex> lk(_queryIdStrMtx);
    _queryIdStr = qIdStr;
    _queryIdStrSet = true;
}

void InfileMerger::mergeCompleteFor(int jobId) {
    std::lock_guard<std::mutex> resultSzLock(_mtxResultSizeMtx);
    _totalResultSize += _perJobResultSize[jobId];  // TODO:UJ this can probably be simplified
}

uint32_t histLimitCount = 0;
util::HistogramRolling histoInfileBuild("&&&uj histoInfileBuild", {0.1, 1.0, 10.0, 100.0, 1000.0}, 1h, 10000);
util::HistogramRolling histoMergeSecs("&&&uj histoMergeSecs", {0.1, 1.0, 10.0, 100.0, 1000.0}, 1h, 10000);
util::HistogramRolling histoMergeSzB("&&&uj histoMergeSzB", {0.1, 1.0, 10.0, 100.0, 1000.0}, 1h, 10000);

bool InfileMerger::mergeHttp(qdisp::UberJob::Ptr const& uberJob, proto::ResponseData const& responseData) {
    UberJobId const uJobId = uberJob->getUjId();
    std::string queryIdJobStr = uberJob->getIdStr();
    if (!_queryIdStrSet) {
        _setQueryIdStr(QueryIdHelper::makeIdStr(uberJob->getQueryId()));
    }

    // Nothing to do if size is zero.
    if (responseData.row_size() == 0) {
        return true;
    }

    // Do nothing if the query got cancelled for any reason.
    if (uberJob->isQueryCancelled()) {
        return true;
    }
    auto executive = uberJob->getExecutive();
    if (executive == nullptr || executive->getCancelled() || executive->isRowLimitComplete()) {
        return true;
    }

    std::unique_ptr<util::SemaLock> semaLock;
    if (_dbEngine != MYISAM) {
        // needed for parallel merging with INNODB and MEMORY
        semaLock.reset(new util::SemaLock(*_semaMgrConn));
    }

    TimeCountTracker<double>::CALLBACKFUNC cbf = [](TIMEPOINT start, TIMEPOINT end, double bytes,
                                                    bool success) {
        if (!success) return;
        if (std::chrono::duration<double> const seconds = end - start; seconds.count() > 0) {
            qdisp::CzarStats::get()->addXRootDSSIRecvRate(bytes / seconds.count());
        }
    };
    auto tct = make_shared<TimeCountTracker<double>>(cbf);

    bool ret = false;
    // Add columns to rows in virtFile.
    util::Timer virtFileT;
    virtFileT.start();
    auto startInfileBuild = CLOCK::now();  //&&&
    // UberJobs only get one attempt
    int resultJobId = makeJobIdAttempt(uberJob->getUjId(), 0);
    ProtoRowBuffer::Ptr pRowBuffer = std::make_shared<ProtoRowBuffer>(
            responseData, resultJobId, _jobIdColName, _jobIdSqlType, _jobIdMysqlType);
    std::string const virtFile = _infileMgr.prepareSrc(pRowBuffer);
    std::string const infileStatement = sql::formLoadInfile(_mergeTable, virtFile);
    virtFileT.stop();
    auto endInfileBuild = CLOCK::now();                                                 //&&&
    std::chrono::duration<double> secsInfileBuild = endInfileBuild - startInfileBuild;  // &&&
    histoInfileBuild.addEntry(endInfileBuild, secsInfileBuild.count());                 //&&&

    // If the job attempt is invalid, exit without adding rows.
    // It will wait here if rows need to be deleted.
    if (_invalidJobAttemptMgr.incrConcurrentMergeCount(resultJobId)) {
        return true;
    }

    size_t const resultSize = responseData.transmitsize();
    size_t tResultSize;
    {
        std::lock_guard<std::mutex> resultSzLock(_mtxResultSizeMtx);
        _perJobResultSize[uJobId] += resultSize;
        tResultSize = _totalResultSize + _perJobResultSize[uJobId];
    }
    if (tResultSize > _maxResultTableSizeBytes) {
        std::ostringstream os;
        os << queryIdJobStr << " cancelling the query, queryResult table " << _mergeTable
           << " is too large at " << tResultSize << " bytes, max allowed size is " << _maxResultTableSizeBytes
           << " bytes";
        LOGS(_log, LOG_LVL_ERROR, os.str());
        _error = util::Error(-1, os.str(), -1);
        return false;
    }

    tct->addToValue(resultSize);
    tct->setSuccess();
    tct.reset();  // stop transmit recieve timer before merging happens.

    qdisp::CzarStats::get()->addTotalBytesRecv(resultSize);
    qdisp::CzarStats::get()->addTotalRowsRecv(responseData.rowcount());

    // Stop here (if requested) after collecting stats on the amount of data collected
    // from workers.
    if (_config.debugNoMerge) {
        return true;
    }

    //&&&auto start = std::chrono::system_clock::now();
    auto start = CLOCK::now();
    switch (_dbEngine) {
        case MYISAM:
            ret = _applyMysqlMyIsam(infileStatement, resultSize);
            break;
        case INNODB:  // Fallthrough
        case MEMORY:
            ret = _applyMysqlInnoDb(infileStatement, resultSize);
            break;
        default:
            throw std::invalid_argument("InfileMerger::_dbEngine is unknown =" + engineToStr(_dbEngine));
    }
    auto end = CLOCK::now();
    auto mergeDur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOGS(_log, LOG_LVL_TRACE,
         "mergeDur=" << mergeDur.count() << " sema(total=" << _semaMgrConn->getTotalCount()
                     << " used=" << _semaMgrConn->getUsedCount() << ")");
    std::chrono::duration<double> secs = end - start;  // &&&
    histoMergeSecs.addEntry(end, secs.count());        //&&&
    histoMergeSzB.addEntry(end, resultSize);           // &&&
    if ((++histLimitCount) % 1000 == 0) {
        LOGS(_log, LOG_LVL_INFO, "&&&uj histo " << histoInfileBuild.getString(""));
        LOGS(_log, LOG_LVL_INFO, "&&&uj histo " << histoMergeSecs.getString(""));
        LOGS(_log, LOG_LVL_INFO, "&&&uj histo " << histoMergeSzB.getString(""));
    }

    if (not ret) {
        LOGS(_log, LOG_LVL_ERROR, "InfileMerger::merge mysql applyMysql failure");
    }
    _invalidJobAttemptMgr.decrConcurrentMergeCount();

    LOGS(_log, LOG_LVL_TRACE, "virtFileT=" << virtFileT.getElapsed() << " mergeDur=" << mergeDur.count());

    return ret;
}

bool InfileMerger::_applyMysqlMyIsam(std::string const& query, size_t resultSize) {
    std::unique_lock<std::mutex> lock(_mysqlMutex);
    for (int j = 0; !_mysqlConn.connected(); ++j) {
        // should have connected during construction
        // Try reconnecting--maybe we timed out.
        if (!_setupConnectionMyIsam()) {
            if (j < sqlConnectionAttempts()) {
                lock.unlock();
                sleep(1);
                lock.lock();
            } else {
                LOGS(_log, LOG_LVL_ERROR,
                     "InfileMerger::_applyMysqlMyIsam _setupConnectionMyIsam() failed!!!");
                return false;  // Reconnection failed. This is an error.
            }
        }
    }

    // Track the operation while the control flow is staying within the function.
    ::ResultMergeTracker const resultMergeTracker;

    // This starts a timer of the result merge rate tracker. The tracker will report
    // the counter (if set) upon leaving the method.
    lsst::qserv::TimeCountTracker<double> mergeRateTracker(::reportMergeRate);

    int rc = mysql_real_query(_mysqlConn.getMySql(), query.data(), query.size());
    if (rc == 0) {
        mergeRateTracker.addToValue(resultSize);
        mergeRateTracker.setSuccess();
        return true;
    }
    LOGS(_log, LOG_LVL_ERROR,
         "InfileMerger::_applyMysqlMyIsam mysql_real_query() " + ::lastMysqlError(_mysqlConn.getMySql()));
    return false;
}

size_t InfileMerger::getTotalResultSize() const { return _totalResultSize; }

bool InfileMerger::finalize(size_t& collectedBytes, int64_t& rowCount) {
    bool finalizeOk = true;
    collectedBytes = _totalResultSize;
    // TODO: Should check for error condition before continuing.
    if (_isFinished) {
        LOGS(_log, LOG_LVL_ERROR, "InfileMerger::finalize(), but _isFinished == true");
    }
    if (_mergeTable != _config.targetTable) {
        // Aggregation needed: Do the aggregation.
        std::string mergeSelect = _config.mergeStmt->getQueryTemplate().sqlFragment();
        // Using MyISAM as single thread writing with no need to recover from errors.
        std::string createMerge = "CREATE TABLE " + _config.targetTable + " ENGINE=MyISAM " + mergeSelect;
        LOGS(_log, LOG_LVL_TRACE, "Prepping merging w/" << *_config.mergeStmt);
        LOGS(_log, LOG_LVL_DEBUG, "Merging w/" << createMerge);
        finalizeOk = _applySqlLocal(createMerge, "createMerge");

        // Find the number of rows in the new table.
        string countRowsSql = "SELECT COUNT(*) FROM " + _config.targetTable;
        sql::SqlResults countRowsResults;
        sql::SqlErrorObject countRowsErrObj;
        bool countSuccess = _applySqlLocal(countRowsSql, countRowsResults, countRowsErrObj);
        if (countSuccess) {
            // should be 1 column, 1 row in the results
            vector<string> counts;
            if (countRowsResults.extractFirstColumn(counts, countRowsErrObj) && counts.size() > 0) {
                rowCount = std::stoll(counts[0]);
                LOGS(_log, LOG_LVL_DEBUG, "rowCount=" << rowCount << " " << countRowsSql);
            } else {
                LOGS(_log, LOG_LVL_ERROR, "Failed to extract row count result");
                rowCount = 0;  // Return 0 rows since there was a problem.
            }
        } else {
            LOGS(_log, LOG_LVL_ERROR,
                 "InfileMerger::finalize countRows query failed " << countRowsErrObj.printErrMsg());
            rowCount = 0;  // Return 0 rows since there was a problem.
        }

        // Cleanup merge table.
        sql::SqlErrorObject eObj;
        // Don't report failure on not exist
        LOGS(_log, LOG_LVL_TRACE, "Cleaning up " << _mergeTable);
#if 1  // Set to 0 when we want to retain mergeTables for debugging.
        bool cleanupOk = _sqlConn->dropTable(_mergeTable, eObj, false, _config.mySqlConfig.dbName);
#else
        bool cleanupOk = true;
#endif
        if (!cleanupOk) {
            LOGS(_log, LOG_LVL_WARN, "Failure cleaning up table " << _mergeTable);
        }
    } else {
        rowCount = -1;  // rowCount is meaningless since there was no postprocessing.
    }
    LOGS(_log, LOG_LVL_TRACE, "Merged " << _mergeTable << " into " << _config.targetTable);
    _isFinished = true;
    return finalizeOk;
}

bool InfileMerger::isFinished() const { return _isFinished; }

void InfileMerger::setMergeStmtFromList(std::shared_ptr<query::SelectStmt> const& mergeStmt) const {
    if (mergeStmt != nullptr) {
        mergeStmt->setFromListAsTable(_mergeTable);
    }
}

bool InfileMerger::getSchemaForQueryResults(query::SelectStmt const& stmt, sql::Schema& schema) {
    sql::SqlResults results;
    sql::SqlErrorObject getSchemaErrObj;
    std::string query = stmt.getQueryTemplate().sqlFragment();
    bool ok = _databaseModels->applySql(query, results, getSchemaErrObj);
    if (not ok) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to get schema:" << getSchemaErrObj.errMsg());
        _error = util::Error(util::ErrorCode::INTERNAL, "Failed to get schema: " + getSchemaErrObj.errMsg());
        return false;
    }
    sql::SqlErrorObject errObj;
    if (errObj.isSet()) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to extract schema from result: " << errObj.errMsg());
        _error = util::Error(util::ErrorCode::INTERNAL,
                             "Failed to extract schema from result: " + errObj.errMsg());
        return false;
    }
    schema = results.makeSchema(errObj);
    LOGS(_log, LOG_LVL_TRACE, "InfileMerger extracted schema: " << schema);
    return true;
}

bool InfileMerger::makeResultsTableForQuery(query::SelectStmt const& stmt) {
    sql::Schema schema;
    if (not getSchemaForQueryResults(stmt, schema)) {
        return false;
    }
    std::string const createStmt = sql::formCreateTable(_mergeTable, schema) + " ENGINE=MyISAM";
    LOGS(_log, LOG_LVL_TRACE, "InfileMerger make results table query: " << createStmt);
    if (not _applySqlLocal(createStmt, "makeResultsTableForQuery")) {
        _error = util::Error(util::ErrorCode::CREATE_TABLE,
                             "Error creating table:" + _mergeTable + ": " + _error.getMsg());
        _isFinished = true;  // Cannot continue.
        LOGS(_log, LOG_LVL_ERROR, "InfileMerger sql error: " << _error.getMsg());
        return false;
    }
    return true;
}

bool InfileMerger::_applySqlLocal(std::string const& sql, std::string const& logMsg,
                                  sql::SqlResults& results) {
    auto begin = std::chrono::system_clock::now();
    bool success = _applySqlLocal(sql, results);
    auto end = std::chrono::system_clock::now();
    LOGS(_log, LOG_LVL_TRACE,
         logMsg << " success=" << success << " microseconds="
                << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count());
    return success;
}

bool InfileMerger::_applySqlLocal(std::string const& sql, std::string const& logMsg) {
    sql::SqlResults results(true);  // true = throw results away immediately
    return _applySqlLocal(sql, logMsg, results);
}

bool InfileMerger::_applySqlLocal(std::string const& sql, sql::SqlResults& results) {
    sql::SqlErrorObject errObj;
    return _applySqlLocal(sql, results, errObj);
}

/// Apply a SQL query, setting the appropriate error upon failure.
bool InfileMerger::_applySqlLocal(std::string const& sql, sql::SqlResults& results,
                                  sql::SqlErrorObject& errObj) {
    std::lock_guard<std::mutex> m(_sqlMutex);

    if (not _sqlConnect(errObj)) {
        return false;
    }
    if (not _sqlConn->runQuery(sql, results, errObj)) {
        _error = util::Error(errObj.errNo(), "Error applying sql: " + errObj.printErrMsg(),
                             util::ErrorCode::MYSQLEXEC);
        LOGS(_log, LOG_LVL_ERROR, "InfileMerger error: " << _error.getMsg());
        return false;
    }
    LOGS(_log, LOG_LVL_TRACE, "InfileMerger query success: " << sql);
    return true;
}

bool InfileMerger::_sqlConnect(sql::SqlErrorObject& errObj) {
    if (_sqlConn == nullptr) {
        _sqlConn = sql::SqlConnectionFactory::make(_config.mySqlConfig);
        if (not _sqlConn->connectToDb(errObj)) {
            _error = util::Error(errObj.errNo(), "Error connecting to db: " + errObj.printErrMsg(),
                                 util::ErrorCode::MYSQLCONNECT);
            _sqlConn.reset();
            LOGS(_log, LOG_LVL_ERROR, "InfileMerger error: " << _error.getMsg());
            return false;
        }
        LOGS(_log, LOG_LVL_TRACE, "InfileMerger " << (void*)this << " connected to db");
    }
    return true;
}

/// Choose the appropriate target name, depending on whether post-processing is
/// needed on the result rows.
void InfileMerger::_fixupTargetName() {
    if (_config.targetTable.empty()) {
        assert(not _config.mySqlConfig.dbName.empty());
        _config.targetTable =
                (boost::format("%1%.result_%2%") % _config.mySqlConfig.dbName % getTimeStampId()).str();
    }

    if (_config.mergeStmt) {
        // Set merging temporary if needed.
        _mergeTable = _config.targetTable + "_m";
    } else {
        _mergeTable = _config.targetTable;
    }
}

bool InfileMerger::_setupConnectionMyIsam() {
    if (_mysqlConn.connect()) {
        _infileMgr.attach(_mysqlConn.getMySql());
        return true;
    }
    return false;
}

}  // namespace lsst::qserv::rproc
