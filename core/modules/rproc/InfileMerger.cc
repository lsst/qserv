// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 AURA/LSST.
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
#include "czar/Czar.h"
#include "global/Bug.h"
#include "global/intTypes.h"
#include "proto/WorkerResponse.h"
#include "proto/ProtoImporter.h"
#include "qdisp/LargeResultMgr.h"
#include "query/SelectStmt.h"
#include "rproc/ProtoRowBuffer.h"
#include "sql/Schema.h"
#include "sql/SqlConnection.h"
#include "sql/SqlResults.h"
#include "sql/SqlErrorObject.h"
#include "sql/statement.h"
#include "util/StringHash.h"

namespace { // File-scope helpers

LOG_LOGGER _log = LOG_GET("lsst.qserv.rproc.InfileMerger");

using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::rproc::InfileMergerConfig;
using lsst::qserv::rproc::InfileMergerError;
using lsst::qserv::util::ErrorCode;

/// @return a timestamp id for use in generating temporary result table names.
std::string getTimeStampId() {
    struct timeval now;
    int rc = gettimeofday(&now, nullptr);
    if (rc != 0) {
        throw InfileMergerError(ErrorCode::INTERNAL, "Failed to get timestamp.");
    }
    std::ostringstream s;
    s << (now.tv_sec % 10000) << now.tv_usec;
    return s.str();
    // Use the lower digits as pseudo-unique (usec, sec % 10000)
    // Alternative (for production?) Use boost::uuid to construct ids that are
    // guaranteed to be unique.
}
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace rproc {


////////////////////////////////////////////////////////////////////////
// InfileMerger public
////////////////////////////////////////////////////////////////////////
InfileMerger::InfileMerger(InfileMergerConfig const& c)
    : _config(c),
      _mysqlConn(_config.mySqlConfig) {
    _alterJobIdColName(); // initialize jobIdColName.
    _fixupTargetName();
    _maxResultTableSizeMB = _config.mySqlConfig.maxTableSizeMB;

    // Assume worst case of 10,000 bytes per row, what's the earliest row to test?
    // Subtract that from the count so the first check doesn't happen for a while.
    // Subsequent checks should happen at reasonable intervals.
    // At 5000MB max size, the first check is made at 550,000 rows, with subsequent checks
    // about every 50,000 rows.
    _sizeCheckRowCount = -100*(_maxResultTableSizeMB);  //  100 = 1,000,000/10,000
    _checkSizeEveryXRows = 10*_maxResultTableSizeMB;
    LOGS(_log, LOG_LVL_DEBUG, "InfileMerger maxResultTableSizeMB=" << _maxResultTableSizeMB
                              << " sizeCheckRowCount=" << _sizeCheckRowCount
                              << " checkSizeEveryXRows=" << _checkSizeEveryXRows);
    if (_config.mergeStmt) {
        _config.mergeStmt->setFromListAsTable(_mergeTable);
    }

    _invalidJobAttemptMgr.setDeleteFunc([this](std::set<int> const& jobAttempts) -> bool {
        return _deleteInvalidRows(jobAttempts);
    });

    _invalidJobAttemptMgr.setTableExistsFunc([this]() -> bool {
        std::lock_guard<std::mutex> lockTable(_createTableMutex);
        return !_needCreateTable;
    });

    if (!_setupConnection()) {
        throw InfileMergerError(util::ErrorCode::MYSQLCONNECT, "InfileMerger mysql connect failure.");
    }
}


InfileMerger::~InfileMerger() {
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


bool InfileMerger::merge(std::shared_ptr<proto::WorkerResponse> response) {
    if (!response) {
        return false;
    }
    // TODO: Check session id (once session id mgmt is implemented)
    std::string queryIdJobStr =
        QueryIdHelper::makeIdStr(response->result.queryid(), response->result.jobid());
    if (!_queryIdStrSet) {
        _setQueryIdStr(QueryIdHelper::makeIdStr(response->result.queryid()));
    }
    LOGS(_log, LOG_LVL_DEBUG,
         "Executing InfileMerger::merge("
         << queryIdJobStr
         << " largeResult=" << response->result.largeresult()
         << " sizes=" << static_cast<short>(response->headerSize)
         << ", " << response->protoHeader.size()
         << ", rowCount=" << response->result.rowcount()
         << ", row_size=" << response->result.row_size()
         << ", attemptCount=" << response-> result.attemptcount()
         << ", errCode=" << response->result.has_errorcode()
         << " hasErMsg=" << response->result.has_errormsg() << ")");

    if (response->result.has_errorcode() || response->result.has_errormsg()) {
        _error = util::Error(response->result.errorcode(), response->result.errormsg(), util::ErrorCode::MYSQLEXEC);
        LOGS(_log, LOG_LVL_ERROR, "Error in response data: " << _error);
        return false;
    }
    if (_needCreateTable) {
        if (!_setupTable(*response)) {
            return false;
        }
    }

    // Nothing to do if size is zero.
    if (response->result.row_size() == 0) {
        return true;
    }
    _sizeCheckRowCount += response->result.row_size();

    bool ret = false;
    // Add columns to rows in virtFile.
    int resultJobId = makeJobIdAttempt(response->result.jobid(), response->result.attemptcount());
    ProtoRowBuffer::Ptr pRowBuffer = std::make_shared<ProtoRowBuffer>(response->result,
                                     resultJobId, _jobIdColName, _jobIdSqlType, _jobIdMysqlType);
    std::string const virtFile = _infileMgr.prepareSrc(pRowBuffer, queryIdJobStr);
    std::string const infileStatement = sql::formLoadInfile(_mergeTable, virtFile);
    auto start = std::chrono::system_clock::now();
    // If the job attempt is invalid, exit without adding rows.
    // It will wait here if rows need to be deleted.
    if (_invalidJobAttemptMgr.incrConcurrentMergeCount(resultJobId)) {
        return true;
    }
    ret = _applyMysql(infileStatement);
    _invalidJobAttemptMgr.decrConcurrentMergeCount();
    auto end = std::chrono::system_clock::now();
    auto mergeDur = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOGS(_log, LOG_LVL_DEBUG, queryIdJobStr << " mergeDur=" << mergeDur.count());
    /// Check the size of the result table.
    if (_sizeCheckRowCount >= _checkSizeEveryXRows) {
        LOGS(_log, LOG_LVL_DEBUG, queryIdJobStr << "checking ResultTableSize " << _mergeTable
                                  << " " << _sizeCheckRowCount
                                  << " max=" << _maxResultTableSizeMB);
        _sizeCheckRowCount = 0;
        auto tSize = _getResultTableSizeMB();
        if (tSize > _maxResultTableSizeMB) {
            // TODO:DM-11524 Try deleting invalid rows if there are any, then check size again &&&
            std::ostringstream os;
            os << queryIdJobStr << " cancelling queryResult table " << _mergeTable
               << " too large at " << tSize << "MB max allowed=" << _maxResultTableSizeMB;
            LOGS(_log, LOG_LVL_WARN, os.str());
            _error = util::Error(-1, os.str(), -1);
            return false;
        }
    }
    return ret;
}


bool InfileMerger::_applyMysql(std::string const& query) {
    std::lock_guard<std::mutex> lock(_mysqlMutex);
    if (!_mysqlConn.connected()) {
        // should have connected during construction
        // Try reconnecting--maybe we timed out.
        if (!_setupConnection()) {
            LOGS(_log, LOG_LVL_ERROR, "InfileMerger::_applyMysql _setupConnection() failed!!!");
            return false; // Reconnection failed. This is an error.
        }
    }

    int rc = mysql_real_query(_mysqlConn.getMySql(),
                              query.data(), query.size());
    return rc == 0;
}


bool InfileMerger::finalize() {
    bool finalizeOk = true;
    // TODO: Should check for error condition before continuing.
    if (_isFinished) {
        LOGS(_log, LOG_LVL_ERROR, "InfileMerger::finalize(), but _isFinished == true");
    }
    // TODO:DM-11524   delete all invalid rows in the table. &&&
    if (_mergeTable != _config.targetTable) {
        // Aggregation needed: Do the aggregation.
        std::string mergeSelect = _config.mergeStmt->getQueryTemplate().sqlFragment();
        // Using MyISAM as single thread writing with no need to recover from errors.
        std::string createMerge = "CREATE TABLE " + _config.targetTable
            + " ENGINE=MyISAM " + mergeSelect;
        LOGS(_log, LOG_LVL_DEBUG, "Merging w/" << createMerge);
        finalizeOk = _applySqlLocal(createMerge, "createMerge");

        // Cleanup merge table.
        sql::SqlErrorObject eObj;
        // Don't report failure on not exist
        LOGS(_log, LOG_LVL_DEBUG, "Cleaning up " << _mergeTable);
#if 1 // Set to 0 when we want to retain mergeTables for debugging.
        bool cleanupOk = _sqlConn->dropTable(_mergeTable, eObj,
                                             false,
                                             _config.mySqlConfig.dbName);
#else
        bool cleanupOk = true;
#endif
        if (!cleanupOk) {
            LOGS(_log, LOG_LVL_DEBUG, "Failure cleaning up table " << _mergeTable);
        }
    } else {
        // Remove jobId and attemptCount information from the result table.
        // Returning a view could be faster, but is more complicated.
        std::string sqlDropCol = std::string("ALTER TABLE ") + _mergeTable
                               + " DROP COLUMN " +  _jobIdColName;
        LOGS(_log, LOG_LVL_DEBUG, "Removing w/" << sqlDropCol);
        finalizeOk = _applySqlLocal(sqlDropCol, "dropCol Removing");
    }
    LOGS(_log, LOG_LVL_DEBUG, "Merged " << _mergeTable << " into " << _config.targetTable);
    _isFinished = true;
    return finalizeOk;
}

bool InfileMerger::isFinished() const {
    return _isFinished;
}

#if 0 // &&&
bool InfileMerger::_deleteInvalidRows(int jobIdAttempt) {
    std::string sqlDelRows = std::string("DELETE FROM ") + _mergeTable
                             + " WHERE " +  _jobIdColName + "=" + std::to_string(jobIdAttempt);
    LOGS(_log, LOG_LVL_DEBUG, "Deleting invalid rows w/" << sqlDelRows);
    bool ok = _applySqlLocal(sqlDelRows, "deleteInvalidRows");
    if (!ok) {
        LOGS(_log, LOG_LVL_ERROR, "Failed to drop columns w/" << sqlDelRows);
        return false;
    }
    return true;
}
#endif

bool InfileMerger::_deleteInvalidRows(std::set<int> const& jobIdAttempts) {
    // delete several rows at a time
    int maxRows = 5000;
    auto iter = jobIdAttempts.begin();
    auto end = jobIdAttempts.end();
    while (iter != end) {
        int j = 0;
        bool first = true;
        std::string invalidStr;
        for (int j=0; j < maxRows && iter != end; ++j) {
            if (!first) {
                invalidStr += ",";
            }
            invalidStr += *iter;
            ++iter;
        }
        std::string sqlDelRows = std::string("DELETE FROM ") + _mergeTable
                + " WHERE " + _jobIdColName + " IN (" + invalidStr + ")";
        bool ok = _applySqlLocal(sqlDelRows, "deleteInvalidRows");
        if (!ok) {
             LOGS(_log, LOG_LVL_ERROR, "Failed to drop columns w/" << sqlDelRows);
             return false;
         }
    }
    return true;
}


int InfileMerger::makeJobIdAttempt(int jobId, int attemptCount) {
    int jobIdAttempt = jobId * MAX_JOB_ATTEMPTS;
    if (attemptCount >= MAX_JOB_ATTEMPTS) {
        std::string msg = _queryIdStr + " jobId=" + std::to_string(jobId)
                + " Canceling query attemptCount too large at " + std::to_string(attemptCount);
        LOGS(_log, LOG_LVL_ERROR, msg);
        throw Bug(msg);
    }
    jobIdAttempt += attemptCount;
    return jobIdAttempt;
}


bool InfileMerger::scrubResults(int jobId, int attemptCount) {
    int jobIdAttempt = makeJobIdAttempt(jobId, attemptCount);
    return _invalidJobAttemptMgr.holdMergingForRowDelete(jobIdAttempt); // &&& create new func to do actual delete, call this scrubPrep
}


bool InfileMerger::_applySqlLocal(std::string const& sql, std::string const& logMsg) {
    auto begin = std::chrono::system_clock::now();
    bool success = _applySqlLocal(sql);
    auto end = std::chrono::system_clock::now();
    LOGS(_log, LOG_LVL_DEBUG, logMsg << " success=" << success
         << " microseconds="
         << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count());
    return success;
}


/// Apply a SQL query, setting the appropriate error upon failure.
bool InfileMerger::_applySqlLocal(std::string const& sql) {
    std::lock_guard<std::mutex> m(_sqlMutex);
    sql::SqlErrorObject errObj;

    if (not _sqlConnect(errObj)) {
        return false;
    }
    if (not _sqlConn->runQuery(sql, errObj)) {
        _error = util::Error(errObj.errNo(), "Error applying sql: " + errObj.printErrMsg(),
                       util::ErrorCode::MYSQLEXEC);
        LOGS(_log, LOG_LVL_ERROR, "InfileMerger error: " << _error.getMsg());
        return false;
    }
    LOGS(_log, LOG_LVL_DEBUG, "InfileMerger query success: " << sql);
    return true;
}


bool InfileMerger::_sqlConnect(sql::SqlErrorObject& errObj) {
    if (_sqlConn == nullptr) {
        _sqlConn = std::make_shared<sql::SqlConnection>(_config.mySqlConfig, true);
        if (not _sqlConn->connectToDb(errObj)) {
            _error = util::Error(errObj.errNo(), "Error connecting to db: " + errObj.printErrMsg(),
                           util::ErrorCode::MYSQLCONNECT);
            _sqlConn.reset();
            LOGS(_log, LOG_LVL_ERROR, "InfileMerger error: " << _error.getMsg());
            return false;
        }
        LOGS(_log, LOG_LVL_DEBUG, "InfileMerger " << (void*) this << " connected to db");
    }
    return true;
}


size_t InfileMerger::_getResultTableSizeMB() {
    std::string tableSizeSql = std::string("SELECT table_name, ")
                             + "round(((data_length + index_length) / 1048576), 2) as 'MB' "
                             + "FROM information_schema.TABLES "
                             + "WHERE table_schema = '" + _config.mySqlConfig.dbName
                             + "' AND table_name = '" + _mergeTable + "'";
    LOGS(_log, LOG_LVL_DEBUG, "Checking ResultTableSize " << tableSizeSql);
    std::lock_guard<std::mutex> m(_sqlMutex);
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    if (not _sqlConnect(errObj)) {
        return 0;
    }
    if (not _sqlConn->runQuery(tableSizeSql, results, errObj)) {
        _error = util::Error(errObj.errNo(), "error getting size sql: " + errObj.printErrMsg(),
                       util::ErrorCode::MYSQLEXEC);
        LOGS(_log, LOG_LVL_ERROR, _getQueryIdStr() << "result table size error: " << _error.getMsg());
        return 0;
    }

    // There should only be 1 row
    auto iter = results.begin();
    if (iter == results.end()) {
        LOGS(_log, LOG_LVL_ERROR, _getQueryIdStr() << " result table size no rows returned " << _mergeTable);
        return 0;
    }
    auto& row = *iter;
    std::string tbName = row[0].first;
    std::string tbSize = row[1].first;
    size_t sz = std::stoul(tbSize);
    LOGS(_log, LOG_LVL_DEBUG,
         _getQueryIdStr() << " ResultTableSizeMB tbl=" << tbName << " tbSize=" << tbSize);
    return sz;
}


/// Read a ProtoHeader message from a buffer and return the number of bytes
/// consumed.
int InfileMerger::_readHeader(proto::ProtoHeader& header, char const* buffer, int length) {
    if (not proto::ProtoImporter<proto::ProtoHeader>::setMsgFrom(header, buffer, length)) {
        // This is only a real error if there are no more bytes.
        _error = InfileMergerError(util::ErrorCode::HEADER_IMPORT,
                                   _getQueryIdStr() + " Error decoding protobuf header");
        return 0;
    }
    return length;
}

/// Read a Result message and return the number of bytes consumed.
int InfileMerger::_readResult(proto::Result& result, char const* buffer, int length) {
    if (not proto::ProtoImporter<proto::Result>::setMsgFrom(result, buffer, length)) {
        _error = InfileMergerError(util::ErrorCode::RESULT_IMPORT,
                                   _getQueryIdStr() + "Error decoding result message");
        throw _error;
    }
    // result.PrintDebugString();
    return length;
}

/// Verify that the sessionId is the same as what we were expecting.
/// This is an additional safety check to protect from importing a message from
/// another session.
/// TODO: this is incomplete.
bool InfileMerger::_verifySession(int sessionId) {
    if (false) {
        _error = InfileMergerError(util::ErrorCode::RESULT_IMPORT, "Session id mismatch");
    }
    return true; // TODO: for better message integrity
}


/// Create a table with the appropriate schema according to the
/// supplied Protobufs message
bool InfileMerger::_setupTable(proto::WorkerResponse const& response) {
    // Create table, using schema
    std::lock_guard<std::mutex> lock(_createTableMutex);
    if (_needCreateTable) {
        // create schema
        proto::RowSchema const& rs = response.result.rowschema();
        sql::Schema sch;
        for(int i=0, e=rs.columnschema_size(); i != e; ++i) {
            proto::ColumnSchema const& cs = rs.columnschema(i);
            sql::ColSchema scs;
            scs.name = cs.name();
            if (cs.hasdefault()) {
                scs.defaultValue = cs.defaultvalue();
                scs.hasDefault = true;
            } else {
                scs.hasDefault = false;
            }
            if (cs.has_mysqltype()) {
                scs.colType.mysqlType = cs.mysqltype();
            }
            scs.colType.sqlType = cs.sqltype();

            sch.columns.push_back(scs);
        }
        // Add jobId column that does not conflict with existing columns.
        for (auto iter = sch.columns.begin(), end = sch.columns.end(); iter != end; ++iter) {
            auto const& col = *iter;
            if (col.name == _jobIdColName) {
                _alterJobIdColName();
                iter = sch.columns.begin(); // start over
            }
        }


        sql::Schema schema;
        {
            sql::ColSchema scs;
            scs.name              = _jobIdColName;
            scs.hasDefault        = false;
            scs.colType.mysqlType = _jobIdMysqlType;
            scs.colType.sqlType   = _jobIdSqlType;
            schema.columns.push_back(scs);
            schema.columns.insert(schema.columns.end(), sch.columns.begin(), sch.columns.end());
        }
        std::string createStmt = sql::formCreateTable(_mergeTable, schema);
        // Specifying engine. There is some question about whether InnoDB or MyISAM is the better
        // choice when multiple threads are writing to the result table.
        createStmt += " ENGINE=MyISAM";
        LOGS(_log, LOG_LVL_DEBUG, _getQueryIdStr() << "InfileMerger query prepared: " << createStmt);

        if (not _applySqlLocal(createStmt, "setupTable")) {
            _error = InfileMergerError(util::ErrorCode::CREATE_TABLE,
                                       "Error creating table (" + _mergeTable + ")");
            _isFinished = true; // Cannot continue.
            LOGS(_log, LOG_LVL_ERROR, _getQueryIdStr() << "InfileMerger sql error: " << _error.getMsg());
            return false;
        }
        _needCreateTable = false;
    } else {
        // Do nothing, table already created.
    }
    LOGS(_log, LOG_LVL_DEBUG, _getQueryIdStr() << "InfileMerger table " << _mergeTable << " ready");
    return true;
}


/// Choose the appropriate target name, depending on whether post-processing is
/// needed on the result rows.
void InfileMerger::_fixupTargetName() {
    if (_config.targetTable.empty()) {
        assert(not _config.mySqlConfig.dbName.empty());
        _config.targetTable = (boost::format("%1%.result_%2%")
                               % _config.mySqlConfig.dbName % getTimeStampId()).str();
    }

    if (_config.mergeStmt) {
        // Set merging temporary if needed.
        _mergeTable = _config.targetTable + "_m";
    } else {
        _mergeTable = _config.targetTable;
    }
}


bool InvalidJobAttemptMgr::incrConcurrentMergeCount(int jobIdAttempt) {
    std::unique_lock<std::mutex> uLock(_iJAMtx);
    if (_isJobAttemptInvalid(jobIdAttempt)) {
        LOGS(_log, LOG_LVL_INFO, jobIdAttempt << " invalid, not merging");
        return true;
    }
    if (_waitFlag) {
        /// wait for flag to clear
        _cv.wait(uLock, [this](){ return !_waitFlag; });
        // Since wait lets the mutex go, this must be checked again.
        if (_isJobAttemptInvalid(jobIdAttempt)) {
            LOGS(_log, LOG_LVL_INFO, jobIdAttempt << " invalid after wait, not merging");
            return true;
        }
    }
    _jobIdAttemptsHaveRows.insert(jobIdAttempt);
    ++_concurrentMergeCount;
    // No rows can be deleted until after decrConcurrentMergeCount() is called, which
    // should ensure that all rows added for this job attempt can be deleted by
    // calls to holdMergingForRowDelete() if needed.
    return false;
}


void InvalidJobAttemptMgr::decrConcurrentMergeCount() {
    std::lock_guard<std::mutex> uLock(_iJAMtx);
    --_concurrentMergeCount;
    assert(_concurrentMergeCount >= 0);
    if (_concurrentMergeCount == 0) {
        // Notify any threads waiting that no merging is occurring.
        _cv.notify_all();
    }
}


bool InvalidJobAttemptMgr::prepScrub(int jobId, int attemptCount) {
    int jobIdAttempt = makeJobIdAttempt(jobId, attemptCount);
    std::unique_lock<std::mutex> lockJA(_iJAMtx);
    _waitFlag = true;
    _invalidJobAttempts.insert(jobIdAttempt);
    bool invalidRowsInResult = false
    if (_jobIdAttemptsHaveRows.find(jobIdAttempt) != _jobIdAttemptsHaveRows.end()) {
        invalidRowsInResult = true;
        _invalidJAWithRows.insert(jobIdAttempt);
    }
    _cleanupIJA();
    return invalidRowsInResult;
}


void InvalidJobAttemptMgr::_cleanupIJA() {
    _waitFlag = false;
    _cv.notify_all();
}

#if 0 // &&&
bool InvalidJobAttemptMgr::holdMergingForRowDelete(int jobIdAttempt) {
    // &&& auto cleanup = [this](){
    // &&&     _waitFlag = false;
    // &&&     _cv.notify_all();
    // &&& };

    std::unique_lock<std::mutex> lockJA(_iJAMtx);
    _waitFlag = true;
    // Prevent rows belonging to jobIdAttempt from being added to the table.
    _invalidJobAttempts.insert(jobIdAttempt);

    // If this jobAttempt hasn't had any rows added, no need to delete rows.
    if (_jobIdAttemptsHaveRows.find(jobIdAttempt) == _jobIdAttemptsHaveRows.end()) {
        LOGS(_log, LOG_LVL_INFO, jobIdAttempt << " should not have any rows, not deleting.");
        _cleanupIJA();
        return true;
    }
    lockJA.unlock();
    /// If the table hasn't been made yet, just return true, nothing to remove.
    /// Rows with jobIdAttempt should be prevented from joining the result table.
    if (!_tableExistsFunc()) {
        LOGS(_log, LOG_LVL_INFO, "Nothing to do as no table yet made for " << jobIdAttempt);
        _cleanupIJA();
        return true;
    }

    lockJA.lock();
    if (_concurrentMergeCount > 0) {
        _cv.wait(lockJA, [this](){ return _concurrentMergeCount == 0;});
    }

    LOGS(_log, LOG_LVL_INFO, "Deleting rows for " << jobIdAttempt);
    bool res = _deleteFunc(jobIdAttempt);
    // Table scrubbed, continue merging results.
    _cleanupIJA();
    return res;
}
#endif


bool InvalidJobAttemptMgr::holdMergingForRowDelete(std::string const& msg) {
    // &&& auto cleanup = [this](){
    // &&&     _waitFlag = false;
    // &&&     _cv.notify_all();
    // &&& };

    std::unique_lock<std::mutex> lockJA(_iJAMtx);
    _waitFlag = true;


    // If this jobAttempt hasn't had any rows added, no need to delete rows.
    // if (!_invalidRowsInResult) { &&&
    if (_invalidJAWithRows.empty()) {
        LOGS(_log, LOG_LVL_INFO, msg << " should not have any invalid rows, no delete needed.");
        _cleanupIJA();
        return true;
    }
    lockJA.unlock();
    /// If the table hasn't been made yet, just return true, nothing to remove.
    /// Rows with jobIdAttempt should be prevented from joining the result table.
    if (!_tableExistsFunc()) {
        LOGS(_log, LOG_LVL_INFO, msg << " Nothing to do as no table yet made");
        _cleanupIJA();
        return true;
    }

    lockJA.lock();
    if (_concurrentMergeCount > 0) {
        _cv.wait(lockJA, [this](){ return _concurrentMergeCount == 0;});
    }

    LOGS(_log, LOG_LVL_INFO, "Deleting rows for " << util::printable(_invalidJobAttempts));
    bool res = _deleteFunc(_invalidJAWithRows);
    // Table scrubbed, continue merging results.
    _cleanupIJA();
    return res;
}


bool InvalidJobAttemptMgr::isJobAttemptInvalid(int jobIdAttempt) {
    // Return true if jobIdAttempt is in the invalid set.
    std::lock_guard<std::mutex> iJALock(_iJAMtx);
    return _isJobAttemptInvalid(jobIdAttempt);
}


/// Precondition, must hold _iJAMtx.
bool InvalidJobAttemptMgr::_isJobAttemptInvalid(int jobIdAttempt) {
    return _invalidJobAttempts.find(jobIdAttempt) != _invalidJobAttempts.end();
}


}}} // namespace lsst::qserv::rproc
