// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#include "rproc/InfileMerger.h"

// System headers
#include <iostream>
#include <sstream>
#include <sys/time.h>

// Third-party headers
#include "boost/format.hpp"
#include "boost/regex.hpp"

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "mysql/LocalInfile.h"
#include "mysql/MySqlConnection.h"
#include "proto/WorkerResponse.h"
#include "proto/ProtoImporter.h"
#include "query/SelectStmt.h"
#include "rproc/ProtoRowBuffer.h"
#include "rproc/SqlInsertIter.h"
#include "sql/Schema.h"
#include "sql/SqlConnection.h"
#include "sql/SqlResults.h"
#include "sql/SqlErrorObject.h"
#include "sql/statement.h"
#include "util/MmapFile.h"
#include "util/StringHash.h"
#include "util/WorkQueue.h"


namespace { // File-scope helpers

using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::proto::WorkerResponse;
using lsst::qserv::rproc::InfileMergerConfig;
using lsst::qserv::rproc::InfileMergerError;

/// @return a timestamp id for use in generating temporary result table names.
std::string getTimeStampId() {
    struct timeval now;
    int rc = gettimeofday(&now, NULL);
    if (rc != 0) {
        throw InfileMergerError(InfileMergerError::INTERNAL,
                                "Failed to get timestamp.");
    }
    std::ostringstream s;
    s << (now.tv_sec % 10000) << now.tv_usec;
    return s.str();
    // Use the lower digits as pseudo-unique (usec, sec % 10000)
    // Alternative (for production?) Use boost::uuid to construct ids that are
    // guaranteed to be unique.
}

boost::shared_ptr<MySqlConfig> makeSqlConfig(InfileMergerConfig const& c) {
    boost::shared_ptr<MySqlConfig> sc(new MySqlConfig());
    assert(sc.get());
    sc->username = c.user;
    sc->dbName = c.targetDb;
    sc->socket = c.socket;
    return sc;
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace rproc {

// InfileMergerError
/// @return true if the error represents a results-too-big condition
bool InfileMergerError::resultTooBig() const {
    return (status == MYSQLEXEC) && (errorCode == 1114);
}

////////////////////////////////////////////////////////////////////////
// InfileMerger::Mgr
////////////////////////////////////////////////////////////////////////

/// InfileMerger::Mgr is a delegate class of InfileMerger that manages a queue
/// of jobs to import rows into a mysqld. The purpose of maintaining a queue and
/// not performing parallel loading is poor performance (as measured in MySQL
/// 5.1). While performance might be better when the merge/result table is an
/// ENGINE=MEMORY table, we cannot use in-memory by default because result
/// tables could spill physical RAM--baseline LSST query requirements allow for
/// large result sets.
class InfileMerger::Mgr {
public:
    class Action;
    friend class Action;

    Mgr(MySqlConfig const& config, std::string const& mergeTable);

    ~Mgr() {}

    /// Enqueue an bundle of result rows to be inserted, as represented by the
    /// Protobufs message bundle
    void enqueueAction(boost::shared_ptr<WorkerResponse> response);

    /// Wait until work queue is empty.
    /// @return true on success
    bool join() {
        boost::unique_lock<boost::mutex> lock(_inflightMutex);
        while(_numInflight > 0) {
            _inflightZero.wait(lock);
        }
        return true;
    }

    /// Apply a mysql query, using synchronization to prevent contention.
    bool applyMysql(std::string const& query);

    /// Report completion of an action (used by Action threads to report their
    /// completion before they destroy themselves).
    void signalDone(bool success, Action& a) {
        boost::lock_guard<boost::mutex> lock(_inflightMutex);
        --_numInflight;
        // TODO: do something with the result so we can catch errors.
        if(_numInflight == 0) {
            _inflightZero.notify_all();
        }
    }

private:
    void _incrementInflight() {
        boost::lock_guard<boost::mutex> lock(_inflightMutex);
        ++_numInflight;
    }

    bool _setupConnection() {
        if(_mysqlConn.connect()) {
            _infileMgr.attach(_mysqlConn.getMySql());
            return true;
        }
        return false;
    }

    mysql::MySqlConnection _mysqlConn;
    boost::mutex _mysqlMutex;
    std::string const& _mergeTable;

    util::WorkQueue _workQueue;
    boost::mutex _inflightMutex;
    boost::condition_variable _inflightZero;
    int _numInflight;

    lsst::qserv::mysql::LocalInfile::Mgr _infileMgr;
};

class InfileMerger::Mgr::Action : public util::WorkQueue::Callable {
public:
    Action(Mgr& mgr, boost::shared_ptr<WorkerResponse> response,
           std::string const& table)
        : _mgr(mgr), _response(response), _table(table) {
        mgr._incrementInflight();
        // Delay preparing the virtual file until just before it is needed.
    }
    void operator()() {
        _virtFile = _mgr._infileMgr.prepareSrc(
            rproc::newProtoRowBuffer(_response->result));

        // load data infile.
        std::string infileStatement = sql::formLoadInfile(_table, _virtFile);
        bool result = _mgr.applyMysql(infileStatement);
        assert(result);
        _mgr.signalDone(result, *this);
    }
    Mgr& _mgr;
    boost::shared_ptr<WorkerResponse> _response;
    std::string _table;
    std::string _virtFile;
};

////////////////////////////////////////////////////////////////////////
// InfileMerger::Mgr implementation
////////////////////////////////////////////////////////////////////////
InfileMerger::Mgr::Mgr(MySqlConfig const& config, std::string const& mergeTable)
    : _mysqlConn(config, true),
      _mergeTable(mergeTable),
      _workQueue(1),
      _numInflight(0) {
    if(!_setupConnection()) {
        throw InfileMergerError(InfileMergerError::MYSQLCONNECT);
    }
}

void InfileMerger::Mgr::enqueueAction(boost::shared_ptr<WorkerResponse> response) {
    boost::shared_ptr<Action> a(new Action(*this, response, _mergeTable));
    _workQueue.add(a);
}

bool InfileMerger::Mgr::applyMysql(std::string const& query) {
    boost::lock_guard<boost::mutex> lock(_mysqlMutex);
    if(!_mysqlConn.connected()) {
        // should have connected during Mgr construction
        // Try reconnecting--maybe we timed out.
        if(!_setupConnection()) {
            return false; // Reconnection failed. This is an error.
        }
    }
    // Go direct--MySqlConnection API expects results and will report
    // an error if there is no result.
    // bool result = _mysqlConn.queryUnbuffered(query);  // expects a result
    int rc = mysql_real_query(_mysqlConn.getMySql(),
                              query.data(), query.size());
    return rc == 0;
}

////////////////////////////////////////////////////////////////////////
// InfileMerger public
////////////////////////////////////////////////////////////////////////
InfileMerger::InfileMerger(InfileMergerConfig const& c)
    : _config(c),
      _sqlConfig(makeSqlConfig(c)),
      _isFinished(false),
      _needCreateTable(true) {
    //LOGF_INFO("InfileMerger construct +++++++ ()%1%" % (void*) this");
    _error.errorCode = InfileMergerError::NONE;
    _fixupTargetName();
    if(_config.mergeStmt) {
        _config.mergeStmt->setFromListAsTable(_mergeTable);
    }
    _mgr.reset(new Mgr(*_sqlConfig, _mergeTable));
}

InfileMerger::~InfileMerger() {
    //LOGF_INFO("InfileMerger destruct ------- ()%1%" % (void*) this);
}


off_t InfileMerger::merge(char const* dumpBuffer, int dumpLength) {
    if(_error.errorCode) { // Do not attempt when in an error state.
        return -1;
    }
    LOGF_DEBUG("EXECUTING InfileMerger::merge(%1%, %2%)"
               % (void*)dumpBuffer % dumpLength);
    int mergeSize = 0;
    // Now buffer is big enough, start processing.
    mergeSize = _importBuffer(dumpBuffer, dumpLength, _needCreateTable);
    if(mergeSize == 0) { // Buffer not big enough.
        LOGF_DEBUG("WARNING, zero merge from InfileMerger::merge(%1%, %2%)"
                   % (void*)dumpBuffer % dumpLength);
    }
    return mergeSize;
}

bool InfileMerger::merge(boost::shared_ptr<proto::WorkerResponse> response) {
    if(_needCreateTable) {
        if(!_setupTable(*response)) {
            return false;
        }
    }
    return _importResponse(response);
}

bool InfileMerger::finalize() {
    bool finalizeOk = _mgr->join();
    // TODO: Should check for error condition before continuing.
    if(_isFinished) {
        LOGF_ERROR("InfileMerger::finalize(), but _isFinished == true");
    }
    if(_mergeTable != _config.targetTable) {
        // Aggregation needed: Do the aggregation.
        std::string mergeSelect = _config.mergeStmt->getTemplate().generate();
        std::string createMerge = "CREATE TABLE " + _config.targetTable
            + " " + mergeSelect;
        LOGF_INFO("Merging w/%1%" % createMerge);
        finalizeOk = _applySqlLocal(createMerge);

        // Cleanup merge table.
        sql::SqlErrorObject eObj;
        // Don't report failure on not exist
        LOGF_INFO("Cleaning up %1%" % _mergeTable);
#if 1 // Set to 0 when we want to retain mergeTables for debugging.
        bool cleanupOk = _sqlConn->dropTable(_mergeTable, eObj,
                                             false,
                                             _config.targetDb);
#else
        bool cleanupOk = true;
#endif
        if(!cleanupOk) {
            LOGF_INFO("Failure cleaning up table %1%" % _mergeTable);
        }
    }
    LOGF_INFO("Merged %1% into %2%" % _mergeTable % _config.targetTable);
    _isFinished = true;
    return finalizeOk;
}

bool InfileMerger::isFinished() const {
    return _isFinished;
}

////////////////////////////////////////////////////////////////////////
// InfileMerger private
////////////////////////////////////////////////////////////////////////

/// Apply a SQL query, setting the appropriate error upon failure.
bool InfileMerger::_applySqlLocal(std::string const& sql) {
    boost::lock_guard<boost::mutex> m(_sqlMutex);
    sql::SqlErrorObject errObj;
    if(!_sqlConn.get()) {
        _sqlConn.reset(new sql::SqlConnection(*_sqlConfig, true));
        if(!_sqlConn->connectToDb(errObj)) {
            _error.status = InfileMergerError::MYSQLCONNECT;
            _error.errorCode = errObj.errNo();
            _error.description = "Error connecting to db. " + errObj.printErrMsg();
            _sqlConn.reset();
            return false;
        } else {
            LOGF_INFO("InfileMerger %1% connected to db." % (void*) this);
        }
    }
    if(!_sqlConn->runQuery(sql, errObj)) {
        _error.status = InfileMergerError::MYSQLEXEC;
        _error.errorCode = errObj.errNo();
        _error.description = "Error applying sql. " + errObj.printErrMsg();
        return false;
    }
    return true;
}

/// Read a ProtoHeader message from a buffer and return the number of bytes
/// consumed.
int InfileMerger::_readHeader(ProtoHeader& header, char const* buffer, int length) {
    if(!ProtoImporter<ProtoHeader>::setMsgFrom(header, buffer, length)) {
        _error.errorCode = InfileMergerError::HEADER_IMPORT;
        _error.description = "Error decoding proto header";
        // This is only a real error if there are no more bytes.
        return 0;
    }
    return length;
}

/// Read a Result message and return the number of bytes consumed.
int InfileMerger::_readResult(proto::Result& result, char const* buffer, int length) {
    if(!ProtoImporter<proto::Result>::setMsgFrom(result, buffer, length)) {
        _error.errorCode = InfileMergerError::RESULT_IMPORT;
        _error.description = "Error decoding result msg";
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
    if(false) {
        _error.errorCode = InfileMergerError::RESULT_IMPORT;
        _error.description = "session id mismatch";
    }
    return true; // TODO: for better message integrity
}

/// Verify the expected MD5 against the actual computed MD5, returning false
/// and setting the error code if they mismatch.
bool InfileMerger::_verifyMd5(std::string const& expected, std::string const& actual) {
    if(expected != actual) {
        _error.description = "Result message MD5 mismatch";
        _error.errorCode = InfileMergerError::RESULT_MD5;
        return false;
    }
    return true;
}

/// Import a buffer passed in by client code.
/// @param setupResult : true to hint that a table should be created.
int InfileMerger::_importBuffer(char const* buffer, int length, bool setupResult) {
    boost::shared_ptr<WorkerResponse> response(new WorkerResponse);
    // First char: sizeof protoheader. always less than 255 char.
    response->headerSize = *reinterpret_cast<unsigned char const*>(buffer);
    // Advance cursor to point after length
    char const* cursor = buffer + 1;
    int remain = length - 1;

    int headerRead =_readHeader(response->protoHeader,
                                cursor, response->headerSize);
    if(headerRead != response->headerSize) {
        return 0;
    }
    cursor += response->headerSize; // Advance to Result msg
    remain -= response->headerSize;

    // Now decode Result msg
    int resultSize = response->protoHeader.size();
    LOGF_INFO("Importing result msg size=%1%" % response->protoHeader.size());
    if(remain < response->protoHeader.size()) {
        // TODO: want to handle bigger messages.
        _error.description = "Buffer too small for result msg, increase buffer size in InfileMerger";
        _error.errorCode = InfileMergerError::HEADER_OVERFLOW;
        return 0;
    }
    if(resultSize != _readResult(response->result, cursor, resultSize)) {
        return 0;
    }
    remain -= resultSize;
    if(!_verifySession(response->result.session())) {
        return 0;
    }
    // Potentially, we can skip verifying the hash if the result reports
    // no rows, but if the hash disagrees, then the schema is suspect as well.
    if(!_verifyMd5(response->protoHeader.md5(),
                   util::StringHash::getMd5(cursor, resultSize))) {
        return -1;
    }
    if(setupResult) {
        if(!_setupTable(*response)) {
            return -1;
        }
    }
    if(!_importResponse(response)) {
        return -1;
    }
    return length;
}

bool InfileMerger::_importResponse(boost::shared_ptr<WorkerResponse> response) {
    // Check for the no-row condition
    if(response->result.row_size() == 0) {
        // Nothing further, don't bother importing
    } else {
        // Delegate merging thread mgmt to mgr, which will handle
        // LocalInfile objects
        _mgr->enqueueAction(response);
    }
    return true;
}

/// Create a table with the appropriate schema according to the
/// supplied Protobufs message
bool InfileMerger::_setupTable(WorkerResponse const& response) {
    // Create table, using schema
    boost::lock_guard<boost::mutex> lock(_createTableMutex);
    if(_needCreateTable) {
        // create schema
        proto::RowSchema const& rs = response.result.rowschema();
        sql::Schema s;
        for(int i=0, e=rs.columnschema_size(); i != e; ++i) {
            proto::ColumnSchema const& cs = rs.columnschema(i);
            sql::ColSchema scs;
            scs.name = cs.name();
            if(cs.hasdefault()) {
                scs.defaultValue = cs.defaultvalue();
                scs.hasDefault = true;
            } else {
                scs.hasDefault = false;
            }
            if(cs.has_mysqltype()) {
                scs.colType.mysqlType = cs.mysqltype();
            }
            scs.colType.sqlType = cs.sqltype();

            s.columns.push_back(scs);
        }
        std::string createStmt = sql::formCreateTable(_mergeTable, s);
        LOGF_DEBUG("InfileMerger create table:%1%" % createStmt);

        if(!_applySqlLocal(createStmt)) {
            _error.errorCode = InfileMergerError::CREATE_TABLE;
            _error.description = "Error creating table (" + _mergeTable + ")";
            _isFinished = true; // Cannot continue.
            return false;
        }
        _needCreateTable = false;
    } else {
        // Do nothing, table already created.
    }
    return true;
}

/// Choose the appropriate target name, depending on whether post-processing is
/// needed on the result rows.
void InfileMerger::_fixupTargetName() {
    if(_config.targetTable.empty()) {
        assert(!_config.targetDb.empty());
        _config.targetTable = (boost::format("%1%.result_%2%")
                               % _config.targetDb % getTimeStampId()).str();
    }

    if(_config.mergeStmt) {
        // Set merging temporary if needed.
        _mergeTable = _config.targetTable + "_m";
    } else {
        _mergeTable = _config.targetTable;
    }
}
}}} // namespace lsst::qserv::rproc
