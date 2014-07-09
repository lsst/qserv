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
#include <boost/format.hpp>
#include <boost/regex.hpp>

// Local headers
#include "log/Logger.h"
#include "mysql/LocalInfile.h"
#include "mysql/MySqlConnection.h"
#include "proto/worker.pb.h"
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


namespace { // File-scope helpers

using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::proto::ProtoHeader;
using lsst::qserv::proto::ProtoImporter;
using lsst::qserv::rproc::InfileMergerConfig;

std::string getTimeStampId() {
    struct timeval now;
    int rc = gettimeofday(&now, NULL);
    if (rc != 0) throw "Failed to get timestamp.";
    std::stringstream s;
    s << (now.tv_sec % 10000) << now.tv_usec;
    return s.str();
    // Use the lower digits as pseudo-unique (usec, sec % 10000)
    // FIXME: is there a better idea?
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

////////////////////////////////////////////////////////////////////////
// InfileMergerError
////////////////////////////////////////////////////////////////////////
bool InfileMergerError::resultTooBig() const {
    return (status == MYSQLEXEC) && (errorCode == 1114);
}

////////////////////////////////////////////////////////////////////////
// InfileMerger::Msgs
////////////////////////////////////////////////////////////////////////
class InfileMerger::Msgs {
public:
    lsst::qserv::proto::ProtoHeader protoHeader;
    lsst::qserv::proto::Result result;
};

////////////////////////////////////////////////////////////////////////
// InfileMerger::Mgr
////////////////////////////////////////////////////////////////////////
class InfileMerger::Mgr {
public:
    class Action;
    friend class Action;

    Mgr(MySqlConfig const& config);

    /// Should takes ownership of msgs.
    Action newAction(std::string const& table, boost::shared_ptr<Msgs> msgs);
    bool applyMysql(std::string const& query);

    void signalDone() {
        boost::lock_guard<boost::mutex> lock(_inflightMutex);
        --_numInflight;
    }

private:
    inline void _incrementInflight() {
        boost::lock_guard<boost::mutex> lock(_inflightMutex);
        ++_numInflight;
    }

    mysql::MySqlConnection _mysqlConn;
    lsst::qserv::mysql::LocalInfile::Mgr _infileMgr;
    boost::mutex _inflightMutex;
    boost::mutex _mysqlMutex;
    int _numInflight;
};

class InfileMerger::Mgr::Action {
public:
    Action(Mgr& mgr,
           boost::shared_ptr<Msgs> msgs,
           std::string const& table, std::string const& virtFile)
        : _mgr(mgr), _msgs(msgs), _table(table), _virtFile(virtFile) {
    }
    Action(Mgr& mgr, boost::shared_ptr<Msgs> msgs, std::string const& table)
        : _mgr(mgr), _msgs(msgs), _table(table) {
        _virtFile = mgr._infileMgr.prepareSrc(rproc::newProtoRowBuffer(msgs->result));
        mgr._incrementInflight();
    }
    void operator()() {
        // load data infile.
        std::string infileStatement = sql::formLoadInfile(_table, _virtFile);
        bool result = _mgr.applyMysql(infileStatement);
        assert(result);
        // TODO: do something with the result so we can catch errors.
    }
    Mgr& _mgr;
    boost::shared_ptr<Msgs> _msgs;
    std::string _table;
    std::string _virtFile;
};

////////////////////////////////////////////////////////////////////////
// InfileMerger::Mgr implementation
////////////////////////////////////////////////////////////////////////
InfileMerger::Mgr::Mgr(MySqlConfig const& config)
    : _mysqlConn(config, true),
      _numInflight(0) {
    if(_mysqlConn.connect()) {
        _infileMgr.attach(_mysqlConn.getMySql());
    } else {
        throw InfileMergerError(InfileMergerError::MYSQLCONNECT);
    }
}

bool InfileMerger::Mgr::applyMysql(std::string const& query) {
    boost::lock_guard<boost::mutex> lock(_mysqlMutex);
    if(!_mysqlConn.connected()) {
        return false; // should have connected during Mgr construction
        // Maybe we should reconnect?
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
      _tableCount(0),
      _isFinished(false),
      _mgr(new Mgr(*_sqlConfig)),
      _needCreateTable(true),
      _needHeader(true) {

    _error.errorCode = InfileMergerError::NONE;
    _fixupTargetName();
    if(_config.mergeStmt) {
        _config.mergeStmt->setFromListAsTable(_mergeTable);
    }
}

int InfileMerger::_fetchHeader(char const* buffer, int length) {
    // First char: sizeof protoheader. always less than 255 char.
    unsigned char phSize = *reinterpret_cast<unsigned char const*>(buffer);
    // Advance cursor to point after length
    char const* cursor = buffer + 1;
    int remain = length - 1;
    boost::shared_ptr<InfileMerger::Msgs> msgs(new InfileMerger::Msgs);

    if(!ProtoImporter<ProtoHeader>::setMsgFrom(msgs->protoHeader,
                                               cursor, phSize)) {
        _error.errorCode = InfileMergerError::HEADER_IMPORT;
        _error.description = "Error decoding proto header";
        // This is only a real error if there are no more bytes.
        return 0;
    }
    cursor += phSize; // Advance to Result msg
    remain -= phSize;
    if(remain < msgs->protoHeader.size()) {
        // TODO: want to handle bigger messages.
        _error.description = "Buffer too small for result msg, increase buffer size in InfileMerger";
        _error.errorCode = InfileMergerError::HEADER_OVERFLOW;
        return 0;
    }
    // Now decode Result msg
    int resultSize = msgs->protoHeader.size();
    LOGGER_INF << "Importing result msg size=" << msgs->protoHeader.size();

    if(!ProtoImporter<proto::Result>::setMsgFrom(msgs->result,
                                                 cursor, resultSize)) {
        _error.errorCode = InfileMergerError::RESULT_IMPORT;
        _error.description = "Error decoding result msg";
        throw _error;
    }
    remain -= resultSize;
    //_msgs->result.PrintDebugString(); // wait a second.
    // doublecheck session
    msgs->result.session(); // TODO
    _setupTable(*msgs);
    if(_error.errorCode) {
        return -1;
    }
    // Check for the no-row condition
    if(msgs->result.row_size() == 0) {
        // Nothing further, don't bother importing.
        return length;
    }
    std::string computedMd5 = util::StringHash::getMd5(cursor, resultSize);
    if(msgs->protoHeader.md5() != computedMd5) {
        _error.description = "Result message MD5 mismatch";
        _error.errorCode = InfileMergerError::RESULT_MD5;
        return 0;
    }
    _needHeader = false;
    // Setup infile properties
    // Spawn thread to handle infile processing
    Mgr::Action a(*_mgr, msgs, _mergeTable);
    msgs.reset();
    boost::thread *t = new boost::thread(a);
    LOGGER_INF << "Started infile thread " << (void*) t << std::endl;
    t->join();
    delete t;
    LOGGER_INF << "Joined infile thread " << (void*) t << std::endl;
    // FIXME: if we spawn a separate thread, we need to save it if we're going to continue before it completes.

    assert(!msgs.get()); // Ownership should have transferred.

//    _thread = new boost::thread(_mgr.newAction(_mgr, _msgs, _mergeTable));

    return length;
}
int InfileMerger::_waitPacket(char const* buffer, int length) {
    // process buffer into rows, as much as possible, saving leftover.
    // consume buffer into rows, and flag return for file handler.

    return 0; // FIXME

}

void InfileMerger::_setupTable(InfileMerger::Msgs const& msgs) {
    // Create table, using schema
    boost::lock_guard<boost::mutex> lock(_createTableMutex);

    if(_needCreateTable) {
        // create schema
        proto::RowSchema const& rs = msgs.result.rowschema();
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
        LOGGER_DBG << "InfileMerger create table:" << createStmt << std::endl;

        if(!_applySqlLocal(createStmt)) {
            _error.errorCode = InfileMergerError::CREATE_TABLE;
            _error.description = "Error creating table (" + _mergeTable + ")";
            _isFinished = true; // Cannot continue.
            return;
        }
        _needCreateTable = false;
    } else {
        // Do nothing, table already created.
    }
}
void InfileMerger::_setupRow() {
    /// Setup infile to merge.
    // ???
}
off_t InfileMerger::merge(char const* dumpBuffer, int dumpLength,
                         std::string const& tableName) {
    if(_error.errorCode) { // Do not attempt when in an error state.
        return -1;
    }
    LOGGER_DBG << "EXECUTING InfileMerger::merge(packetbuffer, " << tableName << ")" << std::endl;
    char const* buffer;
    int length;
    // Now buffer is big enough, start processing.
    if(_needHeader) {
        int hSize = _fetchHeader(dumpBuffer, dumpLength);
        if(hSize == 0) {
            // Buffer not big enough.
            return 0;
        }
        buffer = dumpBuffer + hSize;
        length = dumpLength - hSize;
        _waitPacket(buffer, length);
    } else {
        _waitPacket(dumpBuffer, dumpLength);
    }
    return dumpLength; // _waitPacket doesn't return until it consumes the whole buffer.
}

bool InfileMerger::finalize() {
    bool finalizeOk = true;
    if(_isFinished) {
        LOGGER_ERR << "InfileMerger::finalize(), but _isFinished == true"
                   << std::endl;
    }
    if(_mergeTable != _config.targetTable) {
        // Aggregation needed: Do the aggregation.
        std::string mergeSelect = _config.mergeStmt->getTemplate().generate();
        std::string createMerge = "CREATE TABLE " + _config.targetTable
            + " " + mergeSelect;
        LOGGER_INF << "Merging w/" << createMerge << std::endl;
        finalizeOk = _applySqlLocal(createMerge);

        // Cleanup merge table.
        sql::SqlErrorObject eObj;
        // Don't report failure on not exist
        LOGGER_INF << "Cleaning up " << _mergeTable << std::endl;
#if 1
        bool cleanupOk = _sqlConn->dropTable(_mergeTable, eObj,
                                             false,
                                             _config.targetDb);
#else
        bool cleanupOk = true;
#endif
        if(!cleanupOk) {
            LOGGER_INF << "Failure cleaning up table "
                       << _mergeTable << std::endl;
        }
    }
    LOGGER_INF << "Merged " << _mergeTable << " into " << _config.targetTable
               << std::endl;
    _isFinished = true;
    return finalizeOk;
}

bool InfileMerger::isFinished() const {
    return _isFinished;
}

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
            LOGGER_INF << "TableMerger " << (void*) this
                       << " connected to db." << std::endl;
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

////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////
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
