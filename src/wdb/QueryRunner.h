// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2016 LSST Corporation.
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

#ifndef LSST_QSERV_WDB_QUERYRUNNER_H
#define LSST_QSERV_WDB_QUERYRUNNER_H
 /**
  * @file
  *
  * @brief QueryAction instances perform single-shot query execution with the
  * result reflected in the db state or returned via a SendChannel. Works with
  * new XrdSsi API.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <atomic>
#include <memory>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "util/MultiError.h"
#include "wbase/Task.h"
#include "wbase/TransmitData.h"
#include "wcontrol/SqlConnMgr.h"
#include "wdb/ChunkResource.h"

namespace lsst {
namespace qserv {

namespace proto {
class ProtoHeader;
class Result;
}

namespace util {
class TimerHistogram;
}

namespace xrdsvc {
class StreamBuffer;
}

namespace wcontrol {
class SqlConnMgr;
class TransmitMgr;
}

}}


namespace lsst {
namespace qserv {
namespace wdb {

class SchemaCol {
public:
    SchemaCol() = default;
    SchemaCol(SchemaCol const&) = default;
    SchemaCol& operator=(SchemaCol const&) = default;
    SchemaCol(std::string name, std::string sqltype, int mysqltype) :
        colName(name), colSqlType(sqltype), colMysqlType(mysqltype) {
    }
    std::string colName;
    std::string colSqlType; ///< sqltype for the column
    int colMysqlType = 0; ///< MySQL type number
};


/// On the worker, run a query related to a Task, writing the results to a table or supplied SendChannel.
///
class QueryRunner : public wbase::TaskQueryRunner, public std::enable_shared_from_this<QueryRunner> {
public:
    using Ptr = std::shared_ptr<QueryRunner>;
    static QueryRunner::Ptr newQueryRunner(wbase::Task::Ptr const& task,
                                           ChunkResourceMgr::Ptr const& chunkResourceMgr,
                                           mysql::MySqlConfig const& mySqlConfig,
                                           std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
                                           std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr);
    // Having more than one copy of this would making tracking its progress difficult.
    QueryRunner(QueryRunner const&) = delete;
    QueryRunner& operator=(QueryRunner const&) = delete;
    ~QueryRunner();

    bool runQuery() override;

    /// Cancel the action (in-progress). This should only be called
    /// by Task::cancel(), so if this needs to be cancelled elsewhere,
    /// call Task::cancel().
    /// This should kill an in progress SQL command.
    void cancel() override;

protected:
    QueryRunner(wbase::Task::Ptr const& task,
                ChunkResourceMgr::Ptr const& chunkResourceMgr,
                mysql::MySqlConfig const& mySqlConfig,
                std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
                std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr);
private:
    bool _initConnection();
    void _setDb();

    /// Dispatch with output sent through a SendChannel
    bool _dispatchChannel();
    MYSQL_RES* _primeResult(std::string const& query); ///< Obtain a result handle for a query.


    /// Use the mysql 'result' and other parameters to fill 'tData'.
    /// @return true if there are no more left in 'result'
    bool _fillRows(MYSQL_RES* result, int numFields, uint& rowCount, size_t& tsize);

    /// Use the mysql 'result' to load _schemaCols with the schema.
    void _fillSchema(MYSQL_RES* result);
    proto::Result* _initResult();
    void _initTransmit();


    static size_t _getDesiredLimit();

    /// The pass _transmitData to _sendChannel so it can be transmitted.
    /// 'lastIn' - True if this is the last transmit for this QueryRunner instance.
    /// @return true if _transmitData was passed to _sendChannel and will be transmitted.
    bool _transmit(bool lastIn);

    /// Build a message in 'tData' based on the parameters provided
     void _buildDataMsg(unsigned int rowCount, size_t size);
     void _buildHeader();

    wbase::Task::Ptr const _task; ///< Actual task

    qmeta::CzarId _czarId = 0; ///< To be replaced with the czarId of the requesting czar.

    /// Resource reservation
    ChunkResourceMgr::Ptr _chunkResourceMgr;
    std::string _dbName;
    std::atomic<bool> _cancelled{false};
    std::weak_ptr<xrdsvc::StreamBuffer> _streamBuf; ///< used release condition variable on cancel.
    std::atomic<bool> _removedFromThreadPool{false};
    mysql::MySqlConfig const _mySqlConfig;
    std::unique_ptr<mysql::MySqlConnection> _mysqlConn{nullptr};

    util::MultiError _multiError; // Error log

    std::vector<SchemaCol> _schemaCols; ///< Description of columns for schema.
    wbase::TransmitData::Ptr _transmitData; ///< Data for this transmit.
    bool _largeResult = false; //< True for all transmits after the first transmit.

    /// Used to limit the number of open MySQL connections.
    std::shared_ptr<wcontrol::SqlConnMgr> const _sqlConnMgr;
    std::atomic<bool> _runQueryCalled{false}; ///< If runQuery gets called twice, the scheduler messed up.

    /// Used to limit the number of transmits being sent to czars.
    std::shared_ptr<wcontrol::TransmitMgr> const _transmitMgr;
};

}}} // namespace

#endif
