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
#include "wdb/ChunkResource.h"

namespace lsst {
namespace qserv {
namespace proto {
class ProtoHeader;
class Result;
}}}

namespace lsst {
namespace qserv {
namespace wdb {

/// On the worker, run a query related to a Task, writing the results to a table or supplied SendChannel.
///
class QueryRunner : public wbase::TaskQueryRunner, public std::enable_shared_from_this<QueryRunner> {
public:
    using Ptr = std::shared_ptr<QueryRunner>;
    static QueryRunner::Ptr newQueryRunner(wbase::Task::Ptr const& task,
                                           ChunkResourceMgr::Ptr const& chunkResourceMgr,
                                           mysql::MySqlConfig const& mySqlConfig);
    // Having more than one copy of this would making tracking its progress difficult.
    QueryRunner(QueryRunner const&) = delete;
    QueryRunner operator=(QueryRunner const&) = delete;
    ~QueryRunner();

    bool runQuery() override;
    void cancel() override; ///< Cancel the action (in-progress)

protected:
    QueryRunner(wbase::Task::Ptr const& task,
                ChunkResourceMgr::Ptr const& chunkResourceMgr,
                mysql::MySqlConfig const& mySqlConfig);
private:
    bool _initConnection();
    void _setDb();
    bool _dispatchChannel(); ///< Dispatch with output sent through a SendChannel
    MYSQL_RES* _primeResult(std::string const& query); ///< Obtain a result handle for a query.

    bool _fillRows(MYSQL_RES* result, int numFields, uint& rowCount, size_t& tsize);
    void _fillSchema(MYSQL_RES* result);
    void _initMsgs();
    void _initMsg();
    void _transmit(bool last, uint rowCount, size_t size);
    void _transmitHeader(std::string& msg);

    ///< Actual task
    wbase::Task::Ptr _task;

    ///< Resource reservation
    ChunkResourceMgr::Ptr _chunkResourceMgr;
    std::string _dbName;
    std::atomic<bool> _cancelled{false};
    mysql::MySqlConfig const _mySqlConfig;
    std::unique_ptr<mysql::MySqlConnection> _mysqlConn;

    util::MultiError _multiError; // Error log

    std::shared_ptr<proto::ProtoHeader> _protoHeader;
    std::shared_ptr<proto::Result> _result;
    bool _largeResult{false}; //< True for all transmits after the first transmit.
    unsigned int _initialBlockSize{5000}; //< Maximum size of initial transmit block.
};

}}} // namespace

#endif
