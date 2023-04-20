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
 * @brief QueryRunner instances perform single-shot query execution with the
 * result reflected in the db state or returned via a SendChannel. Works with
 * new XrdSsi API.
 *
 * @author Daniel L. Wang, SLAC; John Gates, SLAC
 */

// System headers
#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>

// Third-party headers
#include <google/protobuf/arena.h>
#include <mysql/mysql.h>

// Class header
#include "wdb/QueryRunner.h"

// LSST headers
#include "lsst/log/Log.h"
#include "global/DbTable.h"
#include "global/LogContext.h"
#include "global/UnsupportedError.h"
#include "mysql/MySqlConfig.h"
#include "mysql/MySqlConnection.h"
#include "mysql/SchemaFactory.h"
#include "proto/ProtoHeaderWrap.h"
#include "proto/worker.pb.h"
#include "sql/Schema.h"
#include "sql/SqlErrorObject.h"
#include "util/Bug.h"
#include "util/common.h"
#include "util/IterableFormatter.h"
#include "util/MultiError.h"
#include "util/StringHash.h"
#include "util/Timer.h"
#include "util/threadSafe.h"
#include "wbase/Base.h"
#include "wbase/SendChannelShared.h"
#include "wdb/ChunkResource.h"
#include "wpublish/QueriesAndChunks.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wdb.QueryRunner");
}

using namespace std;

namespace lsst::qserv::wdb {

QueryRunner::Ptr QueryRunner::newQueryRunner(wbase::Task::Ptr const& task,
                                             ChunkResourceMgr::Ptr const& chunkResourceMgr,
                                             mysql::MySqlConfig const& mySqlConfig,
                                             shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr) {
    Ptr qr(new QueryRunner(task, chunkResourceMgr, mySqlConfig, sqlConnMgr));  // Private constructor.
    // Let the Task know this is its QueryRunner.
    bool cancelled = qr->_task->setTaskQueryRunner(qr);
    if (cancelled) {
        qr->_cancelled = true;
        // runQuery will return quickly if the Task has been cancelled.
    }
    return qr;
}

/// New instances need to be made with QueryRunner to ensure registration with the task
/// and correct setup of enable_shared_from_this.
QueryRunner::QueryRunner(wbase::Task::Ptr const& task, ChunkResourceMgr::Ptr const& chunkResourceMgr,
                         mysql::MySqlConfig const& mySqlConfig,
                         shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr)
        : _task(task),
          _chunkResourceMgr(chunkResourceMgr),
          _mySqlConfig(mySqlConfig),
          _sqlConnMgr(sqlConnMgr) {
    [[maybe_unused]] int rc = mysql_thread_init();
    assert(rc == 0);
}

/// Initialize the db connection
bool QueryRunner::_initConnection() {
    mysql::MySqlConfig localMySqlConfig(_mySqlConfig);
    localMySqlConfig.username = _task->user;  // Override with czar-passed username.
    if (_mysqlConn != nullptr) {
        LOGS(_log, LOG_LVL_ERROR,
             "QueryRunner::_initConnection _mysqlConn not nullptr _mysqlConn=" << _mysqlConn.get());
    }
    _mysqlConn.reset(new mysql::MySqlConnection(localMySqlConfig));

    if (not _mysqlConn->connect()) {
        LOGS(_log, LOG_LVL_ERROR, "Unable to connect to MySQL: " << localMySqlConfig);
        util::Error error(-1, "Unable to connect to MySQL; " + localMySqlConfig.toString());
        _multiError.push_back(error);
        return false;
    }
    return true;
}

/// Override _dbName with _msg->db() if available.
void QueryRunner::_setDb() {
    if (_task->getDb() != "") {
        _dbName = _task->getDb();
        LOGS(_log, LOG_LVL_DEBUG, "QueryRunner overriding dbName with " << _dbName);
    }
}

size_t QueryRunner::_getDesiredLimit() {
    double percent = xrdsvc::StreamBuffer::percentOfMaxTotalBytesUsed();
    size_t minLimit = 1'000'000;
    size_t maxLimit = proto::ProtoHeaderWrap::PROTOBUFFER_DESIRED_LIMIT;
    if (percent < 0.1) return maxLimit;
    double reduce = 1.0 - (percent + 0.2);  // force minLimit when 80% of memory used.
    if (reduce < 0.0) reduce = 0.0;
    size_t lim = maxLimit * reduce;
    if (lim < minLimit) lim = minLimit;
    return lim;
}

util::TimerHistogram memWaitHisto("memWait Hist", {1, 5, 10, 20, 40});

bool QueryRunner::runQuery() {
    util::InstanceCount ic(to_string(_task->getQueryId()) + "_rq_LDB");  // LockupDB
    QSERV_LOGCONTEXT_QUERY_JOB(_task->getQueryId(), _task->getJobId());
    LOGS(_log, LOG_LVL_INFO,
         "QueryRunner::runQuery() tid=" << _task->getIdStr()
                                        << " scsId=" << _task->getSendChannel()->getScsId());
    if (_runQueryCalled.exchange(true)) {
        LOGS(_log, LOG_LVL_ERROR,
             "QueryRunner::runQuery already called for task=" << _task->getQueryId()
                                                              << " job=" << _task->getJobId());
        throw util::Bug(ERR_LOC, "runQuery called twice");
    }

    // Make certain our Task knows that this object is no longer in use when this function exits.
    class Release {
    public:
        Release(wbase::Task::Ptr t, wbase::TaskQueryRunner* tqr) : _t{t}, _tqr{tqr} {}
        ~Release() { _t->freeTaskQueryRunner(_tqr); }

    private:
        wbase::Task::Ptr _t;
        wbase::TaskQueryRunner* _tqr;
    };
    Release release(_task, this);

    if (_task->checkCancelled()) {
        LOGS(_log, LOG_LVL_DEBUG, "runQuery, task was cancelled before it started.");
        return false;
    }

    _czarId = _task->getCzarId();

    // Wait for memman to finish reserving resources. This can take several seconds.
    util::Timer memTimer;
    memTimer.start();
    _task->waitForMemMan();
    memTimer.stop();
    auto logMsg = memWaitHisto.addTime(memTimer.getElapsed(), _task->getIdStr());
    LOGS(_log, LOG_LVL_DEBUG, logMsg);

    if (_task->checkCancelled()) {
        LOGS(_log, LOG_LVL_DEBUG, "runQuery, task was cancelled after locking tables.");
        return false;
    }

    _setDb();
    LOGS(_log, LOG_LVL_INFO, "Exec in flight for Db=" << _dbName << " sqlConnMgr " << _sqlConnMgr->dump());
    // Queries that span multiple tasks should not be high priority for the SqlConMgr as it risks deadlock.
    bool interactive = _task->getScanInteractive() && !(_task->getSendChannel()->getTaskCount() > 1);
    wcontrol::SqlConnLock sqlConnLock(*_sqlConnMgr, not interactive, _task->getSendChannel());
    bool connOk = _initConnection();
    if (!connOk) {
        // Since there's an error, this will be the last transmit from this QueryRunner.
        if (!_task->getSendChannel()->buildAndTransmitError(_multiError, _task, _cancelled)) {
            LOGS(_log, LOG_LVL_WARN, " Could not report error to czar as sendChannel not accepting msgs.");
        }
        return false;
    }

    switch (_task->getProtocol()) {
        case 2:
            // Run the query and send the results back.
            if (!_dispatchChannel()) {
                LOGS(_log, LOG_LVL_WARN, "_dispatchChannel failed.");
                return false;
            }
            return true;
        case 1:
            throw UnsupportedError(_task->getIdStr() + " QueryRunner: Expected protocol > 1 in TaskMsg");
        default:
            throw UnsupportedError(_task->getIdStr() + " QueryRunner: Invalid protocol in TaskMsg");
    }

    return false;
}

MYSQL_RES* QueryRunner::_primeResult(string const& query) {
    bool queryOk = _mysqlConn->queryUnbuffered(query);
    if (!queryOk) {
        sql::SqlErrorObject errObj;
        errObj.setErrNo(_mysqlConn->getErrno());
        errObj.addErrMsg("primeResult error " + _mysqlConn->getError());
        throw errObj;
    }
    return _mysqlConn->getResult();
}

class ChunkResourceRequest {
public:
    using Ptr = std::shared_ptr<ChunkResourceRequest>;

    ChunkResourceRequest(shared_ptr<ChunkResourceMgr> const& mgr, wbase::Task& task)
            // Use old-school member initializers because gcc 4.8.5
            // miscompiles the code when using brace initializers (DM-4704).
            : _mgr(mgr), _task(task) {}

    // Since each Task has only one subchunk, fragment number isn't needed.
    ChunkResource getResourceFragment() {
        if (!_task.getFragmentHasSubchunks()) {
            /// Why acquire anything if there are no subchunks in the fragment?
            /// Future: Need to be certain this never happens before removing.
            return _mgr->acquire(_task.getDb(), _task.getChunkId(), _task.getDbTbls());
        }

        return _mgr->acquire(_task.getDb(), _task.getChunkId(), _task.getDbTbls(), _task.getSubchunksVect());
    }

private:
    shared_ptr<ChunkResourceMgr> _mgr;
    wbase::Task& _task;
};

bool QueryRunner::_dispatchChannel() {
    bool erred = false;
    int numFields = -1;
    // readRowsOk remains true as long as there are no problems with reading/transmitting.
    // However, if it gets set to false, _mysqlConn->freeResult() needs to be
    // called before this function exits.
    bool readRowsOk = true;
    bool needToFreeRes = false;  // set to true once there are results to be freed.
    // Collect the result in _transmitData. When a reasonable amount of data has been collected,
    // or there are no more rows to collect, pass _transmitData to _sendChannel.
    ChunkResourceRequest::Ptr req;
    ChunkResource::Ptr cr;
    try {
        util::Timer subChunkT;
        subChunkT.start();
        req.reset(new ChunkResourceRequest(_chunkResourceMgr, *_task));
        cr.reset(new ChunkResource(req->getResourceFragment()));
        subChunkT.stop();
        // TODO: Hold onto this for longer period of time as the odds of reuse are pretty low at this scale
        //       Ideally, hold it until moving on to the next chunk. Try to clean up ChunkResource code.

        auto taskSched = _task->getTaskScheduler();
        if (!_cancelled && !_task->getSendChannel()->isDead()) {
            string const& query = _task->getQueryString();
            util::Timer primeT;
            primeT.start();
            MYSQL_RES* res = _primeResult(query);  // This runs the SQL query, throws SqlErrorObj on failure.
            primeT.stop();
            needToFreeRes = true;
            if (taskSched != nullptr) {
                taskSched->histTimeOfRunningTasks->addEntry(primeT.getElapsed());
                LOGS(_log, LOG_LVL_DEBUG, "QR " << taskSched->histTimeOfRunningTasks->getString("run"));
            } else {
                LOGS(_log, LOG_LVL_ERROR, "QR runtaskSched == nullptr");
            }
            double runTimeSeconds = primeT.getElapsed();
            double subchunkRunTimeSeconds = subChunkT.getElapsed();
            auto qStats = _task->getQueryStats();
            if (qStats != nullptr) qStats->addTaskRunQuery(runTimeSeconds, subchunkRunTimeSeconds);

            // This thread may have already been removed from the pool for
            // other reasons, such as taking too long.
            if (not _removedFromThreadPool) {
                // This query has been answered by the database and the
                // scheduler for this worker should stop waiting for it.
                // leavePool() will tell the scheduler this task is finished
                // and create a new thread in the pool to replace this one.
                // This thread will wait for the czar to read all of the
                // results of the query and then die.
                auto pet = _task->getAndNullPoolEventThread();
                _removedFromThreadPool = true;
                if (pet != nullptr) {
                    pet->leavePool();
                } else {
                    LOGS(_log, LOG_LVL_WARN, "Result PoolEventThread was null. Probably already moved.");
                }
            }

            // Transition task's state to the next one (reading data from MySQL and sending them to Czar).
            _task->queried();
            // Pass all information on to the shared object to add on to
            // an existing message or build a new one as needed.
            util::InstanceCount ica(to_string(_task->getQueryId()) + "_rqa_LDB");  // LockupDB
            if (_task->getSendChannel()->buildAndTransmitResult(res, numFields, _task, _largeResult,
                                                                _multiError, _cancelled, readRowsOk)) {
                erred = true;
            }

            // ATTENTION: This call is needed to record the _actual_ completion time of the task.
            // It rewrites the finish timestamp within the task that was made when the task got
            // kicked off the scheduler (see the code block above where a value of _removedFromThreadPool
            // gets tested) which is happening shortly after MySQL query finishes and before the data
            // transmission to Czar starts.
            // NOTE: Tasks would stay in the task "cemetery" (class wpublish::QueriesAndChunks)
            // for about 5 minutes after they finish transmitting data. After that no info on
            // the task is available.
            // TODO: Investigate an option for recording state transitions of the persistent
            // metadata store of the worker, or keeping the state transisitons in a separate transient
            // store that won't be affected by the task destruction.
            _task->finished(std::chrono::system_clock::now());
        }
    } catch (sql::SqlErrorObject const& e) {
        LOGS(_log, LOG_LVL_ERROR, "dispatchChannel " << e.errMsg());
        util::Error worker_err(e.errNo(), e.errMsg());
        _multiError.push_back(worker_err);
        erred = true;
    }
    // IMPORTANT, do not leave this function before this check has been made.
    util::InstanceCount icb(to_string(_task->getQueryId()) + "_rqb_LDB");  // LockupDB
    if (needToFreeRes) {
        needToFreeRes = false;
        // All rows have been read out or there was an error. In
        // either case resources need to be freed.
        _mysqlConn->freeResult();
    }
    util::InstanceCount icc(to_string(_task->getQueryId()) + "_rqc_LDB");  // LockupDB
    if (!readRowsOk) {
        // This means a there was a transmit error and there's no way to
        // send anything to the czar. However, there were mysql results
        // that needed to be freed (see needToFree above).
        LOGS(_log, LOG_LVL_ERROR, "Failed to read and transmit rows.");
        return false;
    }
    // Transmit errors, if needed.
    if (!_cancelled && _multiError.size() > 0) {
        LOGS(_log, LOG_LVL_WARN, "Transmitting error " << _task->getIdStr());
        erred = true;
        // Send results. This needs to happen after the error check.
        // If any errors were found, send an error back.
        if (!_task->getSendChannel()->buildAndTransmitError(_multiError, _task, _cancelled)) {
            LOGS(_log, LOG_LVL_WARN, " Could not report error to czar as sendChannel not accepting msgs.");
        }
    }
    return !erred;
}

void QueryRunner::cancel() {
    // QueryRunner::cancel() should only be called by Task::cancel()
    // to keep the bookkeeping straight.
    LOGS(_log, LOG_LVL_WARN, "Trying QueryRunner::cancel() call");
    _cancelled = true;

    if (_mysqlConn == nullptr) {
        LOGS(_log, LOG_LVL_WARN, "QueryRunner::cancel() no MysqlConn");
    } else {
        int status = _mysqlConn->cancel();
        switch (status) {
            case -1:
                LOGS(_log, LOG_LVL_WARN, "QueryRunner::cancel() NOP");
                break;
            case 0:
                LOGS(_log, LOG_LVL_WARN, "QueryRunner::cancel() success");
                break;
            case 1:
                LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() Error connecting to kill query.");
                break;
            case 2:
                LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() Error processing kill query.");
                break;
            default:
                LOGS(_log, LOG_LVL_ERROR, "QueryRunner::cancel() unknown error");
                break;
        }
    }

    auto streamB = _streamBuf.lock();
    if (streamB != nullptr) {
        streamB->cancel();
    }

    // This could be called after the task has been completed, so sendChannel
    // validation is needed.
    auto sChannel = _task->getSendChannel();
    if (sChannel != nullptr) {
        sChannel->kill("QueryRunner cancel");
    }
}

QueryRunner::~QueryRunner() {}

}  // namespace lsst::qserv::wdb

// Future idea: Query cache
// Pseudocode: Record query in query cache table
/*
  result = runQuery(db.get(),
  "INSERT INTO qcache.Queries "
  "(queryTime, query, db, path) "
  "VALUES (NOW(), ?, "
  "'" + dbName + "'"
  ", "
  "'" + _task->resultPath + "'"
  ")",
  script);
  if (result.size() != 0) {
  _errorNo = EIO;
  _errorDesc += result;
  return false;
  }
*/
