// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2016 LSST Corporation.
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
/// Task.h
/// @author Daniel L. Wang (danielw)
#ifndef LSST_QSERV_WBASE_TASK_H
#define LSST_QSERV_WBASE_TASK_H

// System headers
#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "global/DbTable.h"
#include "global/intTypes.h"
#include "memman/MemMan.h"
#include "proto/ScanTableInfo.h"
#include "wbase/TaskState.h"
#include "util/Histogram.h"
#include "util/ThreadPool.h"

// Forward declarations
namespace lsst::qserv::mysql {
class MySqlConfig;
}
namespace lsst::qserv::proto {
class TaskMsg;
class TaskMsg_Fragment;
}  // namespace lsst::qserv::proto
namespace lsst::qserv::wbase {
class FileChannelShared;
}
namespace lsst::qserv::wcontrol {
class SqlConnMgr;
}
namespace lsst::qserv::wdb {
class ChunkResourceMgr;
}
namespace lsst::qserv::wpublish {
class QueriesAndChunks;
class QueryStatistics;
}  // namespace lsst::qserv::wpublish

namespace lsst::qserv::wbase {

class UserQueryInfo;

/// Base class for tracking a database query for a worker Task.
class TaskQueryRunner {
public:
    using Ptr = std::shared_ptr<TaskQueryRunner>;
    virtual ~TaskQueryRunner(){};
    virtual bool runQuery() = 0;
    virtual void cancel() = 0;  ///< Repeated calls to cancel() must be harmless.
};

class Task;

/// Base class for scheduling Tasks.
/// Allows the scheduler to take appropriate action when a task is cancelled.
class TaskScheduler {
public:
    using Ptr = std::shared_ptr<TaskScheduler>;
    TaskScheduler();
    virtual ~TaskScheduler() {}
    virtual void taskCancelled(Task*) = 0;  ///< Repeated calls must be harmless.
    virtual bool removeTask(std::shared_ptr<Task> const& task, bool removeRunning) = 0;

    util::HistogramRolling::Ptr histTimeOfRunningTasks;       ///< Store information about running tasks
    util::HistogramRolling::Ptr histTimeOfTransmittingTasks;  ///< Store information about transmitting tasks.
};

/// Used to find tasks that are in process for debugging with Task::_idStr.
/// This is largely meant to track down incomplete tasks in a possible intermittent
/// failure and should probably be removed when it is no longer needed.
/// It depends on code in BlendScheduler to work. If the decision is made to keep it
/// forever, dependency on BlendScheduler needs to be re-worked.
struct IdSet {
    void add(std::string const& id) {
        std::lock_guard<std::mutex> lock(mx);
        _ids.insert(id);
    }
    void remove(std::string const& id) {
        std::lock_guard<std::mutex> lock(mx);
        _ids.erase(id);
    }
    std::atomic<int> maxDisp{5};  //< maximum number of entries to show with operator<<
    friend std::ostream& operator<<(std::ostream& os, IdSet const& idSet);

private:
    std::set<std::string> _ids;
    mutable std::mutex mx;
};

/// class Task defines a query task to be done, containing a TaskMsg
/// (over-the-wire) additional concrete info related to physical
/// execution conditions.
/// Task is non-copyable
class Task : public util::CommandForThreadPool {
public:
    static std::string const defaultUser;
    using Ptr = std::shared_ptr<Task>;
    using TaskMsgPtr = std::shared_ptr<proto::TaskMsg>;

    /// Class to store constant sets and vectors.
    class DbTblsAndSubchunks {
    public:
        DbTblsAndSubchunks() = delete;
        DbTblsAndSubchunks(DbTblsAndSubchunks const&) = delete;
        DbTblsAndSubchunks& operator=(DbTblsAndSubchunks const&) = delete;

        DbTblsAndSubchunks(DbTableSet const& dbTbls_, IntVector const& subchunksVect_)
                : dbTbls(dbTbls_), subchunksVect(subchunksVect_) {}
        ~DbTblsAndSubchunks() = default;

        /// Set of tables used by ChunkResourceRequest possible. Set in constructor and should never change.
        const DbTableSet dbTbls;

        /// Vector of subchunkIds. Set in constructor and should never change.
        const IntVector subchunksVect;
    };

    struct ChunkEqual {
        bool operator()(Ptr const& x, Ptr const& y);
    };
    struct ChunkIdGreater {
        bool operator()(Ptr const& x, Ptr const& y);
    };

    Task(TaskMsgPtr const& t, int fragmentNumber, std::shared_ptr<UserQueryInfo> const& userQueryInfo,
         size_t templateId, int subchunkId, std::shared_ptr<FileChannelShared> const& sc,
         uint16_t resultsHttpPort = 8080);
    Task& operator=(const Task&) = delete;
    Task(const Task&) = delete;
    virtual ~Task();

    /// Read 'taskMsg' to generate a vector of one or more task objects all using the same 'sendChannel'
    static std::vector<Ptr> createTasks(std::shared_ptr<proto::TaskMsg> const& taskMsg,
                                        std::shared_ptr<wbase::FileChannelShared> const& sendChannel,
                                        std::shared_ptr<wdb::ChunkResourceMgr> const& chunkResourceMgr,
                                        mysql::MySqlConfig const& mySqlConfig,
                                        std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
                                        std::shared_ptr<wpublish::QueriesAndChunks> const& queriesAndChunks,
                                        uint16_t resultsHttpPort = 8080);

    void setQueryStatistics(std::shared_ptr<wpublish::QueryStatistics> const& qC);

    std::shared_ptr<FileChannelShared> getSendChannel() const { return _sendChannel; }
    void resetSendChannel() { _sendChannel.reset(); }  ///< reset the shared pointer for FileChannelShared
    std::string user;                                  ///< Incoming username
    // Note that manpage spec of "26 bytes"  is insufficient

    /// Cancel the query in progress and set _cancelled.
    /// Query cancellation on the worker is fairly complicated. This
    /// function usually called by `SsiRequest::Finished` when xrootd
    /// indicates the job is cancelled. This may come from:
    /// - xrootd - in the case of communications issues
    /// - czar - user query was cancelled, an error, or limit reached.
    /// This function may also be called by `Task::checkCancelled()` - `_sendChannel`
    ///    has been killed, usually a result of failed communication with xrootd.
    /// If a `QueryRunner` object for this task exists, it must
    /// be cancelled to free up threads and other resources.
    /// Otherwise `_cancelled` is set so that an attempt
    /// to run this `Task` will result in a rapid exit.
    /// This functional also attempts to inform the scheduler for this
    /// `Task` that is has been cancelled (scheduler currently does nothing in this case).
    void cancel();

    /// Check if this task should be cancelled and call cancel() as needed.
    /// @return true if this task was or needed to be cancelled.
    bool checkCancelled();

    TaskState state() const { return _state; }
    std::string getQueryString() const;
    int getQueryFragmentNum() { return _queryFragmentNum; }
    std::string const& resultFilePath() const { return _resultFilePath; }
    std::string const& resultFileXrootUrl() const { return _resultFileXrootUrl; }
    std::string const& resultFileHttpUrl() const { return _resultFileHttpUrl; }
    bool setTaskQueryRunner(
            TaskQueryRunner::Ptr const& taskQueryRunner);  ///< return true if already cancelled.
    void freeTaskQueryRunner(TaskQueryRunner* tqr);
    void setTaskScheduler(TaskScheduler::Ptr const& scheduler) { _taskScheduler = scheduler; }
    TaskScheduler::Ptr getTaskScheduler() { return _taskScheduler.lock(); }
    friend std::ostream& operator<<(std::ostream& os, Task const& t);

    // Shared scan information
    bool getHasChunkId() const { return _hasChunkId; }

    /// @return the chunkId for this task. If the task has no chunkId, return -1.
    int getChunkId() const { return _chunkId; }

    QueryId getQueryId() const { return _qId; }
    size_t getTemplateId() const { return _templateId; }
    int getJobId() const { return _jId; }
    int getAttemptCount() const { return _attemptCount; }
    bool getScanInteractive() { return _scanInteractive; }
    int64_t getMaxTableSize() const { return _maxTableSize; }
    proto::ScanInfo& getScanInfo() { return _scanInfo; }
    void setOnInteractive(bool val) { _onInteractive = val; }
    bool getOnInteractive() { return _onInteractive; }
    bool hasMemHandle() const { return _memHandle != memman::MemMan::HandleType::INVALID; }
    memman::MemMan::Handle getMemHandle() { return _memHandle; }
    memman::MemMan::Status getMemHandleStatus();
    void setMemHandle(memman::MemMan::Handle handle) { _memHandle = handle; }
    void setMemMan(memman::MemMan::Ptr const& memMan) { _memMan = memMan; }
    void waitForMemMan();
    bool getSafeToMoveRunning() { return _safeToMoveRunning; }
    void setSafeToMoveRunning(bool val) { _safeToMoveRunning = val; }  ///< For testing only.

    static IdSet allIds;  // set of all task jobId numbers that are not complete.

    /// @return true if qId and jId match this task's query and job ids.
    bool idsMatch(QueryId qId, int jId, uint64_t tseq) const {
        return (_qId == qId && _jId == jId && tseq == _tSeq);
    }

    // Functions for tracking task state and statistics.

    /// @return 'true' if the task is still running (waiting before MySQL will finish executing
    ///  queries or sending a result set to Czar)
    bool isRunning() const;

    /// @return the amount of time spent so far on the task in milliseconds.
    std::chrono::milliseconds getRunTime() const;

    void queued(std::chrono::system_clock::time_point const& now);
    void started(std::chrono::system_clock::time_point const& now);

    /// The actual execution of the corresponding MySQL query (or queries) started.
    void queryExecutionStarted();

    /// MySQL finished executing queries.
    void queried();

    std::chrono::milliseconds finished(std::chrono::system_clock::time_point const& now);

    uint64_t getTSeq() const { return _tSeq; }

    /// The returned string is only useful for logging purposes.
    std::string getIdStr(bool invalid = false) const {
        return QueryIdHelper::makeIdStr(_qId, _jId, invalid) + std::to_string(_tSeq) + ":";
    }

    std::shared_ptr<wpublish::QueryStatistics> getQueryStats() const;

    /// Return a json object describing sdome details of this task.
    nlohmann::json getJson() const;

    std::string getDb() const { return _db; }
    int getCzarId() const { return _czarId; }
    bool getFragmentHasSubchunks() const { return _fragmentHasSubchunks; }
    int getSubchunkId() const { return _subchunkId; }

    /// Returns a reference to dbTbls.
    const DbTableSet& getDbTbls() const { return _dbTblsAndSubchunks->dbTbls; }

    /// Return a reference to the list of subchunk ids.
    const IntVector& getSubchunksVect() const { return _dbTblsAndSubchunks->subchunksVect; }

    /// Return an identifier of the corresponding MySQL query (if any was set).
    unsigned long getMySqlThreadId() const { return _mysqlThreadId.load(); }

    /// Set MySQL thread associated with a MySQL connection open before executing
    /// task's queries. The identifier is sampled by the worker tasks monitoring
    /// system in order to see what MySQL queries are being executed by tasks.
    void setMySqlThreadId(unsigned long id) { _mysqlThreadId.store(id); }

    /// Return true if this task was already booted.
    bool setBooted();

    /// Return true if the task was booted.
    bool isBooted() { return _booted; }

private:
    std::shared_ptr<UserQueryInfo> _userQueryInfo;    ///< Details common to Tasks in this UserQuery.
    std::shared_ptr<FileChannelShared> _sendChannel;  ///< Send channel.

    uint64_t const _tSeq = 0;          ///< identifier for the specific task
    QueryId const _qId = 0;            ///< queryId from czar
    size_t const _templateId;          ///< Id number of the template in _userQueryInfo.
    bool const _hasChunkId;            ///< True if there was a chunkId in the czar message.
    int const _chunkId;                ///< chunkId from czar
    int const _subchunkId;             ///< subchunkId from czar
    int const _jId = 0;                ///< jobId from czar
    int const _attemptCount = 0;       ///< attemptCount from czar
    int const _queryFragmentNum;       ///< The fragment number of the query in the task message.
    bool const _fragmentHasSubchunks;  ///< True if the fragment in this query has subchunks.
    bool const _hasDb;                 ///< true if db was in message from czar.
    std::string _db;                   ///< Task database
    int const _czarId;                 ///< czar Id from the task message.

    /// Set of tables and vector of subchunk ids used by ChunkResourceRequest. Do not change/reset.
    std::unique_ptr<DbTblsAndSubchunks> _dbTblsAndSubchunks;

    /// The path to the result file.
    std::string _resultFilePath;

    /// The XROOTD URL for the result file: "xroot://<host>:<xrootd-port>" + "/" + _resultFilePath
    /// @note an extra '/' after server:port spec is required to make a "valid" XROOTD url
    std::string _resultFileXrootUrl;

    /// The HTTP URL for the result file: "http://<host>:<http-port>" + _resultFilePath
    std::string _resultFileHttpUrl;

    std::atomic<bool> _cancelled{false};
    std::atomic<bool> _safeToMoveRunning{false};  ///< false until done with waitForMemMan().
    TaskQueryRunner::Ptr _taskQueryRunner;
    std::weak_ptr<TaskScheduler> _taskScheduler;
    proto::ScanInfo _scanInfo;
    bool _scanInteractive;  ///< True if the czar thinks this query should be interactive.
    bool _onInteractive{
            false};  ///< True if the scheduler put this task on the interactive (group) scheduler.
    int64_t _maxTableSize = 0;
    std::atomic<memman::MemMan::Handle> _memHandle{memman::MemMan::HandleType::INVALID};
    memman::MemMan::Ptr _memMan;

    mutable std::mutex _stateMtx;  ///< Mutex to protect state related members _state, _???Time.
    std::atomic<TaskState> _state{TaskState::CREATED};
    std::chrono::system_clock::time_point _createTime =
            std::chrono::system_clock::now();              ///< task was created
    std::chrono::system_clock::time_point _queueTime;      ///< task was queued
    std::chrono::system_clock::time_point _startTime;      ///< task processing started
    std::chrono::system_clock::time_point _queryExecTime;  ///< query execution at MySQL started
    std::chrono::system_clock::time_point _queryTime;      ///< MySQL finished executing queries
    std::chrono::system_clock::time_point _finishTime;     ///< data transmission to Czar fiished
    size_t _totalSize = 0;                                 ///< Total size of the result so far.

    /// Stores information on the query's resource usage.
    std::weak_ptr<wpublish::QueryStatistics> _queryStats;

    std::atomic<unsigned long> _mysqlThreadId{0};  ///< 0 if not connected to MySQL

    std::atomic<bool> _booted{false};  ///< Set to true if this task takes too long and is booted.
    /// Time stamp for when `_booted` is set to true, otherwise meaningless.
    TIMEPOINT _bootedTime;
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_TASK_H
