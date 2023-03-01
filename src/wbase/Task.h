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
#include "global/intTypes.h"
#include "memman/MemMan.h"
#include "proto/ScanTableInfo.h"
#include "util/Histogram.h"
#include "util/ThreadPool.h"

// Forward declarations
namespace lsst::qserv {
namespace mysql {
class MySqlConfig;
}  // namespace mysql
namespace proto {
class TaskMsg;
class TaskMsg_Fragment;
}  // namespace proto
namespace wbase {
class SendChannelShared;
}  // namespace wbase
namespace wcontrol {
class SqlConnMgr;
}  // namespace wcontrol
namespace wdb {
class ChunkResourceMgr;
}  // namespace wdb
namespace wpublish {
class QueryStatistics;
}
}  // namespace lsst::qserv

namespace lsst::qserv::wbase {

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

    enum class State { CREATED = 0, QUEUED, EXECUTING_QUERY, READING_DATA, FINISHED };

    struct ChunkEqual {
        bool operator()(Task::Ptr const& x, Task::Ptr const& y);
    };
    struct ChunkIdGreater {
        bool operator()(Ptr const& x, Ptr const& y);
    };

    explicit Task(TaskMsgPtr const& t, std::string const& query, int fragmentNumber,
                  std::shared_ptr<SendChannelShared> const& sc);
    Task& operator=(const Task&) = delete;
    Task(const Task&) = delete;
    virtual ~Task();

    /// Read 'taskMsg' to generate a vector of one or more task objects all using the same 'sendChannel'
    static std::vector<Ptr> createTasks(std::shared_ptr<proto::TaskMsg> const& taskMsg,
                                        std::shared_ptr<wbase::SendChannelShared> const& sendChannel,
                                        std::shared_ptr<wdb::ChunkResourceMgr> const& chunkResourceMgr,
                                        mysql::MySqlConfig const& mySqlConfig,
                                        std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr);

    void setQueryStatistics(std::shared_ptr<wpublish::QueryStatistics> const& qC);

    TaskMsgPtr msg;  ///< Protobufs Task spec
    std::shared_ptr<SendChannelShared> getSendChannel() const { return _sendChannel; }
    void resetSendChannel() { _sendChannel.reset(); }  ///< reset the shared pointer for SendChannelShared
    std::string hash;                                  ///< hash of TaskMsg
    std::string user;                                  ///< Incoming username
    time_t entryTime{0};                               ///< Timestamp for task admission
    char timestr[100];                                 ///< ::ctime_r(&t.entryTime, timestr)
    // Note that manpage spec of "26 bytes"  is insufficient

    /// Cancel the query in progress and set _cancelled.
    void cancel();

    /// Check if this task should be cancelled and call cancel() as needed.
    /// @return true if this task was or needed to be cancelled.
    bool checkCancelled();

    std::string getQueryString() { return _queryString; }
    int getQueryFragmentNum() { return _queryFragmentNum; }
    bool setTaskQueryRunner(
            TaskQueryRunner::Ptr const& taskQueryRunner);  ///< return true if already cancelled.
    void freeTaskQueryRunner(TaskQueryRunner* tqr);
    void setTaskScheduler(TaskScheduler::Ptr const& scheduler) { _taskScheduler = scheduler; }
    TaskScheduler::Ptr getTaskScheduler() { return _taskScheduler.lock(); }
    friend std::ostream& operator<<(std::ostream& os, Task const& t);

    // Shared scan information
    int getChunkId() const;
    QueryId getQueryId() const { return _qId; }
    int getJobId() const { return _jId; }
    int getAttemptCount() const { return _attemptCount; }
    bool getScanInteractive() { return _scanInteractive; }
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
    std::string getIdStr() const { return _idStr; }

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

    /// MySQL finished executing queries.
    void queried();

    std::chrono::milliseconds finished(std::chrono::system_clock::time_point const& now);

    uint64_t getTSeq() const { return _tSeq; }

    std::string makeIdStr(bool invalid = false) const {
        return QueryIdHelper::makeIdStr(_qId, _jId, invalid) + std::to_string(_tSeq) + ":";
    }

    std::shared_ptr<wpublish::QueryStatistics> getQueryStats() const;

    /// Return a json object describing sdome details of this task.
    nlohmann::json getJson() const;

private:
    std::shared_ptr<SendChannelShared> _sendChannel;
    uint64_t const _tSeq = 0;     ///< identifier for the specific task
    QueryId const _qId = 0;       ///< queryId from czar
    int const _jId = 0;           ///< jobId from czar
    int const _attemptCount = 0;  // attemptCount from czar
    /// _idStr for logging only.
    std::string const _idStr = makeIdStr(true);
    std::string _queryString;   ///< The query this task will run.
    int _queryFragmentNum = 0;  ///< The fragment number of the query in the task message.

    std::atomic<bool> _cancelled{false};
    std::atomic<bool> _safeToMoveRunning{false};  ///< false until done with waitForMemMan().
    TaskQueryRunner::Ptr _taskQueryRunner;
    std::weak_ptr<TaskScheduler> _taskScheduler;
    proto::ScanInfo _scanInfo;
    bool _scanInteractive;  ///< True if the czar thinks this query should be interactive.
    bool _onInteractive{
            false};  ///< True if the scheduler put this task on the interactive (group) scheduler.
    std::atomic<memman::MemMan::Handle> _memHandle{memman::MemMan::HandleType::INVALID};
    memman::MemMan::Ptr _memMan;

    mutable std::mutex _stateMtx;  ///< Mutex to protect state related members _state, _???Time.
    State _state{State::CREATED};
    std::chrono::system_clock::time_point _createTime =
            std::chrono::system_clock::now();           ///< task was created
    std::chrono::system_clock::time_point _queueTime;   ///< task was queued
    std::chrono::system_clock::time_point _startTime;   ///< task processing started
    std::chrono::system_clock::time_point _queryTime;   ///< MySQL finished executing queries
    std::chrono::system_clock::time_point _finishTime;  ///< data transmission to Czar fiished
    size_t _totalSize = 0;                              ///< Total size of the result so far.

    /// Stores information on the query's resource usage.
    std::weak_ptr<wpublish::QueryStatistics> _queryStats;
};

}  // namespace lsst::qserv::wbase

#endif  // LSST_QSERV_WBASE_TASK_H
