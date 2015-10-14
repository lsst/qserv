// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2015 LSST Corporation.
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
#include <deque>
#include <memory>
#include <mutex>
#include <string>

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
    struct ScriptMeta;
    class SendChannel;
}
namespace proto {
    class TaskMsg;
    class TaskMsg_Fragment;
}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace wbase {

/// Base class for tracking a database query for a worker Task.
class TaskQueryRunner {
public:
    using Ptr = std::shared_ptr<TaskQueryRunner>;
    virtual ~TaskQueryRunner() {};
    virtual bool runQuery()=0;
    virtual void cancel()=0; ///< Repeated calls to cancel() must be harmless.
};

class Task;

/// Base class for scheduling Tasks.
/// Allows the scheduler to take appropriate action when a task is cancelled.
class TaskScheduler {
public:
    using Ptr = std::shared_ptr<TaskScheduler>;
    virtual ~TaskScheduler() {}
    virtual void taskCancelled(Task*)=0;///< Repeated calls must be harmless.
};

/// struct Task defines a query task to be done, containing a TaskMsg
/// (over-the-wire) additional concrete info related to physical
/// execution conditions.
/// Task is non-copyable
/// Task encapsulates nearly zero logic, aside from:
/// * constructors
struct Task {
public:
    static std::string const defaultUser;

    using Ptr =  std::shared_ptr<Task>;
    using Fragment = proto::TaskMsg_Fragment;
    using FragmentPtr = std::shared_ptr<Fragment>;
    using TaskMsgPtr = std::shared_ptr<proto::TaskMsg>;

    struct ChunkEqual {
        bool operator()(Task::Ptr const& x, Task::Ptr const& y);
    };
    struct ChunkIdGreater {
        bool operator()(Ptr const& x, Ptr const& y);
    };

    Task() {}
    explicit Task(TaskMsgPtr const& t, std::shared_ptr<SendChannel> const& sc);
    Task& operator=(const Task&) = delete;
    Task(const Task&) = delete;

    TaskMsgPtr msg; ///< Protobufs Task spec
    std::shared_ptr<SendChannel> sendChannel; ///< For result reporting
    std::string hash; ///< hash of TaskMsg
    std::string dbName; ///< dominant db
    std::string user; ///< Incoming username
    time_t entryTime {0}; ///< Timestamp for task admission
    char timestr[100]; ///< ::ctime_r(&t.entryTime, timestr)
    // Note that manpage spec of "26 bytes"  is insufficient

    void cancel();
    bool getCancelled(){ return _cancelled; }
    bool setTaskQueryRunner(TaskQueryRunner::Ptr const& taskQueryRunner); ///< return true if already cancelled.
    void freeTaskQueryRunner(TaskQueryRunner *tqr);
    void setTaskScheduler(TaskScheduler::Ptr const& scheduler) { _taskScheduler = scheduler; }
    friend std::ostream& operator<<(std::ostream& os, Task const& t);

private:
    std::atomic<bool> _cancelled{false};
    TaskQueryRunner::Ptr _taskQueryRunner;
    std::weak_ptr<TaskScheduler> _taskScheduler;
};
using TaskQueue =  std::deque<Task::Ptr>;
using TaskQueuePtr = std::shared_ptr<TaskQueue>;

/// MsgProcessor implementations handle incoming TaskMsg objects by creating a Task to write their
///results over a SendChannel
struct MsgProcessor {
    virtual ~MsgProcessor() {}
    /// @return a pointer to the Task so it can be cancelled or tracked.
    virtual std::shared_ptr<Task> processMsg(std::shared_ptr<proto::TaskMsg> const& taskMsg,
                                             std::shared_ptr<SendChannel> const& replyChannel) = 0;

};

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WBASE_TASK_H
