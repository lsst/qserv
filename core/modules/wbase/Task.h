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
#include <deque>
#include <memory>
#include <mutex>
#include <string>

// Qserv headers
#include "util/Callable.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
    class ScriptMeta;
    class SendChannel;
}
namespace proto {
    class TaskMsg;
    class TaskMsg_Fragment;
}}} // End of forward declarations

namespace lsst {
namespace qserv {
namespace wbase {

/** struct Task defines a query task to be done, containing a TaskMsg
 * (over-the-wire) additional concrete info related to physical
 * execution conditions.
 * Task is non-copyable
 * Task encapsulates nearly zero logic, aside from:
 *  * constructors
 *  * poison()
 *
 */
struct Task {
public:
    static std::string const defaultUser;

    typedef std::shared_ptr<Task> Ptr;
    typedef proto::TaskMsg_Fragment Fragment;
    typedef std::shared_ptr<Fragment> FragmentPtr;
    typedef std::shared_ptr<proto::TaskMsg> TaskMsgPtr;

    struct ChunkEqual {
        bool operator()(Task::Ptr const& x, Task::Ptr const& y);
    };
    struct ChunkIdGreater {
        bool operator()(Ptr const& x, Ptr const& y);
    };

    explicit Task() : _poisoned{false} {}
    explicit Task(TaskMsgPtr t, std::shared_ptr<SendChannel> sc);
    Task& operator=(const Task&) = delete;
    Task(const Task&) = delete;

    TaskMsgPtr msg; ///< Protobufs Task spec
    std::shared_ptr<SendChannel> sendChannel; ///< For result reporting
    std::string hash; ///< hash of TaskMsg
    std::string dbName; ///< dominant db
    std::string user; ///< Incoming username
    time_t entryTime; ///< Timestamp for task admission
    char timestr[100]; ///< ::ctime_r(&t.entryTime, timestr)
    // Note that manpage spec of "26 bytes"  is insufficient

    void poison(); ///< Call the previously-set poisonFunc
    void setPoison(std::shared_ptr<util::VoidCallable<void> > poisonFunc);
    friend std::ostream& operator<<(std::ostream& os, Task const& t);

private:
    std::mutex _mutex; // Used for handling poison
    std::shared_ptr<util::VoidCallable<void> > _poisonFunc;
    bool _poisoned; ///< To prevent multiple-poisonings
};
typedef std::deque<Task::Ptr> TaskQueue;
typedef std::shared_ptr<TaskQueue> TaskQueuePtr;

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WBASE_TASK_H
