// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011, 2012 LSST Corporation.
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
// class Task defines a query task to be done, containing a TaskMsg
// (over-the-wire) additional concrete info related to physical
// execution conditions.
/// @author Daniel L. Wang (danielw)
#ifndef LSST_QSERV_WCONTROL_TASK_H
#define LSST_QSERV_WCONTROL_TASK_H

// System headers
#include <deque>
#include <string>

// Third-party headers
#include <boost/shared_ptr.hpp>


// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
    class ScriptMeta;
}
namespace proto {
    class TaskMsg;
    class TaskMsg_Fragment;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace wcontrol {

struct Task {
public:
    static std::string const defaultUser;

    typedef boost::shared_ptr<Task> Ptr;
    typedef proto::TaskMsg_Fragment Fragment;
    typedef boost::shared_ptr<Fragment> FragmentPtr;
    typedef boost::shared_ptr<proto::TaskMsg> TaskMsgPtr;

    struct ChunkEqual {
        bool operator()(Task::Ptr const& x, Task::Ptr const& y);
    };
    struct ChunkIdGreater {
        bool operator()(Ptr const& x, Ptr const& y);
    };

    explicit Task() {}
    explicit Task(wbase::ScriptMeta const& s, std::string const& user_=defaultUser);
    explicit Task(TaskMsgPtr t, std::string const& user_=defaultUser);

    TaskMsgPtr msg;
    std::string hash;
    std::string dbName;
    std::string resultPath;
    std::string user;
    bool needsCreate;
    time_t entryTime;
    char timestr[100]; ///< ::ctime_r(&t.entryTime, timestr)
    // Note that manpage spec of "26 bytes"  is insufficient

    friend std::ostream& operator<<(std::ostream& os, Task const& t);
};
typedef std::deque<Task::Ptr> TaskQueue;
typedef boost::shared_ptr<TaskQueue> TaskQueuePtr;

}}} // namespace lsst::qserv::wcontrol

#endif // LSST_QSERV_WCONTROL_TASK_H
