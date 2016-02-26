// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2016 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_GROUPSCHEDULER_H
#define LSST_QSERV_WSCHED_GROUPSCHEDULER_H

// Qserv headers
#include "util/EventThread.h"
#include "wsched/SchedulerBase.h"

namespace lsst {
namespace qserv {
namespace wsched {

/// A container to hold commands for a single chunk.
/// Similar to util::CommandQueue but it doesn't need the condition variable or mutex.
class GroupQueue {
public:
    using Ptr = std::shared_ptr<GroupQueue>;

    explicit GroupQueue(int maxAccepted, wbase::Task::Ptr const& task);
    bool queTask(wbase::Task::Ptr const& task);
    wbase::Task::Ptr getTask();
    wbase::Task::Ptr peekTask();
    bool isEmpty() { return _tasks.empty(); }

protected:
    bool _hasChunkId{false};
    int _chunkId{0};
    int _maxAccepted{1}; ///< maximum number of commands to accept in this object.
    int _accepted{0}; ///< number of commands accepted.
    std::deque<wbase::Task::Ptr> _tasks;
};

/// GroupScheduler -- A scheduler that is a cross between FIFO and shared scan.
/// Tasks are ordered as they come in, except that queries for the
/// same chunks are grouped together.
class GroupScheduler : public SchedulerBase {
public:
    typedef std::shared_ptr<GroupScheduler> Ptr;

    GroupScheduler(std::string const& name,
                   int maxThreads, int maxReserve, int maxGroupSize, int priority);
    virtual ~GroupScheduler() {}

    bool empty();

    // util::CommandQueue overrides
    void queCmd(util::Command::Ptr const& cmd) override;
    util::Command::Ptr getCmd(bool wait) override;
    void commandFinish(util::Command::Ptr const&) override { --_inFlight; }

    // SchedulerBase overrides
    bool ready() override;
    std::size_t getSize() const override;


private:
    bool _ready();

    std::deque<GroupQueue::Ptr> _queue;
    int _maxGroupSize{1};
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_GROUPSCHEDULER_H
