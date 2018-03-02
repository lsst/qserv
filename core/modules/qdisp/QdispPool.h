// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_QDISP_QDISPPOOL_H
#define LSST_QSERV_QDISP_QDISPPOOL_H

// System headers
#include <map>

// Third-party headers

// Qserv headers
#include "global/Bug.h"
#include "util/ThreadPool.h"

namespace lsst {
namespace qserv {
namespace qdisp {

class PriorityQueue;

class PriorityCommand : public util::CommandTracked {
public:
    using Ptr = std::shared_ptr<PriorityCommand>;
    PriorityCommand() = default;
    explicit PriorityCommand(std::function<void(util::CmdData*)> func) : CommandTracked(func) {}
    ~PriorityCommand() override = default;
    friend PriorityQueue;
private:
    int _priority{0}; // Need to know what queue this was placed on.
};


// FIFO priority queue. Elements with the same priority are handled in
// a FIFO manner. Lower integer values are higher priority.
class PriorityQueue : public util::CommandQueue {
public:
    using Ptr = std::shared_ptr<PriorityQueue>;

    class PriQ : public util::CommandQueue {
    public:
        using Ptr = std::shared_ptr<PriQ>;
        explicit PriQ(int priority, int minRunning) : _priority(priority), _minRunning(minRunning) {}
        ~PriQ() override = default;
        int getPriority() { return _priority; }
        int getMinRunning() { return _minRunning; }

        std::atomic<int> running{0}; ///< number of jobs of this priority currently running.
    private:
        int _priority; ///< priority value of this queue
        int _minRunning; ///< minimum number of threads
    };


    PriorityQueue() = delete;
    PriorityQueue(PriorityQueue const&) = delete;
    PriorityQueue& operator=(PriorityQueue const&) = delete;


    PriorityQueue(int defaultPriority, int minRunning) : _defaultPriority(defaultPriority) {
        _queues[_defaultPriority] = std::make_shared<PriQ>(_defaultPriority, minRunning);
    }

    ///< @Return true if the queue could be added.
    bool addPriQueue(int priority, int minRunning);

    /// The pool needs to be able to place commands in this queue for shutdown.
    void queCmd(util::Command::Ptr const& cmd) override;

    void queCmd(PriorityCommand::Ptr const& cmd, int priority);

    util::Command::Ptr getCmd(bool wait=true) override;
    void prepareShutdown();

    void commandStart(util::Command::Ptr const& cmd) override;
    void commandFinish(util::Command::Ptr const& cmd) override;

    std::string statsStr();

private:
    void _incrDecrRunningCount(util::Command::Ptr const& cmd, int incrDecr);

    std::mutex _mtx;
    std::condition_variable _cv;
    bool _shuttingDown{false};
    bool _changed{false};

    std::map<int, PriQ::Ptr> _queues;
    int _defaultPriority{1};

    friend std::ostream& operator<<(std::ostream& os, PriorityQueue const& pq);
};


class QdispPool {
public:
    typedef std::shared_ptr<QdispPool> Ptr;

    QdispPool() {
        _prQueue->addPriQueue(0,1);  // Highest priority queue
        _prQueue->addPriQueue(1,1);  // High priority queue
        _prQueue->addPriQueue(2,9); // Normal priority queue
        _prQueue->addPriQueue(3,3);  // Low priority queue
        // default priority is the lowest priority.
    }


    void queCmdVeryHigh(PriorityCommand::Ptr const& cmd) {
            _prQueue->queCmd(cmd, 0);
        }

    void queCmdHigh(PriorityCommand::Ptr const& cmd) {
        _prQueue->queCmd(cmd, 1);
    }

    void queCmdLow(PriorityCommand::Ptr const& cmd) {
        _prQueue->queCmd(cmd, 3);
    }

    void queCmdNorm(PriorityCommand::Ptr const& cmd) {
        _prQueue->queCmd(cmd, 2);
    }

    void queCmd(PriorityCommand::Ptr const& cmd, int priority) {
        _prQueue->queCmd(cmd, priority);
    }

    /// Commands on queue's with priority lower than default may not be run.
    void shutdownPool() {
        _prQueue->prepareShutdown();
        _pool->shutdownPool();
    }

private:
    /// The default priority queue is meant for pool control commands.
    PriorityQueue::Ptr _prQueue = std::make_shared<PriorityQueue>(100,1); // default (lowest) priority.
    util::ThreadPool::Ptr _pool{util::ThreadPool::newThreadPool(30, _prQueue)};
};


}}} // namespace lsst::qserv::disp

#endif /* LSST_QSERV_QDISP_QDISPPOOL_H_ */
