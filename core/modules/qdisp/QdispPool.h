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


/// FIFO priority queue. Elements with the same priority are handled in
/// a FIFO manner. Lower integer values are higher priority.
/// Higher priority queues get asked first when a thread becomes available
/// but the system reserves room so that each priority has at least
/// a minimum number of threads running.
class PriorityQueue : public util::CommandQueue {
public:
    using Ptr = std::shared_ptr<PriorityQueue>;

    /// A queue for handling all messages of a given priority.
    class PriQ : public util::CommandQueue {
    public:
        using Ptr = std::shared_ptr<PriQ>;
        explicit PriQ(int priority, int minRunning, int maxRunning) :
            _priority(priority), _minRunning(minRunning), _maxRunning(maxRunning) {}
        ~PriQ() override = default;
        int getPriority() const { return _priority; }
        int getMinRunning() const { return _minRunning; }
        int getMaxRunning() const { return _maxRunning; }

        std::atomic<int> running{0}; ///< number of jobs of this priority currently running.
    private:
        int const _priority;   ///< priority value of this queue
        int const _minRunning; ///< minimum number of threads (unless nothing on this queue to run)
        int const _maxRunning; ///< maximum number of threads for this PriQ to use.
    };


    PriorityQueue() = delete;
    PriorityQueue(PriorityQueue const&) = delete;
    PriorityQueue& operator=(PriorityQueue const&) = delete;


    PriorityQueue(int defaultPriority, int minRunning, int maxRunning) : _defaultPriority(defaultPriority) {
        _queues[_defaultPriority] = std::make_shared<PriQ>(_defaultPriority, minRunning, maxRunning);
    }

    ///< @Return true if the queue could be added.
    bool addPriQueue(int priority, int minRunning, int spareThreads);

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


/// This class is used to provide a pool of threads for handling out going
/// and incoming messages from xrootd as well as a system for prioritizing
/// the messages.
/// This has not worked entirely as intended. Reducing the number of threads
/// had negative impacts on xrootd, but other changes have been made such that
/// reducing the size of the thread pools can be tried again.
/// What it does do is prioritize out going messages (typically jobs going to
/// workers), allow interactive queries to be handled quickly, even under
/// substantial loads, and it gives a good idea of how busy the czar really
/// is. Large numbers of queued items in any of the scan queries, or large
/// results would be good indicators to avoid giving a particular czar more
/// user queries.
///
class QdispPool {
public:
    typedef std::shared_ptr<QdispPool> Ptr;

    QdispPool() { _setup(false); }
    explicit QdispPool(bool unitTest) { _setup(unitTest); }
    QdispPool(QdispPool const&) = delete;
    QdispPool& operator=(QdispPool const&) = delete;

    /// Lower priority numbers are higher priority.
    /// Invalid priorities get the lowest priority, which
    /// is the bottom of the heap.
    void queCmd(PriorityCommand::Ptr const& cmd, int priority) {
        _prQueue->queCmd(cmd, priority);
    }

    /// Commands on queue's with priority lower than default may not be run.
    void shutdownPool() {
        _prQueue->prepareShutdown();
        _pool->shutdownPool();
    }

private:
    void _setup(bool unitTest);

    PriorityQueue::Ptr _prQueue;
    util::ThreadPool::Ptr _pool;
};


}}} // namespace lsst::qserv::disp

#endif /* LSST_QSERV_QDISP_QDISPPOOL_H_ */
