// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 LSST Corporation.
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
#ifndef LSST_QSERV_QDISP_RESPONSEPOOL_H
#define LSST_QSERV_QDISP_RESPONSEPOOL_H

// System headers

// Third-party headers

// Qserv headers
#include "global/Bug.h"
#include "util/ThreadPool.h"

namespace lsst {
namespace qserv {
namespace qdisp {

// FIFO priority queue. Elements with the same priority are handled in
// a FIFO manner. Lower integer values are higher priority.
// Low values are higher priority.
class PriorityQueue : public util::CommandQueue {
public:
    using Ptr = std::shared_ptr<PriorityQueue>;

    class PriQ : public util::CommandQueue {
    public:
        using Ptr = std::shared_ptr<PriQ>;
        explicit PriQ(int priority, int minRunning) : _priority(priority), _minRunning(minRunning) {}
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
    bool addPriQueue(int priority, int minRunning) {
        std::lock_guard<std::mutex> lock(_mtx);
        auto q = std::make_shared<PriQ>(priority, minRunning);
        std::pair<int, PriQ::Ptr> item(priority, q);
        auto ret = _queues.insert(item);
        if (!ret.second) {
            ; /// &&& add log message
        }
        return ret.second;
    }

    void queCmd(util::Command::Ptr const& cmd) override {
        queCmd(cmd, _defaultPriority);
    }

    void queCmd(util::Command::Ptr const& cmd, int priority) {
        {
            std::lock_guard<std::mutex> lock(_mtx);
            auto iter = _queues.find(priority);
            if (iter == _queues.end()) {
                // give it the default priority
                // &&& add log message
                iter = _queues.find(_defaultPriority);
                if (iter == _queues.end()) {
                    throw Bug("PriorityQueue default priority queue not found!");
                }
            }

            iter->second->queCmd(cmd);
            _changed = true;
        }
        _cv.notify_all();
    }

    util::Command::Ptr getCmd(bool wait=true) override {
        util::Command::Ptr ptr;
        std::unique_lock<std::mutex> uLock(_mtx);
        while (true) {
            _changed = false;

            /// Make sure minimum number of jobs running per priority.
            auto iter = _queues.begin();
            auto end = _queues.end();
            if (!_shuttingDown) {
                // If shutting down, this could prevent all jobs from completing.
                // Goes from highest to lowest priority queue
                for (;iter != end; ++iter) {
                    PriQ::Ptr const& que = iter->second;
                    if (que->running < que->getMinRunning()) {
                        ptr = que->getCmd(false); // no wait
                        if (ptr != nullptr) {
                            return ptr;
                        }
                    }
                }
            }

            // Since all the minimums are met, just run the first command found.
            iter = _queues.begin();
            for (;iter != end; ++iter) {
                PriQ::Ptr const& que = iter->second;
                ptr = que->getCmd(false); // no wait
                if (ptr != nullptr) {
                    return ptr;
                }
            }

            // If nothing was found, wait or return nullptr.
            if (wait) {
                _cv.wait(uLock, [this](){ return _changed; });
            } else {
                return ptr;
            }
        }
    }

    void prepareShutdown() {
        std::lock_guard<std::mutex> lock(_mtx);
        _shuttingDown = true;
    }

private:
    std::mutex _mtx;
    std::condition_variable _cv;
    bool _shuttingDown{false};
    bool _changed{false};

    std::map<int, PriQ::Ptr> _queues;
    int _defaultPriority{1};

};



class ResponsePool {
public:
    typedef std::shared_ptr<ResponsePool> Ptr;

    ResponsePool() {
        _prQueue->addPriQueue(0,1); // Highest priority queue
        _prQueue->addPriQueue(1,1); // Normal priority queue
        // default priority is the lowest priority.
    }

    void queCmdHigh(util::Command::Ptr const& cmd) {
        _prQueue->queCmd(cmd, 0);
    }

    void queCmdLow(util::Command::Ptr const& cmd) {
        _prQueue->queCmd(cmd, 2);
    }

    void queCmdNorm(util::Command::Ptr const& cmd) {
        _prQueue->queCmd(cmd, 1);
    }

    void queCmd(util::Command::Ptr const& cmd, int priority) {
        _prQueue->queCmd(cmd, priority);
    }

    /// Commands on queue's with priority lower than default may not be run.
    void shutdownPool() {
        _prQueue->prepareShutdown();
        _pool->shutdownPool();
    }

private:
    PriorityQueue::Ptr _prQueue = std::make_shared<PriorityQueue>(2,1); // default (lowest) priority.
    util::ThreadPool::Ptr _pool{util::ThreadPool::newThreadPool(30, _prQueue)};
};


}}} // namespace lsst::qserv::disp

#endif /* LSST_QSERV_QDISP_RESPONSEPOOL_H_ */
