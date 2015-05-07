// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2009-2015 LSST Corporation.
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
#ifndef LSST_QSERV_UTIL_WORKQUEUE_H
#define LSST_QSERV_UTIL_WORKQUEUE_H

// System headers
#include <deque>
#include <memory>
#include <mutex>
#include <thread>

namespace lsst {
namespace qserv {
namespace util {

///  class WorkQueue -- A class that implements a fixed-size
///  thread-pool for performing tasks.  No pre-emption, so if all
///  threads are blocked, the queue will stall.
///
///  Used to do lightweight concurrent things without thread
///  creation/destruction overhead. No API for blocking until all work is
///  complete: callers should have external means of knowing when their actions
///  are complete (i.e., the work they submit performs callbacks upon
///  completion).
class WorkQueue {
public:
    class Callable {
    public:
        virtual ~Callable() {} // Must halt current operation.
        virtual void operator()() = 0;
        virtual void abort() {} // Halt while running or otherwise
        virtual void cancel() {} // Cleanup (not run yet)
    };

    WorkQueue(int numRunners);
    ~WorkQueue();

    void add(std::shared_ptr<Callable> c);
    void cancelQueued();

    std::shared_ptr<Callable> getNextCallable();

    // For use by runners.
    class Runner;
    void registerRunner(Runner* r);
    void signalDeath(Runner* r);
    bool isPoison(Callable const* const c) {
        return (Callable const* const)0 == c;
    }

private:
    void _addRunner();
    void _dropQueue(bool final=true);

    typedef std::deque<std::shared_ptr<Callable> > WorkDeque;
    typedef std::deque<Runner*> RunnerDeque;

    std::mutex _mutex;
    std::mutex _runnersMutex;
    std::condition_variable _queueNonEmpty;
    std::condition_variable _runnersEmpty;
    std::condition_variable _runnerRegistered;
    WorkDeque _queue;
    bool _isDead;
    RunnerDeque _runners;
};

}}}  // namespace lsst::qserv::util

#endif // LSST_QSERV_UTIL_WORKQUEUE_H
