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
 *
 *  @author: John Gates,
 */

#ifndef LSST_QSERV_UTIL_EVENTTHREAD_H_
#define LSST_QSERV_UTIL_EVENTTHREAD_H_

// System headers
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace lsst {
namespace qserv {
namespace util {

/// Tracker provides an interface for indicating an action is complete.
///
class Tracker {
public:
    Tracker() {};
    virtual ~Tracker() {};
    enum class Status { INPROGRESS, COMPLETE };
    using Ptr = std::shared_ptr<Tracker>;
    void setComplete();
    bool isFinished();
    void waitComplete();
private:
    Status _trStatus{Status::INPROGRESS};
    std::mutex _trMutex;
    std::condition_variable _trCV;
};

/// Base class for commands. Can be used with functions as is or
/// as a base class when data is needed.
class Command {
public:
    using Ptr = std::shared_ptr<Command>;
    Command() {};
    Command(std::function<void(Command*)> func) : _func{func} {}
    virtual ~Command() {};
    virtual int action() {
        _func(this);
        return 0;
    };
    void setFunc(std::function<void(Command*)> func);
protected:
    std::function<void(Command*)> _func = [](Command *){;};
};

/// Extension of Command that can notify other threads when its
/// action is complete.
class CommandTracked : public virtual Command, public virtual Tracker {
public:
    using Ptr = std::shared_ptr<CommandTracked>;
    CommandTracked() {};
    CommandTracked(std::function<void(Command*)> func) : Command{func} {}
    virtual ~CommandTracked() {};
    int action() override {
        _func(this);
        setComplete();
        return 0;
    };
};

/// A queue of Commands meant to drive an EventThread.
///
class CommandQueue {
public:
    using Ptr = std::shared_ptr<CommandQueue>;
    /// Queue a command object in a thread safe way and signal any threads
    /// waiting on the queue that a command is available.
    void queCmd(Command::Ptr const& cmd) {
        std::lock_guard<std::mutex> lock(_mx);
        _qu.push_back(cmd);
        if (_qu.size() > 1) {
            _cv.notify_all();
        } else {
            _cv.notify_one();
        }
    };

    /// Get a command off the queue. If no message is available, wait until one is.
    Command::Ptr getCmd() {
        std::unique_lock<std::mutex> lock(_mx);
        _cv.wait(lock, [this](){return !_qu.empty();});
        auto cmd = _qu.front();
        _qu.pop_front();
        return cmd;
    };

protected:
    std::deque<Command::Ptr> _qu{};
    std::condition_variable _cv{};
    std::mutex              _mx{};
};

/// An event driven thread.
/// Thread must be started with run(). Stop the thread by calling queEnd().
class EventThread {
public:
    enum { HALT = -1000 };
    EventThread() {}
    explicit EventThread(CommandQueue::Ptr const& q) : _q{q} {}
    EventThread(EventThread const&) = delete;
    EventThread operator=(EventThread const&) = delete;
    virtual ~EventThread() {}

    void handleCmds();
    void join() { _t.join(); }
    void run(); ///< Run this EventThread.

    /// Put a Command on this EventThread's queue.
    void queCmd(Command::Ptr cmd) {
        assert(cmd != nullptr);
        _q->queCmd(cmd);
    }

    /// Queues and action that will stop the EventThread that answers it.
    virtual void queEnd() {
        struct MsgEnd : public Command {
            int action() override { return HALT; }
        };
        std::shared_ptr<MsgEnd> cmd = std::make_shared<MsgEnd>();
        queCmd(cmd);
    }

protected:
    virtual void startup() {};  ///< Things to do before handling commands
    virtual void finishup() {}; ///< things to do when the thread is closing down.

    CommandQueue::Ptr _q{std::make_shared<CommandQueue>()}; ///< Queue of commands.
    std::thread _t;                 // Our thread
    bool   _loop {true};       // Keep running the event loop while this is true.
};

class ThreadPool;

/// An EventThread to be used by the ThreadPool class.
/// finishup() is used to tell the ThreadPool that this thread is finished.
class PoolEventThread : public EventThread {
public:
    using Ptr = std::shared_ptr<PoolEventThread>;
    PoolEventThread(std::shared_ptr<ThreadPool> const& threadPool, CommandQueue::Ptr const& q)
     : EventThread{q}, _threadPool{threadPool} {}
    ~PoolEventThread();

protected:
     void finishup() override;
     std::shared_ptr<ThreadPool> _threadPool;
};

/// ThreadPool is a variable size pool of threads all fed by the same CommandQueue.
/// Growing the pool is simple, shrinking the pool is complex. Both operation should
/// have no effect on items running or on the queue.
/// endAll() must be called to shutdown the ThreadPool.
class ThreadPool : public std::enable_shared_from_this<ThreadPool> {
public:
    using Ptr = std::shared_ptr<ThreadPool>;
    static ThreadPool::Ptr newThreadPool(uint thrdCount, CommandQueue::Ptr const& q) {
        Ptr thp{new ThreadPool{thrdCount, q}}; // private constructor
        thp->_resize();
        return thp;
    }
    virtual ~ThreadPool();

    CommandQueue::Ptr getQueue() {return _q;}
    uint getTargetThrdCount() {
        std::lock_guard<std::mutex> lock(_countMutex);
        return _targetThrdCount;
    }
    uint size() {
        std::lock_guard<std::mutex> lock(_poolMutex);
        return _pool.size();
    }

    void waitForResize(int millisecs);
    void endAll() { resize(0); }
    void resize(uint targetThrdCount);
    PoolEventThread::Ptr release(PoolEventThread *thread);

protected:
    ThreadPool(uint thrdCount, CommandQueue::Ptr const& q) : _targetThrdCount{thrdCount}, _q{q} {
        if (_q == nullptr) {
            _q = std::make_shared<CommandQueue>();
        }
    }
    void _resize();

    std::mutex _poolMutex; ///< Protects _pool
    std::vector<std::shared_ptr<PoolEventThread>> _pool; ///< All the threads in our pool.
    uint _targetThrdCount{0}; ///< How many threads we want to have.
    std::mutex _countMutex; ///< protects _targetThrdCount
    std::condition_variable _countCV; ///< Notifies about changes to _pool size, uses _countMutex.
    CommandQueue::Ptr _q; ///< The queue used by all threads in the _pool.

};

}}} // namespace




#endif /* LSST_QSERV_UTIL_EVENTTHREAD_H_ */
