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
#include <deque>
#include <queue>
#include <thread>
#include <vector>

// Qserv headers
#include "util/Command.h"

namespace lsst {
namespace qserv {
namespace util {

///
// The classes in this header are meant to provide the basis for easy to use event
// driven threads. A basic CommandQueue is a simple thread safe fifo, but derived
// classes can overload the member functions and be very complicated schedulers
// (see wsched::ScanScheduler, wsched::BlendScheduler, and wsched::GroupScheduler).
//
// The basic EventThread just runs whatever Command its CommandQueue object hands it.
// It tells the CommandQueue when each command starts and finishes, which can be
// ignored. In the case where a Command needs to know something about the EventThread
// it is running on, specialAction can be overriden.
//
// The ThreadPool is easy to use and conceptually simple, but has some unusual features
// that complicate implementation. A ThreadPool is composed of some number of
// PoolEventThread's that all share a CommandQueue. A Command placed on the CommandQueue
// may be run by any thread in the pool. A PoolEventThread is simply an EventThread
// that is aware that it is part of a ThreadPool.
//
// The ThreadPool starts getting complicated in that there are the unusual cases where
// a Command decides it should no longer be part of the pool or a CommandQueue scheduler
// decides that it should no longer wait for that Command to complete. For this there is
// CommandThreadPool. It is a Command that knows it is in a thread pool and has the
// capability to have the PoolEventThread leave its ThreadPool. When this is done, the
// CommandQueue is told the Command is complete and the ThreadPool creates a replacement
// PoolEventThread.
//
// The primary case for a thread deciding to leave is that it has reached a point where it
// expects to need significantly fewer resources than it was using before (such as waiting
// for something slow to complete before finishing a task). And the primary case for a
// CommandQueue derived scheduler to cause a Command and its PoolEventThread to leave the
// ThreadPool is that the command is too slow for that scheduler.
//


/// A queue of Commands meant to drive an EventThread.
///
class CommandQueue {
public:
    using Ptr = std::shared_ptr<CommandQueue>;
    virtual ~CommandQueue() {};
    /// Queue a command object in a thread safe way and signal any threads
    /// waiting on the queue that a command is available.
    virtual void queCmd(Command::Ptr const& cmd) {
        std::lock_guard<std::mutex> lock(_mx);
        _qu.push_back(cmd);
        notify(_qu.size() > 1);
    };

    /// Get a command off the queue.
    /// If wait is true, wait until a message is available.
    virtual Command::Ptr getCmd(bool wait=true) {
        std::unique_lock<std::mutex> lock(_mx);
        if (wait) {
            _cv.wait(lock, [this](){return !_qu.empty();});
        }
        if (_qu.empty()) {
            return nullptr;
        }
        auto cmd = _qu.front();
        _qu.pop_front();
        return cmd;
    };

    /// Notify all threads waiting on this queue, or just 1 if all is false.
    virtual void notify(bool all=true) {
        if (all) {
            _cv.notify_all();
        } else {
            _cv.notify_one();
        }
    }

    virtual void commandStart(Command::Ptr const&) {}; //< Derived methods must be thread safe.
    virtual void commandFinish(Command::Ptr const&) {}; //< Derived methods must be thread safe.

protected:
    std::deque<Command::Ptr> _qu{};
    std::condition_variable  _cv{};
    mutable std::mutex       _mx{};
};

/// An event driven thread, the event loop is in handleCmds().
/// Thread must be started with run(). Stop the thread by calling queEnd().
class EventThread : public CmdData {
public:
    typedef std::shared_ptr<EventThread> Ptr;
    enum { HALT = -1000 };
    EventThread() {}
    explicit EventThread(CommandQueue::Ptr const& q) : _q{q} {}
    EventThread(EventThread const&) = delete;
    EventThread& operator=(EventThread const&) = delete;
    virtual ~EventThread() {}

    void handleCmds(); //< Event loop!!!
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
            void action(CmdData *data) override {
                auto thisEventThread = dynamic_cast<EventThread*>(data);
                if (thisEventThread != nullptr) {
                    thisEventThread->_loop = false;
                }
            }
        };
        std::shared_ptr<MsgEnd> cmd = std::make_shared<MsgEnd>();
        queCmd(cmd);
    }

    Command* getCurrentCommand() const { return _currentCommand; }

protected:
    virtual void startup() {}  ///< Things to do when the thread is starting up.
    virtual void finishup() {} ///< things to do when the thread is closing down.
    virtual void specialActions(Command::Ptr const& cmd) {} ///< Things to do before running a command.
    void callCommandFinish(Command::Ptr const& cmd); //< Limit commandFinish() to be called once per loop.

    CommandQueue::Ptr _q{std::make_shared<CommandQueue>()}; ///< Queue of commands.
    std::thread _t;            //< Our thread.
    std::atomic<bool> _loop {true}; //< Keep running the event loop while this is true.
    std::atomic<bool> _commandFinishCalled{false}; //< flag to prevent multiple calls to commandFinish.
    Command::Ptr _getCurrentCommandPtr() const { return _cmd; } //< not thread safe.

private:
    Command::Ptr _cmd;
    std::atomic<Command*> _currentCommand{nullptr};
};


class EventThreadJoiner {
public:
    EventThreadJoiner();

    ~EventThreadJoiner() {
        _continue = false;
    }

    void joinLoop() {
        EventThread::Ptr pet;
        while(_continue) {
            {
                std::lock_guard<std::mutex> lg;
                if(!_eventThreads.empty()) {
                    pet = _eventThreads.front();
                    _eventThreads.pop();
                }
                pet->join();
            } else {
                sleep(_sleepTime);
            }
        }
    }

    void addThread(EventThread::Ptr const& eventThread) {
        if (eventThread == nullPtr) return;
        std::lock_guard<std::mutex> lg;
        ++_count;
        _eventThreads.push(eventThread);
    }



private:
    std::atomic<bool> _continue{true};
    int _count{0};
    int sleepTime{100000};
    std::queue<EventThread::Ptr> _eventThreads;
    std::mutex _mtxJoiner;
    std::thread _tJoiner;
};


class ThreadPool;


/// An EventThread to be used by the ThreadPool class.
/// finishup() is used to tell the ThreadPool that this thread is finished.
class PoolEventThread : public EventThread, public std::enable_shared_from_this<PoolEventThread> {
public:
    using Ptr = std::shared_ptr<PoolEventThread>;

    static PoolEventThread::Ptr newPoolEventThread(std::shared_ptr<ThreadPool> const& threadPool,
                                                   CommandQueue::Ptr const& q);
    ~PoolEventThread();

    void leavePool();
    bool leavePool(Command::Ptr const& cmd);

protected:
    void specialActions(Command::Ptr const& cmd) override;
    void finishup() override;
    std::shared_ptr<ThreadPool> _threadPool;
    std::atomic<bool> _finishupOnce{false}; //< Ensure finishup() only called once.

private:
    PoolEventThread(std::shared_ptr<ThreadPool> const& threadPool, CommandQueue::Ptr const& q);
};


/// A Command that is aware that it is running as part of a PoolEventThread,
// which allows it to tell the event thread and pool to take special actions.
class CommandThreadPool : public CommandTracked {
public:
    using Ptr = std::shared_ptr<CommandThreadPool>;

    CommandThreadPool() {}
    CommandThreadPool(std::function<void(CmdData*)> func) : Command{func} {}

    PoolEventThread::Ptr getAndNullPoolEventThread();

    friend class PoolEventThread;
private:
    void _setPoolEventThread(PoolEventThread::Ptr const& poolEventThread);

    std::weak_ptr<PoolEventThread> _poolEventThread;
};


/// ThreadPool is a variable size pool of threads all fed by the same CommandQueue.
/// Growing the pool is simple, shrinking the pool is complex. Both operations should
/// have no effect on items running or on the queue.
/// Note: endAll() MUST be called to shutdown the ThreadPool. There are self
///   referential pointer between ThreadPool and its PoolEventThread's. This is a lesser
///   evil than having PoolEventThread's running without their ThreadPool.
class ThreadPool : public std::enable_shared_from_this<ThreadPool> {
public:
    using Ptr = std::shared_ptr<ThreadPool>;
    static ThreadPool::Ptr newThreadPool(unsigned int thrdCount,
                                         CommandQueue::Ptr const& q) {
        Ptr thp{new ThreadPool{thrdCount, q}}; // private constructor
        thp->_resize();
        return thp;
    }
    virtual ~ThreadPool();

    CommandQueue::Ptr getQueue() {return _q;}
    unsigned int getTargetThrdCount() {
        std::lock_guard<std::mutex> lock(_countMutex);
        return _targetThrdCount;
    }
    unsigned int size() {
        std::lock_guard<std::mutex> lock(_poolMutex);
        return _pool.size();
    }

    void waitForResize(int millisecs);
    void endAll() { resize(0); }
    void resize(unsigned int targetThrdCount);
    PoolEventThread::Ptr release(PoolEventThread *thread);

protected:
    ThreadPool(unsigned int thrdCount, CommandQueue::Ptr const& q) : _targetThrdCount{thrdCount}, _q{q} {
        if (_q == nullptr) {
            _q = std::make_shared<CommandQueue>();
        }
    }
    void _resize();

    std::mutex _poolMutex; ///< Protects _pool
    std::vector<std::shared_ptr<PoolEventThread>> _pool; ///< All the threads in our pool.
    unsigned int _targetThrdCount{0}; ///< How many threads we want to have.
    std::mutex _countMutex; ///< protects _targetThrdCount
    std::condition_variable _countCV; ///< Notifies about changes to _pool size, uses _countMutex.
    CommandQueue::Ptr _q; ///< The queue used by all threads in the _pool.

};


}}} // namespace lsst:qserv:util




#endif /* LSST_QSERV_UTIL_EVENTTHREAD_H_ */
