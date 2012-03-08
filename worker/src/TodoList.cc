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
 
#include "lsst/qserv/worker/TodoList.h"
#include <boost/regex.hpp>

#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/TaskMsgDigest.h"

namespace qWorker = lsst::qserv::worker;

////////////////////////////////////////////////////////////////////////
// anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {
#if 0
    boost::shared_ptr<qWorker::QueryRunnerArg> 
    convertToArg(qWorker::TodoList::TaskMsgPtr const m) {
    }
#endif
    typedef std::vector<int> IntVector;
    typedef boost::shared_ptr<IntVector> IntVectorPtr;
}
////////////////////////////////////////////////////////////////////////
// Helpers for TodoList
////////////////////////////////////////////////////////////////////////
namespace {
    struct taskMatch {
        taskMatch(qWorker::TodoList::MatchF& f_) : f(f_) {}
        inline bool operator()(boost::shared_ptr<qWorker::Task>& t) {
            if(t.get() && t->msg.get()) {
                return f(*(t->msg));
            } else { return false; }
        }
        qWorker::TodoList::MatchF& f;
    };
    
    struct hashMatch {
        hashMatch(std::string const& hash_) : hash(hash_) {}
        inline bool operator()(boost::shared_ptr<qWorker::Task>& t) {
            if(t.get()) {
                return t->hash == hash;
            } else { return false; }
        }
        std::string const hash;
    };
    struct eqMatch {
        eqMatch(qWorker::Task::Ptr const& task_) : task(task_) {}
        inline bool operator()(boost::shared_ptr<qWorker::Task>& t) {
            if(t.get()) {
                return t == task;
            } else { return false; }
        }
        qWorker::Task::Ptr const& task;
    };

    struct chunkMatch {
        chunkMatch(int chunk_) : chunk(chunk_) {}
        inline bool operator()(boost::shared_ptr<qWorker::Task>& t) {
            if(t.get() && t->msg.get() && t->msg->has_chunkid()) {
                return t->msg->chunkid() == chunk;
            } else { return false; }
        }
        int chunk;
    };

    // queue type and condition functor
    template <typename Q, typename C> 
    boost::shared_ptr<qWorker::Task> 
    popAndReturn(boost::mutex& m, Q& q, C c) {
        boost::lock_guard<boost::mutex> mutex(m);
        typename Q::iterator i;
        i = std::find_if(q.begin(), q.end(), c);
        if(i != q.end()) {
            typename Q::value_type v = *i;
            q.erase(i);
            return v;
        }
        // Consider moving task to a "running" list for tracking.
        return typename Q::value_type();
    }
}
////////////////////////////////////////////////////////////////////////
// TodoList implementation
////////////////////////////////////////////////////////////////////////
bool qWorker::TodoList::accept(boost::shared_ptr<TaskMsg> msg) {
    Task::Ptr t(new Task(msg));
    {
        boost::lock_guard<boost::mutex> m(_tasksMutex);
        _tasks.push_back(t);
    }
    _notifyWatchers(t); 
}
void qWorker::TodoList::addWatcher(Watcher::Ptr w) {
    boost::lock_guard<boost::mutex> m(_watchersMutex);
    _watchers.push_back(w);
}
void qWorker::TodoList::removeWatcher(Watcher::Ptr w) {
    boost::lock_guard<boost::mutex> m(_watchersMutex);
    WatcherQueue::iterator i = _watchers.begin();
    for(; i != _watchers.end(); ++i) {
        if(*i == w) {
            _watchers.erase(i);
            return;
        }
    }
    assert(false); // FIXME: prefer a meaningful error.
}

boost::shared_ptr<qWorker::Task> qWorker::TodoList::popTask() {
    boost::lock_guard<boost::mutex> mutex(_tasksMutex);
    if(!_tasks.empty()) {
        boost::shared_ptr<qWorker::Task> t = _tasks.front();
        _tasks.pop_front();
        return t;
    } else { return boost::shared_ptr<qWorker::Task>(); }
}


boost::shared_ptr<qWorker::Task> 
qWorker::TodoList::popTask(qWorker::TodoList::MatchF& m) {
    return popAndReturn(_tasksMutex, _tasks, taskMatch(m));
}

boost::shared_ptr<qWorker::Task> 
qWorker::TodoList::popTask(qWorker::Task::Ptr t) {
    return popAndReturn(_tasksMutex, _tasks, eqMatch(t));
}

boost::shared_ptr<qWorker::Task>
qWorker::TodoList::popByHash(std::string const& hash) {
    return popAndReturn(_tasksMutex, _tasks, hashMatch(hash));
}

boost::shared_ptr<qWorker::Task>
qWorker::TodoList::popByChunk(int chunkId) {
    return popAndReturn(_tasksMutex, _tasks, chunkMatch(chunkId));
}


void qWorker::TodoList::_notifyWatchers(Task::Ptr t) {
    boost::lock_guard<boost::mutex> m(_watchersMutex);
    for(WatcherQueue::iterator i = _watchers.begin();
        i != _watchers.end(); ++i) {
        (**i).handleAccept(t);
    }
}
