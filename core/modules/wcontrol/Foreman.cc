// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 AURA/LSST.
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

// Class header
#include "wcontrol/Foreman.h"

// System headers
#include <cassert>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "wbase/Base.h"
#include "wconfig/Config.h"
#include "wcontrol/RunnerMgr.h"
#include "wdb/ChunkResource.h"


namespace lsst {
namespace qserv {
namespace wcontrol {

Foreman::Ptr Foreman::newForeman(Scheduler::Ptr const& sched) {
    return std::make_shared<Foreman>(sched);
}

Foreman::Foreman(Scheduler::Ptr const& s) : _scheduler{s} {
    // Make the chunk resource mgr
    mysql::MySqlConfig c(wconfig::getConfig().getSqlConfig());
    _chunkResourceMgr = wdb::ChunkResourceMgr::newMgr(c);
    _rManager.reset(new RunnerMgr(*this));
    assert(s); // Cannot operate without scheduler.
}
Foreman::~Foreman() {
    LOGF(_log, LOG_LVL_DEBUG, "Foreman::~Foreman()");
    // FIXME: Cancel and drain runners. It will take significant effort to have xrootd shutdown cleanly
    // and this will never get called until that happens, making this a very low priority item.
    // This should only (get called on shutdown/restart).
}

/// Create and queue a Task from a TaskMsg and a replyChannel.
wbase::Task::Ptr Foreman::processMsg(std::shared_ptr<proto::TaskMsg> const& taskMsg,
                                     std::shared_ptr<wbase::SendChannel> const& replyChannel) {
    auto task = std::make_shared<wbase::Task>(taskMsg, replyChannel);
    newTaskAction(task);
    return task;
}

void Foreman::_startRunner(wbase::Task::Ptr const& t) {
    auto f = [this](wbase::Task::Ptr t) {
        LOGF_DEBUG("Foreman::_startRunner start");
        RunnerMgr *rm = this->_rManager.get();
        Runner::Ptr rp = std::make_shared<Runner>(rm, t);
        (*rp)();
        LOGF_DEBUG("Foreman::_startRunner end");
    };
    std::thread thrd{f, t};
    thrd.detach();
}

void Foreman::newTaskAction(wbase::Task::Ptr const& task) {
    // Pass to scheduler.
    assert(_scheduler);
    auto newReady = _rManager->queueTask(task, _scheduler);
    // Perform only what the scheduler requests.
    if(newReady.get() && (newReady->size() > 0)) {
        wbase::TaskQueue::iterator i = newReady->begin();
        for(; i != newReady->end(); ++i) {
            _startRunner(*i);
        }
    }
}

}}} // namespace
