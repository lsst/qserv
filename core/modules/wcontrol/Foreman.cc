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
#include "proto/worker.pb.h"
#include "wbase/Base.h"
#include "wbase/SendChannel.h"
#include "wconfig/Config.h"
#include "wdb/ChunkResource.h"
#include "wdb/QueryRunner.h"


namespace lsst {
namespace qserv {
namespace wcontrol {

Foreman::Ptr Foreman::newForeman(Scheduler::Ptr const& sched, uint poolSize) {
    return std::make_shared<Foreman>(sched, poolSize);
}

Foreman::Foreman(Scheduler::Ptr const& s, uint poolSize) : _scheduler{s} {
    // Make the chunk resource mgr
    mysql::MySqlConfig c(wconfig::getConfig().getSqlConfig());
    _chunkResourceMgr = wdb::ChunkResourceMgr::newMgr(c);
    assert(s); // Cannot operate without scheduler.

    LOGF_DEBUG("poolSize=%1%" % poolSize);
    _pool = util::ThreadPool::newThreadPool(poolSize, _scheduler);
}

Foreman::~Foreman() {
    LOGF(_log, LOG_LVL_DEBUG, "Foreman::~Foreman()");
    // It will take significant effort to have xrootd shutdown cleanly and this will never get called
    // until that happens.
    _pool->endAll();
}

/// Create and queue a Task from a TaskMsg and a replyChannel.
wbase::Task::Ptr Foreman::processMsg(std::shared_ptr<proto::TaskMsg> const& taskMsg,
                                     std::shared_ptr<wbase::SendChannel> const& replyChannel) {
    auto task = std::make_shared<wbase::Task>(taskMsg, replyChannel);
    auto func = [this, task](){
        proto::TaskMsg const& msg = *task->msg;
        int const resultProtocol = 2; // See proto/worker.proto Result protocol
        if(!msg.has_protocol() || msg.protocol() < resultProtocol) {
            task->sendChannel->sendError("Unsupported wire protocol", 1);
        } else {
            auto qr = _newQueryRunner(task);
            qr->runQuery();
        }
    };
    task->setFunc(func);
    _scheduler->queCmd(task);
    return task;
}

std::shared_ptr<wdb::QueryRunner> Foreman::_newQueryRunner(wbase::Task::Ptr const& t) {
    wdb::QueryRunnerArg a(_log, t, _chunkResourceMgr);
    auto qa = wdb::QueryRunner::newQueryRunner(a);
    return qa;
}

}}} // namespace
