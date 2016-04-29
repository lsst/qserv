// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2016 AURA/LSST.
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
#include "wdb/ChunkResource.h"
#include "wdb/QueryRunner.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wcontrol.Foreman");
}

namespace lsst {
namespace qserv {
namespace wcontrol {

Foreman::Foreman(Scheduler::Ptr const& s, uint poolSize, mysql::MySqlConfig const& mySqlConfig)
    : _scheduler{s}, _mySqlConfig(mySqlConfig) {
    // Make the chunk resource mgr
    _chunkResourceMgr = wdb::ChunkResourceMgr::newMgr(_mySqlConfig);
    assert(s); // Cannot operate without scheduler.

    LOGS(_log, LOG_LVL_DEBUG, "poolSize=" << poolSize);
    _pool = util::ThreadPool::newThreadPool(poolSize, _scheduler);
}

Foreman::~Foreman() {
    LOGS(_log, LOG_LVL_DEBUG, "Foreman::~Foreman()");
    // It will take significant effort to have xrootd shutdown cleanly and this will never get called
    // until that happens.
    _pool->endAll();
}

/// Put the task on the scheduler to be run later.
void Foreman::processTask(std::shared_ptr<wbase::Task> const& task) {

    auto func = [this, task](util::CmdData*){
        proto::TaskMsg const& msg = *task->msg;
        int const resultProtocol = 2; // See proto/worker.proto Result protocol
        if (!msg.has_protocol() || msg.protocol() < resultProtocol) {
            LOGS(_log, LOG_LVL_WARN, "processMsg Unsupported wire protocol");
            if (!task->getCancelled()) {
                // We should not send anything back to xrootd if the task has been cancelled.
                task->sendChannel->sendError("Unsupported wire protocol", 1);
            }
        } else {
            auto qr = wdb::QueryRunner::newQueryRunner(task, _chunkResourceMgr, _mySqlConfig);
            qr->runQuery();
        }
    };

    task->setFunc(func);
    _scheduler->queCmd(task);
}

}}} // namespace
