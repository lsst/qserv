// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2016 LSST Corporation.
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

#ifndef LSST_QSERV_WCONTROL_FOREMAN_H
#define LSST_QSERV_WCONTROL_FOREMAN_H

// System headers
#include <atomic>
#include <memory>

// Qserv headers
#include "util/EventThread.h"
#include "wbase/MsgProcessor.h"
#include "wbase/Task.h"

// Forward declarations
namespace lsst::qserv::mysql {
class MySqlConfig;
}

namespace lsst::qserv::wconfig {
class WorkerConfig;
}

namespace lsst::qserv::wdb {
class ChunkResourceMgr;
class QueryRunner;
}  // namespace lsst::qserv::wdb

namespace lsst::qserv::wpublish {
class QueriesAndChunks;
}

namespace lsst::qserv::wcontrol {

class SqlConnMgr;
class TransmitMgr;

/// An abstract scheduler interface. Foreman objects use Scheduler instances
/// to determine what tasks to launch upon triggering events.
class Scheduler : public wbase::TaskScheduler, public util::CommandQueue {
public:
    /// Smart pointer type for objects of this class
    using Ptr = std::shared_ptr<Scheduler>;

    /// The destructor
    virtual ~Scheduler() {}

    virtual std::string getName() const = 0;  //< @return the name of the scheduler.

    /// Take appropriate action when a task in the Schedule is cancelled. Doing
    /// nothing should be harmless, but some Schedulers may work better if cancelled
    /// tasks are removed.
    void taskCancelled(wbase::Task* task) override {}
};

/// Foreman is used to maintain a thread pool and schedule Tasks for the thread pool.
/// It also manages sub-chunk tables with the ChunkResourceMgr.
/// The schedulers may limit the number of threads they will use from the thread pool.
class Foreman : public wbase::MsgProcessor {
public:
    /**
     * @param scheduler    - pointer to the scheduler
     * @param poolSize     - size of the thread pool
     * @param mySqlConfig  - configuration object for the MySQL service
     * @param queries      - query statistics collector
     * @param sqlConnMgr   - for limiting the number of MySQL connections used for tasks
     * @param transmitMgr  - for throttling outgoing massages to prevent czars from being overloaded
     * @param workerConfig - worker configuration parameters
     */
    Foreman(Scheduler::Ptr const& scheduler, unsigned int poolSize, unsigned int maxPoolThreads,
            mysql::MySqlConfig const& mySqlConfig, std::shared_ptr<wpublish::QueriesAndChunks> const& queries,
            std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr,
            std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr,
            wconfig::WorkerConfig const& workerConfig);

    virtual ~Foreman();

    // This class doesn't have the default construction or copy semantics
    Foreman() = delete;
    Foreman(Foreman const&) = delete;
    Foreman& operator=(Foreman const&) = delete;

    std::shared_ptr<wdb::ChunkResourceMgr> const& chunkResourceMgr() const { return _chunkResourceMgr; }
    mysql::MySqlConfig const& mySqlConfig() const { return _mySqlConfig; }
    std::shared_ptr<wcontrol::SqlConnMgr> const& sqlConnMgr() const { return _sqlConnMgr; }
    std::shared_ptr<wcontrol::TransmitMgr> const& transmitMgr() const { return _transmitMgr; }
    wconfig::WorkerConfig const& workerConfig() const { return _workerConfig; }

    /// Process a group of query processing tasks.
    /// @see MsgProcessor::processTasks()
    void processTasks(std::vector<std::shared_ptr<wbase::Task>> const& tasks) override;

    /// Implement the corresponding method of the base class
    /// @see MsgProcessor::processCommand()
    void processCommand(std::shared_ptr<wbase::WorkerCommand> const& command) override;

    nlohmann::json statusToJson() override;

private:
    std::shared_ptr<wdb::ChunkResourceMgr> _chunkResourceMgr;

    util::ThreadPool::Ptr _pool;
    Scheduler::Ptr _scheduler;

    util::CommandQueue::Ptr _workerCommandQueue;  ///< dedicated queue for the worker commands
    util::ThreadPool::Ptr _workerCommandPool;     ///< dedicated pool for executing worker commands

    mysql::MySqlConfig const& _mySqlConfig;
    std::shared_ptr<wpublish::QueriesAndChunks> const _queries;

    /// For limiting the number of MySQL connections used for tasks.
    std::shared_ptr<wcontrol::SqlConnMgr> const _sqlConnMgr;

    /// Used to throttle outgoing massages to prevent czars from being overloaded.
    std::shared_ptr<wcontrol::TransmitMgr> const _transmitMgr;

    /// Worker configuration parameters.
    wconfig::WorkerConfig const& _workerConfig;
};

}  // namespace lsst::qserv::wcontrol

#endif  // LSST_QSERV_WCONTROL_FOREMAN_H
