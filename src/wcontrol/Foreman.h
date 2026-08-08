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
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/EventThread.h"
#include "util/HoldTrack.h"
#include "wbase/Base.h"
#include "wbase/MsgProcessor.h"
#include "wbase/Task.h"

// Forward declarations

namespace lsst::qserv::wbase {
struct TaskSelector;
}  // namespace lsst::qserv::wbase

namespace lsst::qserv::wcontrol {
class ResourceMonitor;
class SqlConnMgr;
}  // namespace lsst::qserv::wcontrol

namespace lsst::qserv::qhttp {
class Server;
}  // namespace lsst::qserv::qhttp

namespace lsst::qserv::wdb {
class QueryRunner;
}  // namespace lsst::qserv::wdb

namespace lsst::qserv::wpublish {
class ChunkInventory;
class QueriesAndChunks;
}  // namespace lsst::qserv::wpublish

// This header declarations

namespace lsst::qserv::wcontrol {

/// An abstract scheduler interface. Foreman objects use Scheduler instances
/// to determine what tasks to launch upon triggering events.
class Scheduler : public wbase::TaskScheduler, public util::CommandQueue {
public:
    /// Smart pointer type for objects of this class
    using Ptr = std::shared_ptr<Scheduler>;

    /// The destructor
    virtual ~Scheduler() {}

    /// Take appropriate action when a task in the Schedule is cancelled. Doing
    /// nothing should be harmless, but some Schedulers may work better if cancelled
    /// tasks are removed.
    /// Future: Find the task and remove it from the queue, then start a
    ///  thread to run the task, or add it to a thread that just runs tasks in FIFO
    ///  order, maybe after the thread checks that cancelled has been set. The
    ///  cancelled tasks should run very quickly.
    void taskCancelled(wbase::Task* task) override {}
};

/// Foreman is used to maintain a thread pool and schedule Tasks for the thread pool.
/// The schedulers may limit the number of threads they will use from the thread pool.
class Foreman : public wbase::MsgProcessor {
public:
    /**
     * @param scheduler              - pointer to the scheduler
     * @param poolSize               - size of the thread pool
     * @param mySqlConfig            - configuration object for the MySQL service
     * @param queries                - query statistics collector
     * @param chunkInventory         - a collection of the SSI resources published by the worker
     * @param sqlConnMgr             - for limiting the number of MySQL connections used for tasks
     */
    Foreman(Scheduler::Ptr const& scheduler, unsigned int poolSize, unsigned int maxPoolThreads,
            mysql::MySqlConfig const& mySqlConfig, std::shared_ptr<wpublish::QueriesAndChunks> const& queries,
            std::shared_ptr<wpublish::ChunkInventory> const& chunkInventory,
            std::shared_ptr<SqlConnMgr> const& sqlConnMgr);

    virtual ~Foreman() override;

    // This class doesn't have the default construction or copy semantics
    Foreman() = delete;
    Foreman(Foreman const&) = delete;
    Foreman& operator=(Foreman const&) = delete;

    mysql::MySqlConfig const& mySqlConfig() const { return _mySqlConfig; }
    std::shared_ptr<wpublish::QueriesAndChunks> const& queriesAndChunks() const { return _queries; }
    std::shared_ptr<wpublish::ChunkInventory> const& chunkInventory() const { return _chunkInventory; }
    std::shared_ptr<SqlConnMgr> const& sqlConnMgr() const { return _sqlConnMgr; }
    std::shared_ptr<ResourceMonitor> const& resourceMonitor() const { return _resourceMonitor; }

    uint16_t httpPort() const;

    /// Process a group of query processing tasks.
    /// @see MsgProcessor::processTasks()
    void processTasks(std::vector<std::shared_ptr<wbase::Task>> const& tasks) override;

    /// Implement the corresponding method of the base class
    /// @see MsgProcessor::processCommand()
    void processCommand(std::shared_ptr<wbase::WorkerCommand> const& command) override;

    /// Implement the corresponding method of the base class
    /// @see MsgProcessor::statusToJson()
    virtual nlohmann::json statusToJson(wbase::TaskSelector const& taskSelector) override;

private:
    util::ThreadPool::Ptr _pool;
    Scheduler::Ptr _scheduler;

    util::CommandQueue::Ptr _workerCommandQueue;  ///< dedicated queue for the worker commands
    util::ThreadPool::Ptr _workerCommandPool;     ///< dedicated pool for executing worker commands

    mysql::MySqlConfig const _mySqlConfig;
    std::shared_ptr<wpublish::QueriesAndChunks> const _queries;
    std::shared_ptr<wpublish::ChunkInventory> const _chunkInventory;

    /// For limiting the number of MySQL connections used for tasks.
    std::shared_ptr<SqlConnMgr> const _sqlConnMgr;

    util::HoldTrack::Mark::Ptr _mark;

    /// A a counter of the XROOTD/SSI resources which are in use at any given moment
    /// of time by the worker.
    std::shared_ptr<ResourceMonitor> const _resourceMonitor;

    /// BOOST ASIO services needed to run the HTTP server
    boost::asio::io_service _io_service;

    /// The HTTP server for serving/managing result files
    std::shared_ptr<qhttp::Server> const _httpServer;
};

}  // namespace lsst::qserv::wcontrol

#endif  // LSST_QSERV_WCONTROL_FOREMAN_H
