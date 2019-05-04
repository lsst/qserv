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
#include "mysql/MySqlConfig.h"
#include "util/EventThread.h"
#include "wbase/Base.h"
#include "wbase/MsgProcessor.h"
//#include "wbase/Task.h"
#include "wpublish/QueriesAndChunks.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace wdb {
    class SQLBackend;
    class ChunkResourceMgr;
    class QueryRunner;
}}}

namespace lsst {
namespace qserv {
namespace wcontrol {

/// An abstract scheduler interface. Foreman objects use Scheduler instances
/// to determine what tasks to launch upon triggering events.
class Scheduler
    :   public wbase::TaskScheduler,
        public util::CommandQueue {

public:

    /// Smart pointer type for objects of this class
    using Ptr = std::shared_ptr<Scheduler>;

    /// The destructor
    virtual ~Scheduler() {}

    virtual std::string getName() const = 0; //< @return the name of the scheduler.

    /// Take appropriate action when a task in the Schedule is cancelled. Doing
    /// nothing should be harmless, but some Schedulers may work better if cancelled
    /// tasks are removed.
    void taskCancelled(wbase::Task *task) override {}
};

/// Foreman is used to maintain a thread pool and schedule Tasks for the thread pool.
/// It also manages sub-chunk tables with the ChunkResourceMgr.
/// The schedulers may limit the number of threads they will use from the thread pool.
class Foreman
    :   public wbase::MsgProcessor {

public:

    /**
     * @param scheduler   - pointer to the scheduler
     * @param poolSize    - size of the thread pool
     * @param mySqlConfig - configuration object for the MySQL service
     * @param queries     - query statistics collector
     */
    Foreman(Scheduler::Ptr                  const& scheduler,
            uint                                   poolSize,
            mysql::MySqlConfig              const& mySqlConfig,
            wpublish::QueriesAndChunks::Ptr const& queries);

    virtual ~Foreman();

    // This class doesn't have the default construction or copy semantics
    Foreman() = delete;
    Foreman(Foreman const&) = delete;
    Foreman& operator=(Foreman const&) = delete;

    /**
     * Implement the corresponding method of the base class
     *
     * @see MsgProcessor::processTask()
     */
    void processTask(std::shared_ptr<wbase::Task> const& task) override;

   /**
     * Implement the corresponding method of the base class
     *
     * @see MsgProcessor::processCommand()
     */
    void processCommand(std::shared_ptr<wbase::WorkerCommand> const& command) override;

    nlohmann::json statusToJson() override;

private:

    std::shared_ptr<wdb::SQLBackend>       _backend;
    std::shared_ptr<wdb::ChunkResourceMgr> _chunkResourceMgr;

    util::ThreadPool::Ptr _pool;
    Scheduler::Ptr        _scheduler;

    util::CommandQueue::Ptr _workerCommandQueue;    ///< dedicated queue for the worker commands
    util::ThreadPool::Ptr   _workerCommandPool;     ///< dedicated pool for executing worker commands

    mysql::MySqlConfig const        _mySqlConfig;
    wpublish::QueriesAndChunks::Ptr _queries;
};

}}}  // namespace lsst::qserv::wcontrol

#endif // LSST_QSERV_WCONTROL_FOREMAN_H
