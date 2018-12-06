/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_HTTPTASK_H
#define LSST_QSERV_HTTPTASK_H

// System headers
#include <functional>
#include <set>

// Qserv headers
#include "qhttp/Server.h"
#include "replica/Task.h"
#include "replica/DeleteWorkerTask.h"
#include "replica/HealthMonitorTask.h"
#include "replica/ReplicationTask.h"
#include "util/Mutex.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpTask represents a task which runs the built-in HTTP server
 * responding to the REST API for managing the Replication Controller
 * and responding to various information retrieval requests.
 */
class HttpTask
    :   public Task {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<HttpTask> Ptr;

    // Default construction and copy semantics are prohibited

    HttpTask() = delete;
    HttpTask(HttpTask const&) = delete;
    HttpTask& operator=(HttpTask const&) = delete;

    ~HttpTask() final = default;

    /**
     * Create a new task with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param controller
     *   a reference to the Controller for launching requests, jobs, etc.
     *
     * @param onTerminated
     *   callback function to be called upon abnormal termination
     *   of the task. Set it to 'nullptr' if no call back should be made.
     *
     * @param onWorkerEvict
     *   callback function to be called when one or more workers
     *   were requested to be explicitly evicted from the cluster.
     *
     * @param healthMonitorTask
     *   a reference to the Cluster Health Monitoring task is made visible
     *   in a context of this lass to allow suspending/resuming it as needed.
     *
     * @param replicationTask
     *   a reference to the Linear Replication Loop task is made visible
     *   in a context of this lass to allow suspending/resuming it as needed.
     *
     * @param deleteWorkerTask
     *   a reference to the Worker Eviction task is made visible
     *   in a context of this lass to allow suspending/resuming it as needed.
     *
     * @return
     *   the smart pointer to a new object
     */
    static Ptr create(Controller::Ptr const& controller,
                      Task::AbnormalTerminationCallbackType const& onTerminated,
                      HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
                      HealthMonitorTask::Ptr const& healthMonitorTask,
                      ReplicationTask::Ptr const& replicationTask,
                      DeleteWorkerTask::Ptr const& deleteWorkerTask);

protected:

    /**
     * @see Task::run()
     */
    void run() final;

private:

    /**
     * The constructor is available to the class's factory method
     *
     * @see HttpTask::create()
     */
    HttpTask(Controller::Ptr const& controller,
             Task::AbnormalTerminationCallbackType const& onTerminated,
             HealthMonitorTask::WorkerEvictCallbackType const& onWorkerEvict,
             HealthMonitorTask::Ptr const& healthMonitorTask,
             ReplicationTask::Ptr const& replicationTask,
             DeleteWorkerTask::Ptr const& deleteWorkerTask);

    // -------------------------------------
    // Callback for processing test requests
    // -------------------------------------

    /**
     * Process "POST" requests
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _testCreate(qhttp::Request::Ptr req,
                     qhttp::Response::Ptr resp);

    /**
     * Process "GET" requests for a list of resources of the given type
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _testList(qhttp::Request::Ptr req,
                   qhttp::Response::Ptr resp);

    /**
     * Process "GET" requests for a specific resource
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _testGet(qhttp::Request::Ptr req,
                  qhttp::Response::Ptr resp);

    /**
     * Process "PUT" requests for a specific resource
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _testUpdate(qhttp::Request::Ptr req,
                     qhttp::Response::Ptr resp);

    /**
     * Process "DELETE" requests for a specific resource
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _testDelete(qhttp::Request::Ptr req,
                     qhttp::Response::Ptr resp);


    // ---------------------------------------
    // Callback for processing actual requests
    // ---------------------------------------

    /**
     * Process a request which return status of one worker.
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _getWorkerStatus(qhttp::Request::Ptr req,
                          qhttp::Response::Ptr resp);

    /**
     * Process a request which return the status of the replicas.
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _getReplicationLevel(qhttp::Request::Ptr req,
                              qhttp::Response::Ptr resp);

    /**
     * Process a request which return status of all workers.
     *
     * @param req   request received from a client
     * @param resp  response to be sent back
     */
    void _listWorkerStatuses(qhttp::Request::Ptr req,
                             qhttp::Response::Ptr resp);

private:

    /// The callback to be called when there is a request to evict one
    // or many workers from the cluster.
    HealthMonitorTask::WorkerEvictCallbackType const _onWorkerEvict;

    // References(!) to smart pointers to other tasks which can be managed
    // by this class.
    //
    // @note
    //   References to the pointers are used to avoid increasing the reference
    //   counters to the objects.

    HealthMonitorTask::Ptr const& _healthMonitorTask;
    ReplicationTask::Ptr   const& _replicationTask;
    DeleteWorkerTask::Ptr  const& _deleteWorkerTask;

    /// The server for processing REST requests
    qhttp::Server::Ptr const _httpServer;

    /// The flag used for lazy initialization of the Web server the first time
    /// this task runs.
    bool _isInitialized;

    /// The latest state of the replication levels report
    std::string _replicationLevelReport;
    
    /// The timestamp for when the last report was made
    uint64_t _replicationLevelReportTimeMs;

    /// Mutex guarding the cache with the replication levels report
    util::Mutex _replicationLevelMtx;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPTASK_H
