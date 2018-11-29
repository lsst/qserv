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
#ifndef LSST_QSERV_HTTPTHREAD_H
#define LSST_QSERV_HTTPTHREAD_H

// System headers
#include <functional>
#include <set>

// Qserv headers
#include "qhttp/Server.h"
#include "replica/ControlThread.h"
#include "replica/DeleteWorkerThread.h"
#include "replica/HealthMonitorThread.h"
#include "replica/ReplicationThread.h"
#include "util/Mutex.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpThread represents a thread which runs the built-in HTTP server
 * responding to the REST API for managing the Replication Controller
 * and responding to various information retrieval requests.
 */
class HttpThread
    :   public ControlThread {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<HttpThread> Ptr;

    // Default construction and copy semantics are prohibited

    HttpThread() = delete;
    HttpThread(HttpThread const&) = delete;
    HttpThread& operator=(HttpThread const&) = delete;

    ~HttpThread() final = default;

    /**
     * Create a new thread with specified parameters.
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
     *   of the thread. Set it to 'nullptr' if no call back should be made.
     *
     * @param onWorkerEvict
     *   callback function to be called when one or more workers
     *   were requested to be explicitly evicted from the cluster.
     *
     * @param healthMonitorThread
     *   a reference to the Cluster Health Monitoring thread is made visible
     *   in a context of this lass to allow suspending/resuming it as needed.
     *
     * @param replicationThread
     *   a reference to the Linear Replication Loop thread is made visible
     *   in a context of this lass to allow suspending/resuming it as needed.
     *
     * @param deleteWorkerThread
     *   a reference to the Worker Eviction thread is made visible
     *   in a context of this lass to allow suspending/resuming it as needed.
     *
     * @return
     *   the smart pointer to a new object
     */
    static Ptr create(Controller::Ptr const& controller,
                      ControlThread::CallbackType const& onTerminated,
                      HealthMonitorThread::WorkerEvictCallbackType const& onWorkerEvict,
                      HealthMonitorThread::Ptr const& healthMonitorThread,
                      ReplicationThread::Ptr const& replicationThread,
                      DeleteWorkerThread::Ptr const& deleteWorkerThread);

protected:

    /**
     * @see ControlThread::run()
     */
    void run() final;

private:

    /**
     * The constructor is available to the class's factory method
     *
     * @see HttpThread::create()
     */
    HttpThread(Controller::Ptr const& controller,
               CallbackType const& onTerminated,
               HealthMonitorThread::WorkerEvictCallbackType const& onWorkerEvict,
               HealthMonitorThread::Ptr const& healthMonitorThread,
               ReplicationThread::Ptr const& replicationThread,
               DeleteWorkerThread::Ptr const& deleteWorkerThread);


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
    HealthMonitorThread::WorkerEvictCallbackType const _onWorkerEvict;

    // References(!) to smart pointers to other threads which can be managed
    // by this class.
    //
    // @note
    //   References to the pointers are used to avoid increasing the reference
    //   counters to the objects.

    HealthMonitorThread::Ptr const& _healthMonitorThread;
    ReplicationThread::Ptr   const& _replicationThread;
    DeleteWorkerThread::Ptr  const& _deleteWorkerThread;

    /// The server for processing REST requests
    qhttp::Server::Ptr const _httpServer;

    /// The flag used for lazy initialization of the Web server the first time
    /// this thread runs.
    bool _isInitialized;

    /// The latest state of the replication levels report
    std::string _replicationLevelReport;
    
    /// The timestamp for when the last report was made
    uint64_t _replicationLevelReportTimeMs;

    /// Mutex guarding the cache with the replication levels report
    util::Mutex _replicationLevelMtx;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPTHREAD_H
