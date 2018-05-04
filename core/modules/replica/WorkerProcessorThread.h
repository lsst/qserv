// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_WORKER_PROCESSOR_THREAD_H
#define LSST_QSERV_REPLICA_WORKER_PROCESSOR_THREAD_H

/// WorkerProcessorThread.h declares:
///
/// class WorkerProcessorThread
/// (see individual class documentation for more information)

// System headers
#include <atomic>
#include <memory>
#include <thread>

// Qserv headers

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/// Forward declaration for the class
class WorkerProcessor;
class WorkerRequest;

/**
  * Class WorkerProcessorThread is a thread-based request processing engine
  * for replication requests.
  */
class WorkerProcessorThread
    : public std::enable_shared_from_this<WorkerProcessorThread> {

public:

    /// Smart reference to objects of the class
    typedef std::shared_ptr<WorkerProcessorThread> Ptr;

    /// Forwad declaration for the smart pointer to requests
    typedef std::shared_ptr<WorkerRequest> WorkerRequest_pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * 
     * @param processor - a reference to the repository of requests to be processed
     */
    static Ptr create(WorkerProcessor &processor);

    // Default construction and copy semantics are prohibited

    WorkerProcessorThread() = delete;
    WorkerProcessorThread(WorkerProcessorThread const&) = delete;
    WorkerProcessorThread& operator=(WorkerProcessorThread const&) = delete;

    /// Destructor (can't say 'override' because the base class's one is not virtual)
    virtual ~WorkerProcessorThread() = default;

    /// Return an ientifier of this thread object
    unsigned int id() const { return _id; }

    /// Return true if the processing thread is still running
    bool isRunning() const;

    /**
     * Create and run the thread (if none is still running) fetching
     * and processing requests until method stop() is called.
     */
    void run();

    /**
     * Tell the running thread to abort proccessing the current
     * request (if any), put that request back into the input queue,
     * stop fetching new requests and finish. The thread can be resumed
     * later by calling method run().
     *
     * NOTE: This is an asynchronous operation.
     */
    void stop();

    /// Return the context string
    std::string context() const { return "THREAD: " + std::to_string(_id) + "  "; }

private:

    /**
     * The constructor of the class.
     *
     * @param processor - a reference to the repository of requests to be processed
     * @param id        - a unique identifier of this object
     */
    WorkerProcessorThread(WorkerProcessor& processor,
                          unsigned int id);

    /**
     * Event handler called by the thread when it's about to stop
     */
    void stopped();

    /**
     * Event handler called by the thread when a request is cancelled
     */
    void cancelled(const WorkerRequest_pointer &request);
 
private:

    /// The processor
    WorkerProcessor &_processor;

    /// The identifier of this thread object   
    unsigned int _id;

    /// The processing thread is created on demand when calling method run()
    std::shared_ptr<std::thread> _thread;
    
    /// The flag to be raised to tell the running thread to stop.
    /// The thread will reset this flag when it finishes.
    std::atomic<bool> _stop;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_PROCESSOR_THREAD_H
