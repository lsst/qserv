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
#ifndef LSST_QSERV_REPLICA_WORKER_FIND_REQUEST_H
#define LSST_QSERV_REPLICA_WORKER_FIND_REQUEST_H

/// WorkerFindRequest.h declares:
///
/// class WorkerFindRequest
/// class WorkerFindRequestPOSIX
/// class WorkerFindRequestX
/// (see individual class documentation for more information)

// System headers
#include <string>

// Qserv headers
#include "replica/ReplicaInfo.h"
#include "replica/WorkerRequest.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class MultiFileCsComputeEngine;

/**
  * Class WorkerFindRequest represents a context and a state of replica lookup
  * requsts within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerFindRequest
    :   public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindRequest> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create(ServiceProvider::pointer const& serviceProvider,
                          std::string const& worker,
                          std::string const& id,
                          int                priority,
                          std::string const& database,
                          unsigned int       chunk,
                          bool               computeCheckSum);

    // Default construction and copy semantics are prohibited

    WorkerFindRequest() = delete;
    WorkerFindRequest(WorkerFindRequest const&) = delete;
    WorkerFindRequest& operator=(WorkerFindRequest const&) = delete;

    /// Destructor
    ~WorkerFindRequest() override = default;

    // Trivial accessors

    std::string const& database()        const { return _database; }
    unsigned int       chunk()           const { return _chunk; }
    bool               computeCheckSum() const { return _computeCheckSum; }

   /**
     * Return a refernce to a result of the completed request.
     *
     * Note that this operation returns a meanigful result only when a request
     * is completed with status STATUS_SUCCEEDED.
     */
    ReplicaInfo const& replicaInfo() const { return _replicaInfo; }

    /**
     * This method implements the virtual method of the base class
     *
     * @see WorkerRequest::execute
     */
    bool execute() override;

protected:

    /**
     * The normal constructor of the class.
     */
    WorkerFindRequest(ServiceProvider::pointer const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int                priority,
                      std::string const& database,
                      unsigned int       chunk,
                      bool               computeCheckSum);
protected:

    // Parameters of the request

    std::string  _database;
    unsigned int _chunk;
    bool         _computeCheckSum;

    /// Result of the operation
    ReplicaInfo _replicaInfo;
};

/**
  * Class WorkerFindRequestPOSIX provides an actual implementation for
  * the replica lookup requests based on the direct manipulation of files on
  * a POSIX file system.
  */
class WorkerFindRequestPOSIX
    :   public WorkerFindRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindRequestPOSIX> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create(ServiceProvider::pointer const& serviceProvider,
                          std::string const& worker,
                          std::string const& id,
                          int                priority,
                          std::string const& database,
                          unsigned int       chunk,
                          bool               computeCheckSum);

    // Default construction and copy semantics are prohibited

    WorkerFindRequestPOSIX() = delete;
    WorkerFindRequestPOSIX(WorkerFindRequestPOSIX const&) = delete;
    WorkerFindRequestPOSIX& operator=(WorkerFindRequestPOSIX const&) = delete;

    /// Destructor
    ~WorkerFindRequestPOSIX() override = default;

    /**
     * This method implements the virtual method of the base class
     *
     * @see WorkerFindRequest::execute
     */
    bool execute() override;

private:

    /**
     * The normal constructor of the class.
     */
    WorkerFindRequestPOSIX(ServiceProvider::pointer const& serviceProvider,
                           std::string const& worker,
                           std::string const& id,
                           int                priority,
                           std::string const& database,
                           unsigned int       chunk,
                           bool               computeCheckSum);

private:
    
    /// The engine for incremental control sum calculation
    std::unique_ptr<MultiFileCsComputeEngine> _csComputeEnginePtr;
};

/**
  * Class WorkerFindRequestFS provides an actual implementation for
  * the replica deletion based on the direct manipulation of files on
  * a POSIX file system.
  *
  * Note, this is just a typedef to class WorkerDeleteRequestPOSIX.
  */
typedef WorkerFindRequestPOSIX WorkerFindRequestFS;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_FIND_REQUEST_H