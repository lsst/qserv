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
#ifndef LSST_QSERV_REPLICA_WORKER_FIND_ALL_REQUEST_H
#define LSST_QSERV_REPLICA_WORKER_FIND_ALL_REQUEST_H

/// WorkerFindAllRequest.h declares:
///
/// class WorkerFindAllRequest
/// class WorkerFindAllRequestPOSIX
/// class WorkerFindAllRequestX
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

/**
  * Class WorkerFindAllRequest represents a context and a state of replicas lookup
  * requsts within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerFindAllRequest
    :   public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindAllRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                          std::string const& worker,
                          std::string const& id,
                          int                priority,
                          std::string const& database);

    // Default construction and copy semantics are prohibited

    WorkerFindAllRequest() = delete;
    WorkerFindAllRequest(WorkerFindAllRequest const&) = delete;
    WorkerFindAllRequest& operator=(WorkerFindAllRequest const&) = delete;

    ~WorkerFindAllRequest() override = default;

    // Trivial accessors

    std::string const& database() const { return _database; }

   /**
     * Return a refernce to a result of the completed request.
     *
     * Note that this operation returns a meanigful result only when a request
     * is completed with status STATUS_SUCCEEDED.
     */
    ReplicaInfoCollection const& replicaInfoCollection() const;

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
    WorkerFindAllRequest(ServiceProvider::Ptr const& serviceProvider,
                         std::string const& worker,
                         std::string const& id,
                         int                priority,
                         std::string const& database);
protected:

    // Parameters of the request

    std::string _database;

    /// Result of the operation
    ReplicaInfoCollection _replicaInfoCollection;
};

/**
  * Class WorkerFindAllRequestPOSIX provides an actual implementation for
  * the replicas lookup based on the direct manipulation of files on
  * a POSIX file system.
  */
class WorkerFindAllRequestPOSIX
    :   public WorkerFindAllRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindAllRequestPOSIX> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database);

    // Default construction and copy semantics are prohibited

    WorkerFindAllRequestPOSIX() = delete;
    WorkerFindAllRequestPOSIX(WorkerFindAllRequestPOSIX const&) = delete;
    WorkerFindAllRequestPOSIX& operator=(WorkerFindAllRequestPOSIX const&) = delete;

    ~WorkerFindAllRequestPOSIX() override = default;

    /**
     * This method implements the virtual method of the base class
     *
     * @see WorkerRequest::execute
     */
    bool execute() override;

private:

    /**
     * The normal constructor of the class.
     */
    WorkerFindAllRequestPOSIX(ServiceProvider::Ptr const& serviceProvider,
                              std::string const& worker,
                              std::string const& id,
                              int priority,
                              std::string const& database);
};

/**
  * Class WorkerFindAllRequestFS provides an actual implementation for
  * the replica deletion based on the direct manipulation of files on
  * a POSIX file system.
  *
  * Note, this is just a typedef to class WorkerDeleteRequestPOSIX.
  */
typedef WorkerFindAllRequestPOSIX WorkerFindAllRequestFS;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_FIND_ALL_REQUEST_H
