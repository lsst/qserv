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
#ifndef LSST_QSERV_REPLICA_WORKER_DELETE_REQUEST_H
#define LSST_QSERV_REPLICA_WORKER_DELETE_REQUEST_H

/// WorkerDeleteRequest.h declares:
///
/// class WorkerDeleteRequest
/// class WorkerDeleteRequestPOSIX
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
  * Class WorkerDeleteRequest represents a context and a state of replica deletion
  * requsts within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerDeleteRequest
    :   public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerDeleteRequest> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create(ServiceProvider&   serviceProvider,
                          std::string const& worker,
                          std::string const& id,
                          int                priority,
                          std::string const& database,
                          unsigned int       chunk);

    // Default construction and copy semantics are prohibited

    WorkerDeleteRequest() = delete;
    WorkerDeleteRequest(WorkerDeleteRequest const&) = delete;
    WorkerDeleteRequest& operator=(WorkerDeleteRequest const&) = delete;

    /// Destructor
    ~WorkerDeleteRequest() override = default;

    // Trivial accessors

    std::string const& database() const { return _database; }
    unsigned int       chunk()    const { return _chunk; }

    /// Return extended status of the request
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
    WorkerDeleteRequest(ServiceProvider&   serviceProvider,
                        std::string const& worker,
                        std::string const& id,
                        int                priority,
                        std::string const& database,
                        unsigned int       chunk);
protected:

    // Parameters of the object

    std::string  _database;
    unsigned int _chunk;

    /// Extended status of the replica deletion request
    ReplicaInfo _replicaInfo;
};

/**
  * Class WorkerDeleteRequestPOSIX provides an actual implementation for
  * the replica deletion based on the direct manipulation of files on
  * a POSIX file system.
  */
class WorkerDeleteRequestPOSIX
    :   public WorkerDeleteRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerDeleteRequestPOSIX> pointer;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     */
    static pointer create(ServiceProvider&   serviceProvider,
                          std::string const& worker,
                          std::string const& id,
                          int                priority,
                          std::string const& database,
                          unsigned int       chunk);

    // Default construction and copy semantics are prohibited

    WorkerDeleteRequestPOSIX() = delete;
    WorkerDeleteRequestPOSIX(WorkerDeleteRequestPOSIX const&) = delete;
    WorkerDeleteRequestPOSIX& operator=(WorkerDeleteRequestPOSIX const&) = delete;

    /// Destructor
    ~WorkerDeleteRequestPOSIX() override = default;

    /**
     * This method implements the virtual method of the base class
     *
     * @see WorkerDeleteRequest::execute
     */
    bool execute() override;

private:

    /**
     * The normal constructor of the class.
     */
    WorkerDeleteRequestPOSIX(ServiceProvider&  serviceProvider,
                             std::string const& worker,
                             std::string const& id,
                             int                priority,
                             std::string const& database,
                             unsigned int       chunk);
};

/**
  * Class WorkerDeleteRequestFS provides an actual implementation for
  * the replica deletion based on the direct manipulation of files on
  * a POSIX file system.
  *
  * Note, this is just a typedef to class WorkerDeleteRequestPOSIX.
  */
typedef WorkerDeleteRequestPOSIX WorkerDeleteRequestFS;


}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_DELETE_REQUEST_H