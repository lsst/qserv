/*
 * LSST Data Management System
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
#ifndef LSST_QSERV_REPLICA_WORKERFINDREQUEST_H
#define LSST_QSERV_REPLICA_WORKERFINDREQUEST_H

// System headers
#include <string>

// Qserv headers
#include "replica/ReplicaInfo.h"
#include "replica/WorkerRequest.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class MultiFileCsComputeEngine;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
  * Class WorkerFindRequest represents a context and a state of replica lookup
  * requests within the worker servers. It can also be used for testing the framework
  * operation as its implementation won't make any changes to any files or databases.
  *
  * Real implementations of the request processing must derive from this class.
  */
class WorkerFindRequest : public WorkerRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   provider is needed to access the Configuration of a setup
     *   and for validating the input parameters
     *
     * @param worker
     *   the name of a worker. The name must match the worker which
     *   is going to execute the request.
     *
     * @param id
     *   an identifier of a client request
     * 
     * @param priority
     *   indicates the importance of the request
     *
     * @param database
     *   the name of a database defines a scope of the replica
     *   lookup operation
     *
     * @param chunk
     *   the chunk whose replicas will be looked for
     *
     * @param computeCheckSum
     *   flag indicating if check/control sums should be
     *   computed on all files of the chunk
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database,
                      unsigned int chunk,
                      bool computeCheckSum);

    // Default construction and copy semantics are prohibited

    WorkerFindRequest() = delete;
    WorkerFindRequest(WorkerFindRequest const&) = delete;
    WorkerFindRequest& operator=(WorkerFindRequest const&) = delete;

    ~WorkerFindRequest() override = default;

    // Trivial get methods

    std::string const& database() const { return _database; }

    unsigned int chunk() const { return _chunk; }

    bool computeCheckSum() const { return _computeCheckSum; }

    /**
     * Extract request status into the Protobuf response object.
     *
     * @param response
     *   Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseFind& response) const;

    /// @see WorkerRequest::execute
    bool execute() override;

protected:

    /// @see WorkerFindRequest::create()
    WorkerFindRequest(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database,
                      unsigned int chunk,
                      bool computeCheckSum);


    // Input parameters

    std::string  const _database;
    unsigned int const _chunk;
    bool         const _computeCheckSum;

    /// Result of the operation
    ReplicaInfo _replicaInfo;
};

/**
  * Class WorkerFindRequestPOSIX provides an actual implementation for
  * the replica lookup requests based on the direct manipulation of files on
  * a POSIX file system.
  */
class WorkerFindRequestPOSIX : public WorkerFindRequest {

public:

    /// Pointer to self
    typedef std::shared_ptr<WorkerFindRequestPOSIX> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     * 
     * For a description of parameters:
     * 
     * @see WorkerFindRequestPOSIX::create()
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      std::string const& worker,
                      std::string const& id,
                      int priority,
                      std::string const& database,
                      unsigned int chunk,
                      bool computeCheckSum);

    // Default construction and copy semantics are prohibited

    WorkerFindRequestPOSIX() = delete;
    WorkerFindRequestPOSIX(WorkerFindRequestPOSIX const&) = delete;
    WorkerFindRequestPOSIX& operator=(WorkerFindRequestPOSIX const&) = delete;

    ~WorkerFindRequestPOSIX() final = default;

    /// @see WorkerFindRequest::execute
    bool execute() final;

private:

    /// @see WorkerFindRequestPOSIX::create()
    WorkerFindRequestPOSIX(ServiceProvider::Ptr const& serviceProvider,
                           std::string const& worker,
                           std::string const& id,
                           int priority,
                           std::string const& database,
                           unsigned int chunk,
                           bool computeCheckSum);

    
    /// The engine for incremental control sum calculation
    std::unique_ptr<MultiFileCsComputeEngine> _csComputeEnginePtr;
};

/**
 * Class WorkerFindRequestFS has the same implementation as the 'typedef'-ed
 * class for the replica deletion based on the direct manipulation of files on
 * a POSIX file system.
 */
typedef WorkerFindRequestPOSIX WorkerFindRequestFS;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKERFINDREQUEST_H
