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
#ifndef LSST_QSERV_REPLICA_WORKERREQUESTFACTORY_H
#define LSST_QSERV_REPLICA_WORKERREQUESTFACTORY_H

// System headers
#include <cstdint>
#include <memory>
#include <string>

// Qserv headers
#include "replica/ServiceProvider.h"
#include "replica/WorkerRequest.h"

// Forward declarations
namespace lsst { namespace qserv { namespace replica {
class ProtocolRequestIndex;
class ProtocolRequestSql;
class WorkerDeleteRequest;
class WorkerEchoRequest;
class WorkerFindAllRequest;
class WorkerFindRequest;
class WorkerIndexRequest;
class WorkerSqlRequest;
class WorkerReplicationRequest;
namespace database { namespace mysql {
class ConnectionPool;
}}   // namespace database::mysql
}}}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class WorkerRequestFactoryBase is an abstract base class for a family of
 * various implementations of factories for creating request objects.
 */
class WorkerRequestFactoryBase {
public:
    // Pointers to specific request types

    typedef std::shared_ptr<database::mysql::ConnectionPool> ConnectionPoolPtr;

    typedef std::shared_ptr<WorkerDeleteRequest> WorkerDeleteRequestPtr;
    typedef std::shared_ptr<WorkerEchoRequest> WorkerEchoRequestPtr;
    typedef std::shared_ptr<WorkerFindRequest> WorkerFindRequestPtr;
    typedef std::shared_ptr<WorkerFindAllRequest> WorkerFindAllRequestPtr;
    typedef std::shared_ptr<WorkerReplicationRequest> WorkerReplicationRequestPtr;
    typedef std::shared_ptr<WorkerSqlRequest> WorkerSqlRequestPtr;
    typedef std::shared_ptr<WorkerIndexRequest> WorkerIndexRequestPtr;

    WorkerRequestFactoryBase() = delete;
    WorkerRequestFactoryBase(WorkerRequestFactoryBase const&) = delete;
    WorkerRequestFactoryBase& operator=(WorkerRequestFactoryBase const&) = delete;

    virtual ~WorkerRequestFactoryBase() = default;

    /// @return the name of a technology the factory is based upon
    virtual std::string technology() const = 0;

    /// @see class WorkerReplicationRequest
    virtual WorkerReplicationRequestPtr createReplicationRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestReplicate const& request) const = 0;

    /// @see class WorkerDeleteRequest
    virtual WorkerDeleteRequestPtr createDeleteRequest(std::string const& worker, std::string const& id,
                                                       int priority,
                                                       WorkerRequest::ExpirationCallbackType const& onExpired,
                                                       unsigned int requestExpirationIvalSec,
                                                       ProtocolRequestDelete const& request) const = 0;

    /// @see class WorkerFindRequest
    virtual WorkerFindRequestPtr createFindRequest(std::string const& worker, std::string const& id,
                                                   int priority,
                                                   WorkerRequest::ExpirationCallbackType const& onExpired,
                                                   unsigned int requestExpirationIvalSec,
                                                   ProtocolRequestFind const& request) const = 0;

    /// @see class WorkerFindAllRequest
    virtual WorkerFindAllRequestPtr createFindAllRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestFindAll const& request) const = 0;

    /// @see class WorkerEchoRequest
    virtual WorkerEchoRequestPtr createEchoRequest(std::string const& worker, std::string const& id,
                                                   int priority,
                                                   WorkerRequest::ExpirationCallbackType const& onExpired,
                                                   unsigned int requestExpirationIvalSec,
                                                   ProtocolRequestEcho const& request) const = 0;

    /// @see class WorkerSqlRequest
    virtual WorkerSqlRequestPtr createSqlRequest(std::string const& worker, std::string const& id,
                                                 int priority,
                                                 WorkerRequest::ExpirationCallbackType const& onExpired,
                                                 unsigned int requestExpirationIvalSec,
                                                 ProtocolRequestSql const& request) const = 0;

    /// @see class WorkerIndexRequest
    virtual WorkerIndexRequestPtr createIndexRequest(std::string const& worker, std::string const& id,
                                                     int priority,
                                                     WorkerRequest::ExpirationCallbackType const& onExpired,
                                                     unsigned int requestExpirationIvalSec,
                                                     ProtocolRequestIndex const& request) const = 0;

protected:
    /**
     * @param serviceProvider a provider of various services
     * @param connectionPool a pool of persistent database connections
     */
    WorkerRequestFactoryBase(ServiceProvider::Ptr const& serviceProvider,
                             ConnectionPoolPtr const& connectionPool);

protected:
    ServiceProvider::Ptr const _serviceProvider;
    ConnectionPoolPtr const _connectionPool;
};

/**
 * Class WorkerRequestFactory is a proxy class which is constructed with
 * a choice of a specific implementation of the factory.
 */
class WorkerRequestFactory : public WorkerRequestFactoryBase {
public:
    WorkerRequestFactory() = delete;
    WorkerRequestFactory(WorkerRequestFactory const&) = delete;
    WorkerRequestFactory& operator=(WorkerRequestFactory const&) = delete;

    /**
     * The constructor of the class.
     *
     * The technology name must be valid. Otherwise std::invalid_argument will
     * be thrown. If the default value of the parameter is assumed then the one
     * from the current configuration will be assumed.
     *
     * This is the list of technologies which are presently supported:
     *
     *   'TEST'   - request objects which are meant to be used for testing the framework
     *              operation w/o making any persistent side effects.
     *
     *   'POSIX'  - request objects based on the direct manipulation of files
     *              on a POSIX file system.
     *
     *   'FS'     - request objects based on the direct manipulation of local files
     *              on a POSIX file system and for reading remote files using
     *              the built-into-worker simple file server.
     *
     * @param serviceProvider provider of various services (including configurations)
     * @param connectionPool a pool of persistent database connections
     * @param technology (optional) the name of a technology
     */
    WorkerRequestFactory(ServiceProvider::Ptr const& serviceProvider, ConnectionPoolPtr const& connectionPool,
                         std::string const& technology = std::string());

    ~WorkerRequestFactory() final { delete _ptr; }

    std::string technology() const final { return _ptr->technology(); }

    WorkerReplicationRequestPtr createReplicationRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestReplicate const& request) const final;

    WorkerDeleteRequestPtr createDeleteRequest(std::string const& worker, std::string const& id, int priority,
                                               WorkerRequest::ExpirationCallbackType const& onExpired,
                                               unsigned int requestExpirationIvalSec,
                                               ProtocolRequestDelete const& request) const final;

    WorkerFindRequestPtr createFindRequest(std::string const& worker, std::string const& id, int priority,
                                           WorkerRequest::ExpirationCallbackType const& onExpired,
                                           unsigned int requestExpirationIvalSec,
                                           ProtocolRequestFind const& request) const final;

    WorkerFindAllRequestPtr createFindAllRequest(std::string const& worker, std::string const& id,
                                                 int priority,
                                                 WorkerRequest::ExpirationCallbackType const& onExpired,
                                                 unsigned int requestExpirationIvalSec,
                                                 ProtocolRequestFindAll const& request) const final;

    WorkerEchoRequestPtr createEchoRequest(std::string const& worker, std::string const& id, int priority,
                                           WorkerRequest::ExpirationCallbackType const& onExpired,
                                           unsigned int requestExpirationIvalSec,
                                           ProtocolRequestEcho const& request) const final;

    WorkerSqlRequestPtr createSqlRequest(std::string const& worker, std::string const& id, int priority,
                                         WorkerRequest::ExpirationCallbackType const& onExpired,
                                         unsigned int requestExpirationIvalSec,
                                         ProtocolRequestSql const& request) const final;

    WorkerIndexRequestPtr createIndexRequest(std::string const& worker, std::string const& id, int priority,
                                             WorkerRequest::ExpirationCallbackType const& onExpired,
                                             unsigned int requestExpirationIvalSec,
                                             ProtocolRequestIndex const& request) const final;

protected:
    /// Pointer to the final implementation of the factory
    WorkerRequestFactoryBase const* _ptr;
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERREQUESTFACTORY_H
