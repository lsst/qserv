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
namespace lsst::qserv::replica {
class ProtocolRequestDirectorIndex;
class ProtocolRequestSql;
class WorkerDeleteRequest;
class WorkerEchoRequest;
class WorkerFindAllRequest;
class WorkerFindRequest;
class WorkerDirectorIndexRequest;
class WorkerSqlRequest;
class WorkerReplicationRequest;
namespace database::mysql {
class ConnectionPool;
}  // namespace database::mysql
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerRequestFactoryBase is an abstract base class for a family of
 * various implementations of factories for creating request objects.
 */
class WorkerRequestFactoryBase {
public:
    WorkerRequestFactoryBase() = delete;
    WorkerRequestFactoryBase(WorkerRequestFactoryBase const&) = delete;
    WorkerRequestFactoryBase& operator=(WorkerRequestFactoryBase const&) = delete;

    virtual ~WorkerRequestFactoryBase() = default;

    /// @return the name of a technology the factory is based upon
    virtual std::string technology() const = 0;

    /// @see class WorkerReplicationRequest
    virtual std::shared_ptr<WorkerReplicationRequest> createReplicationRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestReplicate const& request) const = 0;

    /// @see class WorkerDeleteRequest
    virtual std::shared_ptr<WorkerDeleteRequest> createDeleteRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestDelete const& request) const = 0;

    /// @see class WorkerFindRequest
    virtual std::shared_ptr<WorkerFindRequest> createFindRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestFind const& request) const = 0;

    /// @see class WorkerFindAllRequest
    virtual std::shared_ptr<WorkerFindAllRequest> createFindAllRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestFindAll const& request) const = 0;

    /// @see class WorkerEchoRequest
    virtual std::shared_ptr<WorkerEchoRequest> createEchoRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestEcho const& request) const = 0;

    /// @see class WorkerSqlRequest
    virtual std::shared_ptr<WorkerSqlRequest> createSqlRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestSql const& request) const = 0;

    /// @see class WorkerDirectorIndexRequest
    virtual std::shared_ptr<WorkerDirectorIndexRequest> createDirectorIndexRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestDirectorIndex const& request) const = 0;

protected:
    /**
     * @param serviceProvider a provider of various services
     * @param connectionPool a pool of persistent database connections
     */
    WorkerRequestFactoryBase(ServiceProvider::Ptr const& serviceProvider,
                             std::shared_ptr<database::mysql::ConnectionPool> const& connectionPool);

protected:
    ServiceProvider::Ptr const _serviceProvider;
    std::shared_ptr<database::mysql::ConnectionPool> const _connectionPool;
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
    WorkerRequestFactory(ServiceProvider::Ptr const& serviceProvider,
                         std::shared_ptr<database::mysql::ConnectionPool> const& connectionPool,
                         std::string const& technology = std::string());

    ~WorkerRequestFactory() final { delete _ptr; }

    std::string technology() const final { return _ptr->technology(); }

    std::shared_ptr<WorkerReplicationRequest> createReplicationRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestReplicate const& request) const final;

    std::shared_ptr<WorkerDeleteRequest> createDeleteRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestDelete const& request) const final;

    std::shared_ptr<WorkerFindRequest> createFindRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestFind const& request) const final;

    std::shared_ptr<WorkerFindAllRequest> createFindAllRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestFindAll const& request) const final;

    std::shared_ptr<WorkerEchoRequest> createEchoRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestEcho const& request) const final;

    std::shared_ptr<WorkerSqlRequest> createSqlRequest(std::string const& worker, std::string const& id,
                                                       int priority,
                                                       WorkerRequest::ExpirationCallbackType const& onExpired,
                                                       unsigned int requestExpirationIvalSec,
                                                       ProtocolRequestSql const& request) const final;

    std::shared_ptr<WorkerDirectorIndexRequest> createDirectorIndexRequest(
            std::string const& worker, std::string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestDirectorIndex const& request) const final;

protected:
    /// Pointer to the final implementation of the factory
    WorkerRequestFactoryBase const* _ptr;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERREQUESTFACTORY_H
