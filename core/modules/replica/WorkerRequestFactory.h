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
#ifndef LSST_QSERV_REPLICA_WORKER_REQUEST_FACTORY_H
#define LSST_QSERV_REPLICA_WORKER_REQUEST_FACTORY_H

/// WorkerRequestFactory.h declares:
///
/// class WorkerRequestFactoryBase
/// class WorkerRequestFactory
/// (see individual class documentation for more information)

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "replica/ServiceProvider.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class WorkerReplicationRequest;
class WorkerDeleteRequest;
class WorkerFindRequest;
class WorkerFindAllRequest;

/**
  * Class WorkerRequestFactoryBase is an abstract base class for a family of
  * various implementations of factories for creating request objects.
  */
class WorkerRequestFactoryBase {

public:

    // Pointers to specific request types

    typedef std::shared_ptr<WorkerReplicationRequest> WorkerReplicationRequest_pointer;
    typedef std::shared_ptr<WorkerDeleteRequest>      WorkerDeleteRequest_pointer;
    typedef std::shared_ptr<WorkerFindRequest>        WorkerFindRequest_pointer;
    typedef std::shared_ptr<WorkerFindAllRequest>     WorkerFindAllRequest_pointer;

    // The default constructor and copy semantics are prohibited

    WorkerRequestFactoryBase() = delete;
    WorkerRequestFactoryBase(WorkerRequestFactoryBase const&) = delete;
    WorkerRequestFactoryBase& operator=(WorkerRequestFactoryBase const&) = delete;

    /// Destructor
    virtual ~WorkerRequestFactoryBase() = default;

    /// Return the name of a technology the factory is based upon
    virtual std::string technology() const = 0;

    /**
     * Create an instance of the replication request
     *
     * @see class WorkerReplicationRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerReplicationRequest_pointer createReplicationRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database,
            unsigned int       chunk,
            std::string const& sourceWorker) = 0;

   /**
     * Create an instance of the replica deletion request
     *
     * @see class WorkerDeleteRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerDeleteRequest_pointer createDeleteRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database,
            unsigned int       chunk) = 0;

   /**
     * Create an instance of the replica lookup request
     *
     * @see class WorkerFindRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerFindRequest_pointer createFindRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database,
            unsigned int       chunk,
            bool               computeCheckSum) = 0;

   /**
     * Create an instance of the replicas lookup request
     *
     * @see class WorkerFindAllRequest
     *
     * @return a pointer to the newely created object
     */
    virtual WorkerFindAllRequest_pointer createFindAllRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database) = 0;
            
protected:

    /**
     * The constructor of the class.
     *
     * @param serviceProvider - a provider of various services
     */
    explicit WorkerRequestFactoryBase(ServiceProvider::pointer const& serviceProvider);

protected:

    // Parameters of the object

    ServiceProvider::pointer _serviceProvider;
};

/**
  * Class WorkerRequestFactory is a proxy class which is constructed with
  * a choice of a specific implementation of the factory.
  */
class WorkerRequestFactory
    :   public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactory() = delete;
    WorkerRequestFactory(WorkerRequestFactory const&) = delete;
    WorkerRequestFactory& operator=(WorkerRequestFactory const&) = delete;

    /**
     * The constructor of the class.
     *
     * The technology name must be valid. Otherwise std::invalid_argument will
     * be thrown. If the default value of the parameter is assumed then the one
     * from the currnet configuration will be assumed.
     *
     * This is the list of technologies which are presently supported:
     *
     *   'TEST'   - request objects wghich are ment to be used for testing the framework
     *              operation w/o making any persistent side effects.
     *
     *   'POSIX'  - request objects based on the direct manipulation of files
     *              on a POSIX file system.
     *
     *   'FS'     - request objects based on the direct manipulation of local files
     *              on a POSIX file system and for reading remote files using
     *              the built-into-worker simple file server.
     *
     * @param serviceProvider - a provider of various serviceses (including configurations)
     * @param technology      - the name of the technology
     */
    explicit WorkerRequestFactory(ServiceProvider::pointer const& serviceProvider,
                                  std::string const& technology=std::string());

    /// Destructor
    ~WorkerRequestFactory() override {
        delete _ptr;
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::technology
     */
    std::string technology() const override {
        return _ptr->technology();
    }

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createReplicationRequest
     */
    WorkerReplicationRequest_pointer createReplicationRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database,
            unsigned int       chunk,
            std::string const& sourceWorker) override {

        return _ptr->createReplicationRequest(
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

   /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createDeleteRequest
     */
    WorkerDeleteRequest_pointer createDeleteRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database,
            unsigned int       chunk) override {

        return _ptr->createDeleteRequest(
            worker,
            id,
            priority,
            database,
            chunk);
    }

   /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindRequest
     */
    WorkerFindRequest_pointer createFindRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database,
            unsigned int       chunk,
            bool               computeCheckSum) override {
        
        return _ptr->createFindRequest(
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum);
    }

   /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::createFindAllRequest
     */
    WorkerFindAllRequest_pointer createFindAllRequest(
            std::string const& worker,
            std::string const& id,
            int                priority,
            std::string const& database) override {
        
        return _ptr->createFindAllRequest(
            worker,
            id,
            priority,
            database);
    }

protected:

    /// A pointer to the final implementation of the factory
    WorkerRequestFactoryBase* _ptr;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_WORKER_REQUEST_FACTORY_H
