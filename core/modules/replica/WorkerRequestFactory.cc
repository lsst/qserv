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

// Class header
#include "replica/WorkerRequestFactory.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerDeleteRequest.h"
#include "replica/WorkerFindAllRequest.h"
#include "replica/WorkerFindRequest.h"
#include "replica/WorkerReplicationRequest.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerRequestFactory");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryBase ////////////////////
///////////////////////////////////////////////////////////////////

WorkerRequestFactoryBase::WorkerRequestFactoryBase(ServiceProvider &serviceProvider)
    :   _serviceProvider(serviceProvider) {
}

///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryTest ////////////////////
///////////////////////////////////////////////////////////////////

/**
  * Class WorkerRequestFactory is a factory class constructing the test versions
  * of the request objects which make no persistent side effects.
  */
class WorkerRequestFactoryTest
    :   public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactoryTest() = delete;
    WorkerRequestFactoryTest(WorkerRequestFactoryTest const&) = delete;
    WorkerRequestFactoryTest& operator=(WorkerRequestFactoryTest const&) = delete;

    /// Normal constructor
    WorkerRequestFactoryTest(ServiceProvider& serviceProvider)
        :   WorkerRequestFactoryBase(serviceProvider) {
    }
    
    /// Destructor
    ~WorkerRequestFactoryTest() override = default;

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::technology
     */
    std::string technology() const { return "TEST"; }

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
        return WorkerReplicationRequest::create(
            _serviceProvider,
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
        return WorkerDeleteRequest::create(
            _serviceProvider,
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
        return WorkerFindRequest::create(
            _serviceProvider,
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
        return WorkerFindAllRequest::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }
};

////////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryPOSIX ////////////////////
////////////////////////////////////////////////////////////////////

/**
  * Class WorkerRequestFactoryPOSIX creates request objects based on the direct
  * manipulation of files on a POSIX file system.
  */
class WorkerRequestFactoryPOSIX
    :   public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactoryPOSIX() = delete;
    WorkerRequestFactoryPOSIX(WorkerRequestFactoryPOSIX const&) = delete;
    WorkerRequestFactoryPOSIX& operator=(WorkerRequestFactoryPOSIX const&) = delete;

    /// Normal constructor
    WorkerRequestFactoryPOSIX(ServiceProvider& serviceProvider)
        :   WorkerRequestFactoryBase(serviceProvider) {
    }
    
    /// Destructor
    ~WorkerRequestFactoryPOSIX() override = default;

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::technology
     */
    std::string technology() const { return "POSIX"; }

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
        return WorkerReplicationRequestPOSIX::create(
            _serviceProvider,
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
        return WorkerDeleteRequestPOSIX::create(
            _serviceProvider,
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
        return WorkerFindRequestPOSIX::create(
            _serviceProvider,
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
        return WorkerFindAllRequestPOSIX::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }
};

/////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryFS ////////////////////
/////////////////////////////////////////////////////////////////

/**
  * Class WorkerRequestFactoryFS creates request objects based on the direct
  * manipulation of local files on a POSIX file system and for reading remote
  * files using the built-into-worker simple file server.
  */
class WorkerRequestFactoryFS
    :   public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactoryFS() = delete;
    WorkerRequestFactoryFS(WorkerRequestFactoryFS const&) = delete;
    WorkerRequestFactoryFS& operator=(WorkerRequestFactoryFS const&) = delete;

    /// Normal constructor
    WorkerRequestFactoryFS(ServiceProvider& serviceProvider)
        :   WorkerRequestFactoryBase(serviceProvider) {
    }
    
    /// Destructor
    ~WorkerRequestFactoryFS() override = default;

    /**
     * Implements the corresponding method of the base class
     *
     * @see WorkerReplicationRequestBase::technology
     */
    std::string technology() const { return "FS"; }

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
        return WorkerReplicationRequestFS::create(
            _serviceProvider,
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
        return WorkerDeleteRequestFS::create(
            _serviceProvider,
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
        return WorkerFindRequestFS::create(
            _serviceProvider,
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
        return WorkerFindAllRequestFS::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }
};

///////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactory ////////////////////
///////////////////////////////////////////////////////////////

WorkerRequestFactory::WorkerRequestFactory(ServiceProvider&   serviceProvider,
                                           std::string const& technology)
    :   WorkerRequestFactoryBase(serviceProvider) {
        
    std::string const finalTechnology =
        technology.empty() ? serviceProvider.config()->workerTechnology() : technology;

    if      (finalTechnology == "TEST")  { _ptr = new WorkerRequestFactoryTest( serviceProvider); }
    else if (finalTechnology == "POSIX") { _ptr = new WorkerRequestFactoryPOSIX(serviceProvider); }
    else if (finalTechnology == "FS")    { _ptr = new WorkerRequestFactoryFS(   serviceProvider); }
    else {
        throw std::invalid_argument(
                        "WorkerRequestFactory::WorkerRequestFactory() unknown technology: '" +
                        finalTechnology);
    }
}

}}} // namespace lsst::qserv::replica