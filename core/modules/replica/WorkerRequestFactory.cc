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

// Class header
#include "replica/WorkerRequestFactory.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/DatabaseMySQL.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "replica/WorkerDeleteRequest.h"
#include "replica/WorkerEchoRequest.h"
#include "replica/WorkerFindAllRequest.h"
#include "replica/WorkerFindRequest.h"
#include "replica/WorkerIndexRequest.h"
#include "replica/WorkerReplicationRequest.h"
#include "replica/WorkerSqlRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerRequestFactory");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryBase ////////////////////
///////////////////////////////////////////////////////////////////

WorkerRequestFactoryBase::WorkerRequestFactoryBase(
            ServiceProvider::Ptr const& serviceProvider,
            database::mysql::ConnectionPool::Ptr const& connectionPool)
    :   _serviceProvider(serviceProvider),
        _connectionPool(connectionPool) {
}


///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryTest ////////////////////
///////////////////////////////////////////////////////////////////

/**
  * Class WorkerRequestFactory is a factory class constructing the test versions
  * of the request objects which make no persistent side effects.
  */
class WorkerRequestFactoryTest : public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactoryTest() = delete;
    WorkerRequestFactoryTest(WorkerRequestFactoryTest const&) = delete;
    WorkerRequestFactoryTest& operator=(WorkerRequestFactoryTest const&) = delete;

    WorkerRequestFactoryTest(ServiceProvider::Ptr const& serviceProvider,
                             database::mysql::ConnectionPool::Ptr const& connectionPool)
        :   WorkerRequestFactoryBase(serviceProvider,
                                     connectionPool) {
    }

    ~WorkerRequestFactoryTest() final = default;

    /// @see WorkerRequestFactoryBase::technology
    string technology() const final { return "TEST"; }

    /// @see WorkerRequestFactoryBase::createReplicationRequest
    WorkerReplicationRequestPtr createReplicationRequest(string const& worker,
                                                         string const& id,
                                                         int priority,
                                                         string const& database,
                                                         unsigned int chunk,
                                                         string const& sourceWorker) const final {
        return WorkerReplicationRequest::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

    /// @see WorkerRequestFactoryBase::createDeleteRequest
    WorkerDeleteRequestPtr createDeleteRequest(string const& worker,
                                               string const& id,
                                               int priority,
                                               string const& database,
                                               unsigned int chunk) const final {
        return WorkerDeleteRequest::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /// @see WorkerRequestFactoryBase::createFindRequest
    WorkerFindRequestPtr createFindRequest(string const& worker,
                                           string const& id,
                                           int priority,
                                           string const& database,
                                           unsigned int chunk,
                                           bool computeCheckSum) const final {
        return WorkerFindRequest::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum);
    }

    /// @see WorkerRequestFactoryBase::createFindAllRequest
    WorkerFindAllRequestPtr createFindAllRequest(string const& worker,
                                                 string const& id,
                                                 int priority,
                                                 string const& database) const final {
        return WorkerFindAllRequest::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }

    /// @see WorkerRequestFactoryBase::createEchoRequest
    WorkerEchoRequestPtr createEchoRequest(string const& worker,
                                           string const& id,
                                           int priority,
                                           string const& data,
                                           uint64_t delay) const final {
        return WorkerEchoRequest::create(
            _serviceProvider,
            worker,
            id,
            priority,
            data,
            delay);
    }

    /// @see WorkerRequestFactoryBase::createSqlRequest
    WorkerSqlRequestPtr createSqlRequest(string const& worker,
                                         string const& id,
                                         ProtocolRequestSql const& request) const final {
        return WorkerSqlRequest::create(
            _serviceProvider,
            worker,
            id,
            request);
    }

    /// @see WorkerRequestFactoryBase::createIndexRequest
    WorkerIndexRequestPtr createIndexRequest(string const& worker,
                                             string const& id,
                                             ProtocolRequestIndex const& request) const final {
        return WorkerIndexRequest::create(
            _serviceProvider,
            _connectionPool,
            worker,
            id,
            request);
    }
};


////////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryPOSIX ////////////////////
////////////////////////////////////////////////////////////////////

/**
  * Class WorkerRequestFactoryPOSIX creates request objects based on the direct
  * manipulation of files on a POSIX file system.
  */
class WorkerRequestFactoryPOSIX : public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactoryPOSIX() = delete;
    WorkerRequestFactoryPOSIX(WorkerRequestFactoryPOSIX const&) = delete;
    WorkerRequestFactoryPOSIX& operator=(WorkerRequestFactoryPOSIX const&) = delete;

    WorkerRequestFactoryPOSIX(ServiceProvider::Ptr const& serviceProvider,
                              database::mysql::ConnectionPool::Ptr const& connectionPool)
        :   WorkerRequestFactoryBase(serviceProvider,
                                     connectionPool) {
    }

    ~WorkerRequestFactoryPOSIX() final = default;

    /// @see WorkerRequestFactoryBase::technology
    string technology() const final { return "POSIX"; }

    /// @see WorkerRequestFactoryBase::createReplicationRequest
    WorkerReplicationRequestPtr createReplicationRequest(string const& worker,
                                                         string const& id,
                                                         int priority,
                                                         string const& database,
                                                         unsigned int chunk,
                                                         string const& sourceWorker) const final {
        return WorkerReplicationRequestPOSIX::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

    /// @see WorkerRequestFactoryBase::createDeleteRequest
    WorkerDeleteRequestPtr createDeleteRequest(string const& worker,
                                               string const& id,
                                               int priority,
                                               string const& database,
                                               unsigned int chunk) const final {
        return WorkerDeleteRequestPOSIX::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /// @see WorkerRequestFactoryBase::createFindRequest
    WorkerFindRequestPtr createFindRequest(string const& worker,
                                           string const& id,
                                           int priority,
                                           string const& database,
                                           unsigned int chunk,
                                           bool computeCheckSum) const final {
        return WorkerFindRequestPOSIX::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum);
    }

    /// @see WorkerRequestFactoryBase::createFindAllRequest
    WorkerFindAllRequestPtr createFindAllRequest(string const& worker,
                                                 string const& id,
                                                 int priority,
                                                 string const& database) const final {
        return WorkerFindAllRequestPOSIX::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }

    /// @see WorkerRequestFactoryBase::createEchoRequest
    WorkerEchoRequestPtr createEchoRequest(string const& worker,
                                           string const& id,
                                           int priority,
                                           string const& data,
                                           uint64_t delay) const final {
        return WorkerEchoRequestPOSIX::create(
            _serviceProvider,
            worker,
            id,
            priority,
            data,
            delay);
    }

    /// @see WorkerRequestFactoryBase::createSqlRequest
    WorkerSqlRequestPtr createSqlRequest(string const& worker,
                                         string const& id,
                                         ProtocolRequestSql const& request) const final {
        return WorkerSqlRequestPOSIX::create(
            _serviceProvider,
            worker,
            id,
            request);
    }

    /// @see WorkerRequestFactoryBase::createIndexRequest
    WorkerIndexRequestPtr createIndexRequest(string const& worker,
                                             string const& id,
                                             ProtocolRequestIndex const& request) const final {
        return WorkerIndexRequestPOSIX::create(
            _serviceProvider,
            _connectionPool,
            worker,
            id,
            request);
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
class WorkerRequestFactoryFS : public WorkerRequestFactoryBase {

public:

    // Default construction and copy semantics are prohibited

    WorkerRequestFactoryFS() = delete;
    WorkerRequestFactoryFS(WorkerRequestFactoryFS const&) = delete;
    WorkerRequestFactoryFS& operator=(WorkerRequestFactoryFS const&) = delete;

    WorkerRequestFactoryFS(ServiceProvider::Ptr const& serviceProvider,
                           database::mysql::ConnectionPool::Ptr const& connectionPool)
        :   WorkerRequestFactoryBase(serviceProvider,
                                     connectionPool) {
    }

    ~WorkerRequestFactoryFS() final = default;

    /// @see WorkerRequestFactoryBase::technology
    string technology() const final { return "FS"; }

    /// @see WorkerRequestFactoryBase::createReplicationRequest
    WorkerReplicationRequestPtr createReplicationRequest(string const& worker,
                                                         string const& id,
                                                         int priority,
                                                         string const& database,
                                                         unsigned int chunk,
                                                         string const& sourceWorker) const final {
        return WorkerReplicationRequestFS::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            sourceWorker);
    }

    /// @see WorkerRequestFactoryBase::createDeleteRequest
    WorkerDeleteRequestPtr createDeleteRequest(string const& worker,
                                               string const& id,
                                               int priority,
                                               string const& database,
                                               unsigned int chunk) const final {
        return WorkerDeleteRequestFS::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk);
    }

    /// @see WorkerRequestFactoryBase::createFindRequest
    WorkerFindRequestPtr createFindRequest(string const& worker,
                                           string const& id,
                                           int priority,
                                           string const& database,
                                           unsigned int chunk,
                                           bool computeCheckSum) const final {
        return WorkerFindRequestFS::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum);
    }

    /// @see WorkerRequestFactoryBase::createFindAllRequest
    WorkerFindAllRequestPtr createFindAllRequest(string const& worker,
                                                 string const& id,
                                                 int priority,
                                                 string const& database) const final {
        return WorkerFindAllRequestFS::create(
            _serviceProvider,
            worker,
            id,
            priority,
            database);
    }

    /// @see WorkerRequestFactoryBase::createEchoRequest
    WorkerEchoRequestPtr createEchoRequest(string const& worker,
                                           string const& id,
                                           int priority,
                                           string const& data,
                                           uint64_t delay) const final {
        return WorkerEchoRequestFS::create(
            _serviceProvider,
            worker,
            id,
            priority,
            data,
            delay);
    }

    /// @see WorkerRequestFactoryBase::createSqlRequest
    WorkerSqlRequestPtr createSqlRequest(string const& worker,
                                         string const& id,
                                         ProtocolRequestSql const& request) const final {
        return WorkerSqlRequestFS::create(
            _serviceProvider,
            worker,
            id,
            request);
    }

    /// @see WorkerRequestFactoryBase::createIndexRequest
    WorkerIndexRequestPtr createIndexRequest(string const& worker,
                                             string const& id,
                                             ProtocolRequestIndex const& request) const final {
        return WorkerIndexRequestFS::create(
            _serviceProvider,
            _connectionPool,
            worker,
            id,
            request);
    }
};


///////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactory ////////////////////
///////////////////////////////////////////////////////////////

WorkerRequestFactory::WorkerRequestFactory(ServiceProvider::Ptr const& serviceProvider,
                                           database::mysql::ConnectionPool::Ptr const& connectionPool,
                                           string const& technology)
    :   WorkerRequestFactoryBase(serviceProvider,
                                 connectionPool) {

    string const finalTechnology =
        technology.empty() ? serviceProvider->config()->workerTechnology() : technology;

    if      (finalTechnology == "TEST")  _ptr = new WorkerRequestFactoryTest( serviceProvider, connectionPool);
    else if (finalTechnology == "POSIX") _ptr = new WorkerRequestFactoryPOSIX(serviceProvider, connectionPool);
    else if (finalTechnology == "FS")    _ptr = new WorkerRequestFactoryFS(   serviceProvider, connectionPool);
    else {
        throw invalid_argument(
                "WorkerRequestFactory::" + string(__func__) +
                " unknown technology: '" + finalTechnology);
    }
}

}}} // namespace lsst::qserv::replica
