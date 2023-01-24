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
#include "replica/WorkerDirectorIndexRequest.h"
#include "replica/WorkerReplicationRequest.h"
#include "replica/WorkerSqlRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerRequestFactory");

}  // namespace

namespace lsst::qserv::replica {

///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryBase ////////////////////
///////////////////////////////////////////////////////////////////

WorkerRequestFactoryBase::WorkerRequestFactoryBase(ServiceProvider::Ptr const& serviceProvider,
                                                   database::mysql::ConnectionPool::Ptr const& connectionPool)
        : _serviceProvider(serviceProvider), _connectionPool(connectionPool) {}

///////////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactoryTest ////////////////////
///////////////////////////////////////////////////////////////////

/**
 * Class WorkerRequestFactory is a factory class constructing the test versions
 * of the request objects which make no persistent side effects.
 */
class WorkerRequestFactoryTest : public WorkerRequestFactoryBase {
public:
    WorkerRequestFactoryTest() = delete;
    WorkerRequestFactoryTest(WorkerRequestFactoryTest const&) = delete;
    WorkerRequestFactoryTest& operator=(WorkerRequestFactoryTest const&) = delete;

    WorkerRequestFactoryTest(ServiceProvider::Ptr const& serviceProvider,
                             database::mysql::ConnectionPool::Ptr const& connectionPool)
            : WorkerRequestFactoryBase(serviceProvider, connectionPool) {}

    ~WorkerRequestFactoryTest() final = default;

    string technology() const final { return "TEST"; }

    WorkerReplicationRequest::Ptr createReplicationRequest(
            string const& worker, string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestReplicate const& request) const final {
        return WorkerReplicationRequest::create(_serviceProvider, worker, id, priority, onExpired,
                                                requestExpirationIvalSec, request);
    }

    WorkerDeleteRequest::Ptr createDeleteRequest(string const& worker, string const& id, int priority,
                                                 WorkerRequest::ExpirationCallbackType const& onExpired,
                                                 unsigned int requestExpirationIvalSec,
                                                 ProtocolRequestDelete const& request) const final {
        return WorkerDeleteRequest::create(_serviceProvider, worker, id, priority, onExpired,
                                           requestExpirationIvalSec, request);
    }

    WorkerFindRequest::Ptr createFindRequest(string const& worker, string const& id, int priority,
                                             WorkerRequest::ExpirationCallbackType const& onExpired,
                                             unsigned int requestExpirationIvalSec,
                                             ProtocolRequestFind const& request) const final {
        return WorkerFindRequest::create(_serviceProvider, worker, id, priority, onExpired,
                                         requestExpirationIvalSec, request);
    }

    WorkerFindAllRequest::Ptr createFindAllRequest(string const& worker, string const& id, int priority,
                                                   WorkerRequest::ExpirationCallbackType const& onExpired,
                                                   unsigned int requestExpirationIvalSec,
                                                   ProtocolRequestFindAll const& request) const final {
        return WorkerFindAllRequest::create(_serviceProvider, worker, id, priority, onExpired,
                                            requestExpirationIvalSec, request);
    }

    WorkerEchoRequest::Ptr createEchoRequest(string const& worker, string const& id, int priority,
                                             WorkerRequest::ExpirationCallbackType const& onExpired,
                                             unsigned int requestExpirationIvalSec,
                                             ProtocolRequestEcho const& request) const final {
        return WorkerEchoRequest::create(_serviceProvider, worker, id, priority, onExpired,
                                         requestExpirationIvalSec, request);
    }

    WorkerSqlRequest::Ptr createSqlRequest(string const& worker, string const& id, int priority,
                                           WorkerRequest::ExpirationCallbackType const& onExpired,
                                           unsigned int requestExpirationIvalSec,
                                           ProtocolRequestSql const& request) const final {
        return WorkerSqlRequest::create(_serviceProvider, worker, id, priority, onExpired,
                                        requestExpirationIvalSec, request);
    }

    WorkerDirectorIndexRequest::Ptr createDirectorIndexRequest(
            string const& worker, string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestDirectorIndex const& request) const final {
        return WorkerDirectorIndexRequest::create(_serviceProvider, _connectionPool, worker, id, priority,
                                                  onExpired, requestExpirationIvalSec, request);
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
    WorkerRequestFactoryPOSIX() = delete;
    WorkerRequestFactoryPOSIX(WorkerRequestFactoryPOSIX const&) = delete;
    WorkerRequestFactoryPOSIX& operator=(WorkerRequestFactoryPOSIX const&) = delete;

    WorkerRequestFactoryPOSIX(ServiceProvider::Ptr const& serviceProvider,
                              database::mysql::ConnectionPool::Ptr const& connectionPool)
            : WorkerRequestFactoryBase(serviceProvider, connectionPool) {}

    ~WorkerRequestFactoryPOSIX() final = default;

    string technology() const final { return "POSIX"; }

    WorkerReplicationRequest::Ptr createReplicationRequest(
            string const& worker, string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestReplicate const& request) const final {
        return WorkerReplicationRequestPOSIX::create(_serviceProvider, worker, id, priority, onExpired,
                                                     requestExpirationIvalSec, request);
    }

    WorkerDeleteRequest::Ptr createDeleteRequest(string const& worker, string const& id, int priority,
                                                 WorkerRequest::ExpirationCallbackType const& onExpired,
                                                 unsigned int requestExpirationIvalSec,
                                                 ProtocolRequestDelete const& request) const final {
        return WorkerDeleteRequestPOSIX::create(_serviceProvider, worker, id, priority, onExpired,
                                                requestExpirationIvalSec, request);
    }

    WorkerFindRequest::Ptr createFindRequest(string const& worker, string const& id, int priority,
                                             WorkerRequest::ExpirationCallbackType const& onExpired,
                                             unsigned int requestExpirationIvalSec,
                                             ProtocolRequestFind const& request) const final {
        return WorkerFindRequestPOSIX::create(_serviceProvider, worker, id, priority, onExpired,
                                              requestExpirationIvalSec, request);
    }

    WorkerFindAllRequest::Ptr createFindAllRequest(string const& worker, string const& id, int priority,
                                                   WorkerRequest::ExpirationCallbackType const& onExpired,
                                                   unsigned int requestExpirationIvalSec,
                                                   ProtocolRequestFindAll const& request) const final {
        return WorkerFindAllRequestPOSIX::create(_serviceProvider, worker, id, priority, onExpired,
                                                 requestExpirationIvalSec, request);
    }

    WorkerEchoRequest::Ptr createEchoRequest(string const& worker, string const& id, int priority,
                                             WorkerRequest::ExpirationCallbackType const& onExpired,
                                             unsigned int requestExpirationIvalSec,
                                             ProtocolRequestEcho const& request) const final {
        return WorkerEchoRequestPOSIX::create(_serviceProvider, worker, id, priority, onExpired,
                                              requestExpirationIvalSec, request);
    }

    WorkerSqlRequest::Ptr createSqlRequest(string const& worker, string const& id, int priority,
                                           WorkerRequest::ExpirationCallbackType const& onExpired,
                                           unsigned int requestExpirationIvalSec,
                                           ProtocolRequestSql const& request) const final {
        return WorkerSqlRequestPOSIX::create(_serviceProvider, worker, id, priority, onExpired,
                                             requestExpirationIvalSec, request);
    }

    WorkerDirectorIndexRequest::Ptr createDirectorIndexRequest(
            string const& worker, string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestDirectorIndex const& request) const final {
        return WorkerDirectorIndexRequestPOSIX::create(_serviceProvider, _connectionPool, worker, id,
                                                       priority, onExpired, requestExpirationIvalSec,
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
    WorkerRequestFactoryFS() = delete;
    WorkerRequestFactoryFS(WorkerRequestFactoryFS const&) = delete;
    WorkerRequestFactoryFS& operator=(WorkerRequestFactoryFS const&) = delete;

    WorkerRequestFactoryFS(ServiceProvider::Ptr const& serviceProvider,
                           database::mysql::ConnectionPool::Ptr const& connectionPool)
            : WorkerRequestFactoryBase(serviceProvider, connectionPool) {}

    ~WorkerRequestFactoryFS() final = default;

    string technology() const final { return "FS"; }

    WorkerReplicationRequest::Ptr createReplicationRequest(
            string const& worker, string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestReplicate const& request) const final {
        return WorkerReplicationRequestFS::create(_serviceProvider, worker, id, priority, onExpired,
                                                  requestExpirationIvalSec, request);
    }

    WorkerDeleteRequest::Ptr createDeleteRequest(string const& worker, string const& id, int priority,
                                                 WorkerRequest::ExpirationCallbackType const& onExpired,
                                                 unsigned int requestExpirationIvalSec,
                                                 ProtocolRequestDelete const& request) const final {
        return WorkerDeleteRequestFS::create(_serviceProvider, worker, id, priority, onExpired,
                                             requestExpirationIvalSec, request);
    }

    WorkerFindRequest::Ptr createFindRequest(string const& worker, string const& id, int priority,
                                             WorkerRequest::ExpirationCallbackType const& onExpired,
                                             unsigned int requestExpirationIvalSec,
                                             ProtocolRequestFind const& request) const final {
        return WorkerFindRequestFS::create(_serviceProvider, worker, id, priority, onExpired,
                                           requestExpirationIvalSec, request);
    }

    WorkerFindAllRequest::Ptr createFindAllRequest(string const& worker, string const& id, int priority,
                                                   WorkerRequest::ExpirationCallbackType const& onExpired,
                                                   unsigned int requestExpirationIvalSec,
                                                   ProtocolRequestFindAll const& request) const final {
        return WorkerFindAllRequestFS::create(_serviceProvider, worker, id, priority, onExpired,
                                              requestExpirationIvalSec, request);
    }

    WorkerEchoRequest::Ptr createEchoRequest(string const& worker, string const& id, int priority,
                                             WorkerRequest::ExpirationCallbackType const& onExpired,
                                             unsigned int requestExpirationIvalSec,
                                             ProtocolRequestEcho const& request) const final {
        return WorkerEchoRequestFS::create(_serviceProvider, worker, id, priority, onExpired,
                                           requestExpirationIvalSec, request);
    }

    WorkerSqlRequest::Ptr createSqlRequest(string const& worker, string const& id, int priority,
                                           WorkerRequest::ExpirationCallbackType const& onExpired,
                                           unsigned int requestExpirationIvalSec,
                                           ProtocolRequestSql const& request) const final {
        return WorkerSqlRequestFS::create(_serviceProvider, worker, id, priority, onExpired,
                                          requestExpirationIvalSec, request);
    }

    WorkerDirectorIndexRequest::Ptr createDirectorIndexRequest(
            string const& worker, string const& id, int priority,
            WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
            ProtocolRequestDirectorIndex const& request) const final {
        return WorkerDirectorIndexRequestFS::create(_serviceProvider, _connectionPool, worker, id, priority,
                                                    onExpired, requestExpirationIvalSec, request);
    }
};

///////////////////////////////////////////////////////////////
///////////////////// WorkerRequestFactory ////////////////////
///////////////////////////////////////////////////////////////

WorkerRequestFactory::WorkerRequestFactory(ServiceProvider::Ptr const& serviceProvider,
                                           database::mysql::ConnectionPool::Ptr const& connectionPool,
                                           string const& technology)
        : WorkerRequestFactoryBase(serviceProvider, connectionPool) {
    string const finalTechnology =
            technology.empty() ? serviceProvider->config()->get<string>("worker", "technology") : technology;

    if (finalTechnology == "TEST")
        _ptr = new WorkerRequestFactoryTest(serviceProvider, connectionPool);
    else if (finalTechnology == "POSIX")
        _ptr = new WorkerRequestFactoryPOSIX(serviceProvider, connectionPool);
    else if (finalTechnology == "FS")
        _ptr = new WorkerRequestFactoryFS(serviceProvider, connectionPool);
    else {
        throw invalid_argument("WorkerRequestFactory::" + string(__func__) + " unknown technology: '" +
                               finalTechnology);
    }
}

WorkerReplicationRequest::Ptr WorkerRequestFactory::createReplicationRequest(
        string const& worker, string const& id, int priority,
        WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
        ProtocolRequestReplicate const& request) const {
    auto ptr = _ptr->createReplicationRequest(worker, id, priority, onExpired, requestExpirationIvalSec,
                                              request);
    ptr->init();
    return ptr;
}

WorkerDeleteRequest::Ptr WorkerRequestFactory::createDeleteRequest(
        string const& worker, string const& id, int priority,
        WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
        ProtocolRequestDelete const& request) const {
    auto ptr = _ptr->createDeleteRequest(worker, id, priority, onExpired, requestExpirationIvalSec, request);
    ptr->init();
    return ptr;
}

WorkerFindRequest::Ptr WorkerRequestFactory::createFindRequest(
        string const& worker, string const& id, int priority,
        WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
        ProtocolRequestFind const& request) const {
    auto ptr = _ptr->createFindRequest(worker, id, priority, onExpired, requestExpirationIvalSec, request);
    ptr->init();
    return ptr;
}

WorkerFindAllRequest::Ptr WorkerRequestFactory::createFindAllRequest(
        string const& worker, string const& id, int priority,
        WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
        ProtocolRequestFindAll const& request) const {
    auto ptr = _ptr->createFindAllRequest(worker, id, priority, onExpired, requestExpirationIvalSec, request);
    ptr->init();
    return ptr;
}

WorkerEchoRequest::Ptr WorkerRequestFactory::createEchoRequest(
        string const& worker, string const& id, int priority,
        WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
        ProtocolRequestEcho const& request) const {
    auto ptr = _ptr->createEchoRequest(worker, id, priority, onExpired, requestExpirationIvalSec, request);
    ptr->init();
    return ptr;
}

WorkerSqlRequest::Ptr WorkerRequestFactory::createSqlRequest(
        string const& worker, string const& id, int priority,
        WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
        ProtocolRequestSql const& request) const {
    auto ptr = _ptr->createSqlRequest(worker, id, priority, onExpired, requestExpirationIvalSec, request);
    ptr->init();
    return ptr;
}

WorkerDirectorIndexRequest::Ptr WorkerRequestFactory::createDirectorIndexRequest(
        string const& worker, string const& id, int priority,
        WorkerRequest::ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
        ProtocolRequestDirectorIndex const& request) const {
    auto ptr = _ptr->createDirectorIndexRequest(worker, id, priority, onExpired, requestExpirationIvalSec,
                                                request);
    ptr->init();
    return ptr;
}

}  // namespace lsst::qserv::replica
