/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "replica/AddReplicaQservMgtRequest.h"

// System headers

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "lsst/log/Log.h"
#include "replica/ServiceProvider.h"
#include "replica/Configuration.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.AddReplicaQservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

AddReplicaQservMgtRequest::pointer AddReplicaQservMgtRequest::create(
                                        ServiceProvider& serviceProvider,
                                        boost::asio::io_service& io_service,
                                        std::string const& worker,
                                        unsigned int chunk,
                                        std::string const& databaseFamily,
                                        wpublish::ChunkGroupQservRequest::calback_type onFinish,
                                        bool keepTracking) {
    return AddReplicaQservMgtRequest::pointer(
        new AddReplicaQservMgtRequest(serviceProvider,
                                      io_service,
                                      worker,
                                      chunk,
                                      databaseFamily,
                                      onFinish,
                                      keepTracking));
}

AddReplicaQservMgtRequest::AddReplicaQservMgtRequest(
                                ServiceProvider& serviceProvider,
                                boost::asio::io_service& io_service,
                                std::string const& worker,
                                unsigned int chunk,
                                std::string const& databaseFamily,
                                wpublish::ChunkGroupQservRequest::calback_type onFinish,
                                bool keepTracking)
    :   QservMgtRequest(serviceProvider,
                        io_service,
                        "ADD_REPLICA",
                        worker,
                        keepTracking),
        _chunk(chunk),
        _databaseFamily(databaseFamily),
        _onFinish(onFinish),
        _qservRequest(nullptr) {
}

void AddReplicaQservMgtRequest::startImpl() {

    XrdSsiResource resource(ResourceUnit::makeWorkerPath(_worker));

    _qservRequest = wpublish::AddChunkGroupQservRequest::create(
                                    _chunk,
                                    _serviceProvider.config()->databases(_databaseFamily),
                                    _onFinish);

    _service->ProcessRequest(*_qservRequest, resource);
}

void AddReplicaQservMgtRequest::finishImpl() {
}

void AddReplicaQservMgtRequest::notify() {
}
    
}}} // namespace lsst::qserv::replica