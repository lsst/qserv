// -*- LSST-C++ -*-
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
#include "xrdreq/QueryManagementAction.h"

// System headers
#include <stdexcept>

// Third party headers
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "xrdreq/QueryManagementRequest.h"

// LSST headers
#include "lsst/log/Log.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdreq.QueryManagementAction");

string xrootdStatus2str(XrdCl::XRootDStatus const& s) {
    return "status=" + to_string(s.status) + ", code=" + to_string(s.code) + ", errNo=" + to_string(s.errNo) +
           ", message='" + s.GetErrorMessage() + "'";
}

/// The RAII wrapper around the silly C pointer to facilitate proper deletion
/// of the object returned by the XROOTD API.
struct LocationInfoRAII {
    XrdCl::LocationInfo* locationInfo = nullptr;
    ~LocationInfoRAII() { delete locationInfo; }
};

}  // namespace

namespace lsst::qserv::xrdreq {

void QueryManagementAction::notifyAllWorkers(string const& xrootdFrontendUrl,
                                             proto::QueryManagement::Operation op, QueryId queryId,
                                             CallbackType onFinish) {
    auto const ptr = shared_ptr<QueryManagementAction>(new QueryManagementAction());
    ptr->_notifyAllWorkers(xrootdFrontendUrl, op, queryId, onFinish);
}

QueryManagementAction::QueryManagementAction() {
    LOGS(_log, LOG_LVL_DEBUG, "QueryManagementAction  ** CONSTRUCTED **");
}

QueryManagementAction::~QueryManagementAction() {
    LOGS(_log, LOG_LVL_DEBUG, "QueryManagementAction  ** DELETED **");
}

void QueryManagementAction::_notifyAllWorkers(std::string const& xrootdFrontendUrl,
                                              proto::QueryManagement::Operation op, QueryId queryId,
                                              CallbackType onFinish) {
    string const context = "QueryManagementAction::" + string(__func__) + " ";

    // Find all subscribers (worker XROOTD servers) serving this special resource.
    // Throw an exception if no workers are registered.
    ::LocationInfoRAII locationInfoHandler;
    string const queryResourceName = "/query";
    XrdCl::FileSystem fileSystem(xrootdFrontendUrl);
    XrdCl::XRootDStatus const status = fileSystem.Locate(queryResourceName, XrdCl::OpenFlags::Flags::None,
                                                         locationInfoHandler.locationInfo);
    if (!status.IsOK()) {
        throw runtime_error(context + "failed to locate subscribers for resource " + queryResourceName +
                            ", " + ::xrootdStatus2str(status));
    }
    if (uint32_t const numLocations = locationInfoHandler.locationInfo->GetSize(); numLocations == 0) {
        throw runtime_error(context + "no subscribers are serving resource " + queryResourceName);
    } else {
        // Fill worker addresses as keys into the response object.
        for (uint32_t i = 0; i < numLocations; ++i) {
            _response[locationInfoHandler.locationInfo->At(i).GetAddress()] = string();
        }
    }

    // Send a request to each worker. Note capturing a copy of 'self' to ensure
    // the curent object will still existr while the requests will be being processed.
    auto const self = shared_from_this();
    for (auto itr : _response) {
        string const workerAddress = itr.first;

        // Connect to the worker service
        XrdSsiErrInfo errInfo;
        XrdSsiService* serviceProvider = XrdSsiProviderClient->GetService(errInfo, workerAddress);
        if (nullptr == serviceProvider) {
            throw runtime_error(context + " failed to contact worker service " + workerAddress +
                                ", error: " + errInfo.Get());
        }

        // Make and configure the request object
        auto request = xrdreq::QueryManagementRequest::create(
                op, queryId,
                [self, workerAddress, onFinish](xrdreq::QueryManagementRequest::Status status,
                                                string const& error) {
                    if (status != xrdreq::QueryManagementRequest::Status::SUCCESS) {
                        self->_response[workerAddress] = error;
                    }
                    if (++(self->_numWorkerRequestsFinished) == self->_response.size()) {
                        if (onFinish != nullptr) onFinish(self->_response);
                    }
                });

        // Initiate request processing
        XrdSsiResource resource(queryResourceName);
        serviceProvider->ProcessRequest(*request, resource);
    }
}

}  // namespace lsst::qserv::xrdreq
