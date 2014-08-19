// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file
  *
  * @brief QueryResource. An XrdSsiService::Resource
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qdisp/QueryResource.h"

// System headers
#include <cassert>
#include <iostream>
#include <sstream>

#include "log/Logger.h"
#include "qdisp/ExecStatus.h"
#include "qdisp/QueryRequest.h"
#include "qdisp/QueryResource.h"
#include "qdisp/QueryReceiver.h"

namespace lsst {
namespace qserv {
namespace qdisp {

/// May not throw exceptions because the calling code comes from
/// xrootd land and will not catch any exceptions.
void QueryResource::ProvisionDone(XrdSsiSession* s) { // Step 3
        LOGGER_INF << "Provision done\n";
        if(!s) {
            // Check eInfo in resource for error details
            int code;
            char const* msg = eInfo.Get(code);
            LOGGER_ERR << "Error provisioning, msg=" << msg << " code="
                       << code << "\n";
            _status.report(ExecStatus::PROVISION_NACK, code, std::string(msg));
            // FIXME code may be wrong.
            _receiver->errorFlush(std::string(msg), code);
            return;
        }
        _session = s;
        _request = new QueryRequest(s, _payload, _receiver,
                                    _retryFunc, _status);
        assert(_request);
        // Hand off the request and release ownership.
        _status.report(ExecStatus::REQUEST);
        bool requestSent = _session->ProcessRequest(_request);

        if(requestSent) {
            _request = 0; // _session now has ownership
        } else {
            int code;
            char const* msg = eInfo.Get(code);
            _status.report(ExecStatus::REQUEST_ERROR, code, msg);
            LOGGER_ERR << "Failed to send request " << *_request << std::endl;
            delete _request;
            _request = 0;
            // Retry the request.
            // TODO: should be more selective about retrying a query.
            if(_retryFunc) {
                (*_retryFunc)();
            }
        }

        // If we are not doing anything else with the session,
        // we can stop it after our requests are complete.
        delete this; // Delete ourselves, nobody needs this resource anymore.
    }

}}} // lsst::qserv::qdisp
