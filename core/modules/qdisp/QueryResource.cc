// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "qdisp/QueryResource.h"

// System headers
#include <cassert>
#include <iostream>
#include <sstream>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qdisp/ExecStatus.h"
#include "qdisp/QueryRequest.h"
#include "qdisp/QueryResource.h"
#include "qdisp/ResponseRequester.h"

namespace lsst {
namespace qserv {
namespace qdisp {

/// May not throw exceptions because the calling code comes from
/// xrootd land and will not catch any exceptions.
void QueryResource::ProvisionDone(XrdSsiSession* s) { // Step 3
    LOGF_INFO("Provision done");
    if(!s) {
        // Check eInfo in resource for error details
        int code = 0;
        std::string msg = eInfoGet(code);
        LOGF_ERROR("Error provisioning, msg=%1% code=%2%" % msg % code);
        _status.report(ExecStatus::PROVISION_NACK, code, msg);
        // FIXME code may be wrong.
        _requester->errorFlush(msg, code);
        delete this;
        return;
    }
    if(_requester->cancelled()) {
        delete this;
        return; // Don't bother doing anything if the requester doesn't care.
    }
    _session = s;

    // QueryRequest handles its own deletion by registering a cancellation
    // functor with the requester that deletes the request in its destructor.
    QueryRequest * request = new QueryRequest(s, _payload, _requester,
                                              _finishFunc, _retryFunc,
                                              _status);

    // Hand off the request.
    _status.report(ExecStatus::REQUEST);
    bool requestSent = _session->ProcessRequest(request);
    if(!requestSent) {
        int code = 0;
        std::string msg = eInfoGet(code);
        _status.report(ExecStatus::REQUEST_ERROR, code, msg);
        // It is unclear whether it is safe to refer to request here, say for
        // better log messages. It may have been deleted by another thread.
        LOGF_ERROR("Failed to send request");
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

const char* QueryResource::eInfoGet(int &code) {
    char const* message = eInfo.Get(code);
    return message ? message : "no message from XrdSsi, code may not be reliable";
}

}}} // lsst::qserv::qdisp
