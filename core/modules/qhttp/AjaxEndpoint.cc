/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class-header
#include "qhttp/AjaxEndpoint.h"

// Local headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "qhttp/Server.h"

namespace lsst {
namespace qserv {
namespace qhttp {


AjaxEndpoint::AjaxEndpoint()
{
}


AjaxEndpoint::Ptr AjaxEndpoint::add(Server& server, std::string const& path)
{
    auto aep = std::shared_ptr<AjaxEndpoint>(new AjaxEndpoint);
    server.addHandler("GET", path, [aep](Request::Ptr request, Response::Ptr response) {
        std::lock_guard<std::mutex> lock{aep->_pendingResponsesMutex};
        aep->_pendingResponses.push_back(response);
    });
    return aep;
}


void AjaxEndpoint::update(std::string const& json)
{
    std::lock_guard<std::mutex> lock(_pendingResponsesMutex);
    for(auto& pendingResponse : _pendingResponses) {
        pendingResponse->headers["Cache-Control"] = "no-cache";
        pendingResponse->send(json, "application/json");
    }
    _pendingResponses.clear();
}

}}} // namespace lsst::qserv::qhttp
