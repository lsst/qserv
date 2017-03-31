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

#ifndef LSST_QSERV_QHTTP_AJAXENDPOINT_H
#define LSST_QSERV_QHTTP_AJAXENDPOINT_H

// System headers
#include <memory>
#include <mutex>
#include <string>
#include <vector>

// Local headers
#include "qhttp/Response.h"

namespace lsst {
namespace qserv {
namespace qhttp {

class Server;

class AjaxEndpoint
{
public:

    using Ptr = std::shared_ptr<AjaxEndpoint>;

    static Ptr add(Server& server, std::string const& path);
    void update(std::string const& json); // thread-safe

private:

    AjaxEndpoint();

    std::vector<Response::Ptr> _pendingResponses;
    std::mutex _pendingResponsesMutex;

};

}}} // namespace lsst::qserv::qhttp

#endif // LSST_QSERV_QHTTP_AJAXENDPOINT_H
