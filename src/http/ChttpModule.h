
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
#ifndef LSST_QSERV_HTTP_CHTTPMODULE_H
#define LSST_QSERV_HTTP_CHTTPMODULE_H

// System headers
#include <string>
#include <unordered_map>

// Qserv headers
#include "http/Module.h"

// Forward declarations

namespace httplib {
class Request;
class Response;
}  // namespace httplib

namespace lsst::qserv::http {
class RequestBodyJSON;
class RequestQuery;
}  // namespace lsst::qserv::http

// This header declarations
namespace lsst::qserv::http {

/**
 * Class ChttpModule is an extended base class specialized for constructing
 * the CPP-HTTPLIB request processing modules.
 */
class ChttpModule : public Module {
public:
    ChttpModule() = delete;
    ChttpModule(ChttpModule const&) = delete;
    ChttpModule& operator=(ChttpModule const&) = delete;

    virtual ~ChttpModule() = default;

protected:
    /**
     * @param authContext  An authorization context for operations which require extra security.
     * @param req  The HTTP request.
     * @param resp  The HTTP response channel.
     */
    ChttpModule(http::AuthContext const& authContext, httplib::Request const& req, httplib::Response& resp);

    httplib::Request const& req() { return _req; }
    httplib::Response& resp() { return _resp; }

    virtual std::string method() const;
    virtual std::unordered_map<std::string, std::string> params() const;
    virtual RequestQuery query() const;
    virtual std::string headerEntry(std::string const& key) const;
    virtual void getRequestBody(std::string& content, std::string const& requiredContentType);
    virtual void sendResponse(std::string const& content, std::string const& contentType);

private:
    httplib::Request const& _req;
    httplib::Response& _resp;
};

}  // namespace lsst::qserv::http

#endif  // LSST_QSERV_HTTP_CHTTPMODULE_H
