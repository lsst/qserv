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
#ifndef LSST_QSERV_HTTPREQUESTSMODULE_H
#define LSST_QSERV_HTTPREQUESTSMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/contr/HttpModule.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpRequestsModule implements a handler for pulling info on
 * the Replication system's Requests.
 */
class HttpRequestsModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpRequestsModule> Ptr;

    /**
     * @note supported values for parameter 'subModuleName' are
     * the empty string (for pulling info on all known Requests),
     * or 'SELECT-ONE-BY-ID' for a single request.
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        http::AuthType const authType = http::AuthType::NONE);

    HttpRequestsModule() = delete;
    HttpRequestsModule(HttpRequestsModule const&) = delete;
    HttpRequestsModule& operator=(HttpRequestsModule const&) = delete;

    ~HttpRequestsModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpRequestsModule(Controller::Ptr const& controller, std::string const& taskName,
                       HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp);

    nlohmann::json _requests();
    nlohmann::json _oneRequest();
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPREQUESTSMODULE_H
