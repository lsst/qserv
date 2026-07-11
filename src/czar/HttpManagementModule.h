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
#ifndef LSST_QSERV_CZAR_HTTPMANAGEMENTMODULE_H
#define LSST_QSERV_CZAR_HTTPMANAGEMENTMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "czar/QhttpModule.h"

// Forward declarations

namespace lsst::qserv::cconfig {
class EventService;
}  // namespace lsst::qserv::cconfig

namespace lsst::qserv::qhttp {
class Request;
class Response;
}  // namespace lsst::qserv::qhttp

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpManagementModule implements a handler for managing various run-time
 * events and actions initiated by the Replication/Ingest subsystem.
 */
class HttpManagementModule : public QhttpModule {
public:
    /**
     * Supported values for parameter 'subModuleName' are:
     *   'EVENT' - post an event
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(std::string const& context, std::shared_ptr<qhttp::Request> const& req,
                        std::shared_ptr<qhttp::Response> const& resp,
                        std::shared_ptr<cconfig::EventService> const& eventService,
                        std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpManagementModule() = delete;
    HttpManagementModule(HttpManagementModule const&) = delete;
    HttpManagementModule& operator=(HttpManagementModule const&) = delete;

    ~HttpManagementModule() final = default;

protected:
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpManagementModule(std::string const& context, std::shared_ptr<qhttp::Request> const& req,
                         std::shared_ptr<qhttp::Response> const& resp,
                         std::shared_ptr<cconfig::EventService> const& eventService);

    /// @return Configuration parameters.
    nlohmann::json _event();

    /// The event service handling events posted by the Replication system.
    std::shared_ptr<cconfig::EventService> const _eventService;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPMANAGEMENTMODULE_H
