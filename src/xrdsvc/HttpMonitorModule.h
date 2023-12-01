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
#ifndef LSST_QSERV_XRDSVC_HTTPMONITORMODULE_H
#define LSST_QSERV_XRDSVC_HTTPMONITORMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "xrdsvc/HttpModule.h"

namespace lsst::qserv::qhttp {
class Request;
class Response;
}  // namespace lsst::qserv::qhttp

namespace lsst::qserv::wcontrol {
class Foreman;
}  // namespace lsst::qserv::wcontrol

// This header declarations
namespace lsst::qserv::xrdsvc {

/**
 * Class HttpMonitorModule implements a handler for reporting various run-time
 * monitoring metrics and statistics collected at the Qserv worker.
 */
class HttpMonitorModule : public xrdsvc::HttpModule {
public:
    /**
     * @note supported values for parameter 'subModuleName' are:
     *   'CONFIG' - get configuration parameters
     *   'MYSQL'  - get the status (running queries) of the worker's MySQL service
     *   'STATUS' - get the status info (tasks, schedulers, etc.)
     *   'FILES'  - get info on the partial result files
     *   'ECHO'   - send back the received data
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(std::string const& context, std::shared_ptr<wcontrol::Foreman> const& foreman,
                        std::shared_ptr<qhttp::Request> const& req,
                        std::shared_ptr<qhttp::Response> const& resp, std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpMonitorModule() = delete;
    HttpMonitorModule(HttpMonitorModule const&) = delete;
    HttpMonitorModule& operator=(HttpMonitorModule const&) = delete;

    ~HttpMonitorModule() final = default;

protected:
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpMonitorModule(std::string const& context, std::shared_ptr<wcontrol::Foreman> const& foreman,
                      std::shared_ptr<qhttp::Request> const& req,
                      std::shared_ptr<qhttp::Response> const& resp);

    /// @return Configuration parameters.
    nlohmann::json _config();

    /// @return The status (running queries) of the worker's MySQL service.
    nlohmann::json _mysql();

    /// @return The worker status info (tasks, schedulers, etc.).
    nlohmann::json _status();

    /// @return An info on the partial result files.
    nlohmann::json _files();

    /// @return Send back the received data.
    nlohmann::json _echo();
};

}  // namespace lsst::qserv::xrdsvc

#endif  // LSST_QSERV_XRDSVC_HTTPMONITORMODULE_H
