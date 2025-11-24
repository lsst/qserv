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
#ifndef LSST_QSERV_HTTPEXPORTMODULE_H
#define LSST_QSERV_HTTPEXPORTMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/contr/HttpModule.h"
#include "replica/util/Common.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpExportModule provides support exporting tables
 * from Qserv.
 */
class HttpExportModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpExportModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *  CONFIG-DATABASE  Return configuration for the specified database.
     *  CONFIG-TABLE     Return configuration for the specified table.
     *  TABLE-LOCATIONS  Return service locations for the specified table.

     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        http::AuthType const authType = http::AuthType::NONE);

    HttpExportModule() = delete;
    HttpExportModule(HttpExportModule const&) = delete;
    HttpExportModule& operator=(HttpExportModule const&) = delete;

    ~HttpExportModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpExportModule(Controller::Ptr const& controller, std::string const& taskName,
                     HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp);

    nlohmann::json _getDatabaseConfig();
    nlohmann::json _getTableConfig();
    nlohmann::json _getTableLocations();
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPEXPORTMODULE_H
