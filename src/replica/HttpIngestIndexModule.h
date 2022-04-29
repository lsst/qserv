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
#ifndef LSST_QSERV_HTTPINGESTINDEXMODULE_H
#define LSST_QSERV_HTTPINGESTINDEXMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/HttpModule.h"

// This header declarations
namespace lsst { namespace qserv { namespace replica {

/**
 * Class HttpIngestIndexModule manages the "secondary" indexes in Qserv.
 */
class HttpIngestIndexModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpIngestIndexModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   BUILD-SECONDARY-INDEX  for building (or rebuilding) the "secondary" index
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        HttpAuthType const authType = HttpAuthType::NONE);

    HttpIngestIndexModule() = delete;
    HttpIngestIndexModule(HttpIngestIndexModule const&) = delete;
    HttpIngestIndexModule& operator=(HttpIngestIndexModule const&) = delete;

    ~HttpIngestIndexModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpIngestIndexModule(Controller::Ptr const& controller, std::string const& taskName,
                          HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    nlohmann::json _buildSecondaryIndex();
};

}}}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPINGESTINDEXMODULE_H
