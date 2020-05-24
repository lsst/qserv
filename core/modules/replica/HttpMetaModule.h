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
#ifndef LSST_QSERV_HTTPMETAMODULE_H
#define LSST_QSERV_HTTPMETAMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/HttpModule.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpMetaModule implements a handler for the metadata queries
 * on the REST API itself.
 */
class HttpMetaModule: public HttpModule {
public:
    typedef std::shared_ptr<HttpMetaModule> Ptr;

    /// The current version of the REST API
    static unsigned int const version;

    /**
     * @note supported values for parameter 'subModuleName' are:
     *   'VERSION' - return a version of the REST API
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller,
                        std::string const& taskName,
                        HttpProcessorConfig const& processorConfig,
                        qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName=std::string(),
                        HttpModule::AuthType const authType=HttpModule::AUTH_NONE);

    HttpMetaModule() = delete;
    HttpMetaModule(HttpMetaModule const&) = delete;
    HttpMetaModule& operator=(HttpMetaModule const&) = delete;

    ~HttpMetaModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpMetaModule(Controller::Ptr const& controller,
                   std::string const& taskName,
                   HttpProcessorConfig const& processorConfig,
                   qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    nlohmann::json _version();
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPMETAMODULE_H
