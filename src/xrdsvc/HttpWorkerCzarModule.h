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
#ifndef LSST_QSERV_XRDSVC_HTTPWORKERCZARMODULE_H
#define LSST_QSERV_XRDSVC_HTTPWORKERCZARMODULE_H

// System headers
#include <functional>
#include <memory>
#include <set>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "qmeta/types.h"
#include "xrdsvc/HttpModule.h"

namespace lsst::qserv::qhttp {
class Request;
class Response;
}  // namespace lsst::qserv::qhttp

namespace lsst::qserv::wcontrol {
class Foreman;
}  // namespace lsst::qserv::wcontrol

namespace lsst::qserv::xrdsvc {
class SsiProviderServer;
}  // namespace lsst::qserv::xrdsvc

// This header declarations
namespace lsst::qserv::xrdsvc {

/// &&& doc
class HttpWorkerCzarModule : public xrdsvc::HttpModule {
public:
    /// @note supported values for parameter 'subModuleName' are:
    ///  'QUERYJOB'     - Convert an UberJob message into Tasks and a send channel.
    /// @throws std::invalid_argument for unknown values of parameter 'subModuleName'
    static void process(std::string const& context, std::shared_ptr<wcontrol::Foreman> const& foreman,
                        std::shared_ptr<qhttp::Request> const& req,
                        std::shared_ptr<qhttp::Response> const& resp, std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpWorkerCzarModule() = delete;
    HttpWorkerCzarModule(HttpWorkerCzarModule const&) = delete;
    HttpWorkerCzarModule& operator=(HttpWorkerCzarModule const&) = delete;

    ~HttpWorkerCzarModule() final = default;

protected:
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpWorkerCzarModule(std::string const& context, std::shared_ptr<wcontrol::Foreman> const& foreman,
                         std::shared_ptr<qhttp::Request> const& req,
                         std::shared_ptr<qhttp::Response> const& resp);

    /// &&& doc
    nlohmann::json _queryJob();

    /// &&& doc
    nlohmann::json _handleQueryJob(std::string const& func);

    /// &&&uj temporary function for testing communication. Something like this will
    ///       need to be called when the uberjob has finished making the result file.
    void _temporaryRespFunc(std::string const& targetWorkerId, std::string const& czarName,
                            qmeta::CzarId czarId, std::string const& czarHostName, int czarPort,
                            uint64_t ujQueryId, uint64_t ujId);
};

}  // namespace lsst::qserv::xrdsvc

#endif  // LSST_QSERV_XRDSVC_HTTPWORKERCZARMODULE_H
