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
#ifndef LSST_QSERV_CZAR_HTTPCZARINGESTMODULE_H
#define LSST_QSERV_CZAR_HTTPCZARINGESTMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "czar/HttpCzarIngestModuleBase.h"
#include "http/ChttpModule.h"

// Forward declarations

namespace httplib {
class Request;
class Response;
}  // namespace httplib

// This header declarations
namespace lsst::qserv::czar {

/**
 * Class HttpCzarIngestModule implements a handler for processing requests for ingesting
 * user-generated data prodicts via the HTTP-based frontend.
 */
class HttpCzarIngestModule : public http::ChttpModule, public HttpCzarIngestModuleBase {
public:
    /**
     * @note supported values for parameter 'subModuleName' are:
     *   'INGEST-DATA'     - create a table and load it with data (sync)
     *   'DELETE-DATABASE' - delete an existing database (sync)
     *   'DELETE-TABLE'    - delete an existing table (sync)
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(boost::asio::io_service& io_service, std::string const& context,
                        httplib::Request const& req, httplib::Response& resp,
                        std::string const& subModuleName,
                        http::AuthType const authType = http::AuthType::NONE);

    HttpCzarIngestModule() = delete;
    HttpCzarIngestModule(HttpCzarIngestModule const&) = delete;
    HttpCzarIngestModule& operator=(HttpCzarIngestModule const&) = delete;

    virtual ~HttpCzarIngestModule() = default;

protected:
    virtual std::string context() const final;
    virtual nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpCzarIngestModule(boost::asio::io_service& io_service, std::string const& context,
                         httplib::Request const& req, httplib::Response& resp);

    nlohmann::json _ingestData();
    nlohmann::json _deleteDatabase();
    nlohmann::json _deleteTable();

    /// The context string for posting messages into the logging stream.
    std::string const _context;
};

}  // namespace lsst::qserv::czar

#endif  // LSST_QSERV_CZAR_HTTPCZARINGESTMODULE_H
