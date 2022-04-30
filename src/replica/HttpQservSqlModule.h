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
#ifndef LSST_QSERV_HTTPQSERVSQLMODULE_H
#define LSST_QSERV_HTTPQSERVSQLMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/HttpModule.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpQservSqlModule implements a handler for executing SQL queries
 * via database services of the Qserv workers.
 */
class HttpQservSqlModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpQservSqlModule> Ptr;

    /**
     * The only supported value for parameter 'subModuleName' is the empty
     * string for executing a query via database services of the Qserv
     * workers.
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        HttpAuthType const authType = HttpAuthType::NONE);

    HttpQservSqlModule() = delete;
    HttpQservSqlModule(HttpQservSqlModule const&) = delete;
    HttpQservSqlModule& operator=(HttpQservSqlModule const&) = delete;

    ~HttpQservSqlModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpQservSqlModule(Controller::Ptr const& controller, std::string const& taskName,
                       HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp);

    /**
     * Process a request for executing a query against a worker database.
     * A result set of the query will be returned for those query types which
     * have the one upon a successful completion of a request.
     */
    nlohmann::json _execute();
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPQSERVSQLMODULE_H
