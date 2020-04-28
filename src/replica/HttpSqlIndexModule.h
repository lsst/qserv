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
#ifndef LSST_QSERV_HTTPSQLINDEXMODULE_H
#define LSST_QSERV_HTTPSQLINDEXMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/HttpModule.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpSqlIndexModule manages table indexes on the published catalogs.
 */
class HttpSqlIndexModule: public HttpModule {
public:
    typedef std::shared_ptr<HttpSqlIndexModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   ""                for obtaining a status of existing indexes
     *   "CREATE-INDEXES"  for creating an index on all instances of a table
     *   "DROP-INDEXES"    for dropping an existing index on all instances of a table
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

    HttpSqlIndexModule() = delete;
    HttpSqlIndexModule(HttpSqlIndexModule const&) = delete;
    HttpSqlIndexModule& operator=(HttpSqlIndexModule const&) = delete;

    ~HttpSqlIndexModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpSqlIndexModule(Controller::Ptr const& controller,
                       std::string const& taskName,
                       HttpProcessorConfig const& processorConfig,
                       qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp);

    /**
     * Return a status of the existing indexes on all instances of a table.
     */
    nlohmann::json _getIndexes();

    /**
     * Create a new index on all instances of a table.
     */
    nlohmann::json _createIndexes();

    /**
     * Dropping an existing index from all instances of a table.
     */
    nlohmann::json _dropIndexes();
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPSQLINDEXMODULE_H
