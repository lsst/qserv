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
#ifndef LSST_QSERV_HTTPCATALOGSMODULE_H
#define LSST_QSERV_HTTPCATALOGSMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/HttpModule.h"
#include "util/Mutex.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpCatalogsModule implements a handler for the database
 * catalog requests.
 */
class HttpCatalogsModule: public HttpModule {
public:

    typedef std::shared_ptr<HttpCatalogsModule> Ptr;

    static Ptr create(Controller::Ptr const& controller,
                      std::string const& taskName,
                      unsigned int workerResponseTimeoutSec);

    HttpCatalogsModule() = delete;
    HttpCatalogsModule(HttpCatalogsModule const&) = delete;
    HttpCatalogsModule& operator=(HttpCatalogsModule const&) = delete;

    ~HttpCatalogsModule() final = default;

protected:

    void executeImpl(qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp,
                     std::string const& subModuleName) final;

private:

    HttpCatalogsModule(Controller::Ptr const& controller,
                       std::string const& taskName,
                       unsigned int workerResponseTimeoutSec);

    /**
     * Retrieve the latest state of the database stats from a persistent
     * store.
     *
     * @param database  the name of a database
     * @param dummyReport  if 'true' then return a report with all zeroes for known databases and tables
     * @return data statistics for the specified database
     */
    nlohmann::json _databaseStats(std::string const& database,
                                  bool dummyReport=false) const;

    /// The cached state of the last catalog stats report
    nlohmann::json _catalogsReport = nlohmann::json::object();

    /// The time of the last cached report
    uint64_t _catalogsReportTimeMs = 0;

    /// Protects the catalog stats cache
    util::Mutex _catalogsMtx;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPCATALOGSMODULE_H
