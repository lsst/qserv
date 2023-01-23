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
#include "replica/Mutex.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpCatalogsModule implements a handler for the database
 * catalog requests.
 */
class HttpCatalogsModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpCatalogsModule> Ptr;

    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        HttpAuthType const authType = HttpAuthType::NONE);

    HttpCatalogsModule() = delete;
    HttpCatalogsModule(HttpCatalogsModule const&) = delete;
    HttpCatalogsModule& operator=(HttpCatalogsModule const&) = delete;

    ~HttpCatalogsModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpCatalogsModule(Controller::Ptr const& controller, std::string const& taskName,
                       HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp);

    /**
     * Retrieve the latest state of the database stats from a persistent store.
     * @param database The name of a database.
     * @return The data statistics for the specified database.
     */
    nlohmann::json _databaseStats(std::string const& database) const;

    // The cached state is shared by all instances of the class

    /// The cached state of the last catalog stats report
    static nlohmann::json _catalogsReport;

    /// The time of the last cached report
    static uint64_t _catalogsReportTimeMs;

    /// Protects the catalog stats cache
    static replica::Mutex _catalogsMtx;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPCATALOGSMODULE_H
