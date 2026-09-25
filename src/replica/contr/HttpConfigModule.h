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
#ifndef LSST_QSERV_HTTPCONFIGMODULE_H
#define LSST_QSERV_HTTPCONFIGMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/contr/HttpModule.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpConfigModule implements a handler for reporting
 * various info on or modifying Config of the Replication system.
 */
class HttpConfigModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpConfigModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   the empty string        for reporting the current state of the Config
     *   UPDATE-GENERAL          for updating one or many general configuration parameters
     *   UPDATE-WORKER           for updating configuration parameters of a worker
     *   DELETE-WORKER           for removing a known worker from the Config
     *   ADD-WORKER              for adding a new worker to the Config
     *   DELETE-DATABASE-FAMILY  for removing a known database family from the Config
     *   ADD-DATABASE-FAMILY     for adding a new database family to the Config
     *   DELETE-DATABASE         for removing a known database from the Config
     *   ADD-DATABASE            for adding a new database to the Config
     *   [UN-]PUBLISH-DATABASE   for temporary change of the database status to be unpublished/published
     *   DELETE-TABLE            for removing a known database table from the Config
     *   ADD-TABLE               for adding a new database table to the Config
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName = std::string(),
                        http::AuthType const authType = http::AuthType::NONE);

    HttpConfigModule() = delete;
    HttpConfigModule(HttpConfigModule const&) = delete;
    HttpConfigModule& operator=(HttpConfigModule const&) = delete;

    ~HttpConfigModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpConfigModule(Controller::Ptr const& controller, std::string const& taskName,
                     qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp);

    /**
     * Return the current Config of the system.
     */
    nlohmann::json _get();

    /**
     * Process a request which updates the Config of the Replication
     * system and reports back its new state.
     */
    nlohmann::json _updateGeneral();

    /**
     * Process a request which updates parameters of an existing worker in
     * the Config of the Replication system and reports back the new
     * state of the system
     */
    nlohmann::json _updateWorker();

    /**
     * Process a request which removes an existing worker from the Config
     * of the Replication system and reports back the new state of the system
     */
    nlohmann::json _deleteWorker();

    /**
     * Process a request which adds a new worker into the Config
     * of the Replication system and reports back the new state of the system
     */
    nlohmann::json _addWorker();

    /**
     * Process a request which removes an existing database family from
     * the Config of the Replication system and reports back the new
     * state of the system
     */
    nlohmann::json _deleteFamily();

    /**
     * Process a request which adds a new database family into the Config
     * of the Replication system and reports back the new state of the system
     */
    nlohmann::json _addFamily();

    /**
     * Process a request which removes an existing database from the Config
     * of the Replication system and reports back the new state of the system
     */
    nlohmann::json _deleteDatabase();

    /**
     * Process a request which adds a new database into the Config
     * of the Replication system and reports back the new state of the system
     */
    nlohmann::json _addDatabase();

    /// The method implements the common logic for the publish and unpublish requests.
    nlohmann::json _unpublishDatabase();

    /**
     * Process a request which removes an existing table from the Config
     * of the Replication system and reports back the new state of the system
     */
    nlohmann::json _deleteTable();

    /**
     * Process a request which adds a new database table into the Config
     * of the Replication system and reports back the new state of the system
     */
    nlohmann::json _addTable();
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPCONFIGMODULE_H
