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
#ifndef LSST_QSERV_HTTPCONFIGURATIONMODULE_H
#define LSST_QSERV_HTTPCONFIGURATIONMODULE_H

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
 * Class HttpConfigurationModule implements a handler for reporting
 * various info on or modifying Configuration of the Replication system.
 */
class HttpConfigurationModule: public HttpModule {
public:
    typedef std::shared_ptr<HttpConfigurationModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   the empty string        for reporting the current state of the Configuration
     *   UPDATE-GENERAL          for updating one or many general configuration parameters
     *   UPDATE-WORKER           for updating configuration parameters of a worker
     *   DELETE-WORKER           for removing a known worker from the Configuration
     *   ADD-WORKER              for adding a new worker to the Configuration
     *   DELETE-DATABASE-FAMILY  for removing a known database family from the Configuration
     *   ADD-DATABASE-FAMILY     for adding a new database family to the Configuration
     *   DELETE-DATABASE         for removing a known database from the Configuration
     *   ADD-DATABASE            for adding a new database to the Configuration
     *   DELETE-TABLE            for removing a known database table from the Configuration
     *   ADD-TABLE               for adding a new database table to the Configuration
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

    HttpConfigurationModule() = delete;
    HttpConfigurationModule(HttpConfigurationModule const&) = delete;
    HttpConfigurationModule& operator=(HttpConfigurationModule const&) = delete;

    ~HttpConfigurationModule() final = default;

protected:
    void executeImpl(std::string const& subModuleName) final;

private:
    HttpConfigurationModule(Controller::Ptr const& controller,
                            std::string const& taskName,
                            HttpProcessorConfig const& processorConfig,
                            qhttp::Request::Ptr const& req,
                            qhttp::Response::Ptr const& resp);

    /**
     * Return the current Configuration of the system.
     */
    void _get();

    /**
     * Process a request which updates the Configuration of the Replication
     * system and reports back its new state.
     */
    void _updateGeneral();

    /**
     * Process a request which updates parameters of an existing worker in
     * the Configuration of the Replication system and reports back the new
     * state of the system
     */
    void _updateWorker();

    /**
     * Process a request which removes an existing worker from the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _deleteWorker();

    /**
     * Process a request which adds a new worker into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addWorker();

    /**
     * Process a request which removes an existing database family from
     * the Configuration of the Replication system and reports back the new
     * state of the system
     */
    void _deleteFamily();

    /**
     * Process a request which adds a new database family into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addFamily();

    /**
     * Process a request which removes an existing database from the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _deleteDatabase();

    /**
     * Process a request which adds a new database into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addDatabase();

    /**
     * Process a request which removes an existing table from the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _deleteTable();

    /**
     * Process a request which adds a new database table into the Configuration
     * of the Replication system and reports back the new state of the system
     */
    void _addTable();
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPCONFIGURATIONMODULE_H
