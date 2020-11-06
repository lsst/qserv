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

// Class header
#include "replica/HttpModule.h"

// Qserv headers
#include "css/CssAccess.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQL.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>

using namespace std;
using json = nlohmann::json;

namespace {
    LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.HttpModule");
}

namespace lsst {
namespace qserv {
namespace replica {

HttpModule::HttpModule(Controller::Ptr const& controller,
                       string const& taskName,
                       HttpProcessorConfig const& processorConfig,
                       qhttp::Request::Ptr const& req,
                       qhttp::Response::Ptr const& resp)
    :   EventLogger(controller, taskName),
        HttpModuleBase(processorConfig.authKey,
                       req,
                       resp) {
}


string HttpModule::context() const {
    return name() + " ";
}


database::mysql::Connection::Ptr HttpModule::qservMasterDbConnection(string const& database) const {
    auto const config = controller()->serviceProvider()->config();
    return database::mysql::Connection::open(
        database::mysql::ConnectionParams(
            config->qservMasterDatabaseHost(),
            config->qservMasterDatabasePort(),
            "root",
            Configuration::qservMasterDatabasePassword(),
            database
        )
    );
}


shared_ptr<css::CssAccess> HttpModule::qservCssAccess(bool readOnly) const {
    auto const config = controller()->serviceProvider()->config();
    map<string, string> cssConfig;
    cssConfig["technology"] = "mysql";
    // Address translation is required because CSS MySQL connector doesn't set
    // the TCP protocol option for 'localhost' and tries to connect via UNIX socket.
    cssConfig["hostname"] = config->qservMasterDatabaseHost() == "localhost" ?
            "127.0.0.1" : config->qservMasterDatabaseHost();
    cssConfig["port"] = to_string(config->qservMasterDatabasePort());
    cssConfig["username"] = "root";
    cssConfig["password"] = Configuration::qservMasterDatabasePassword();
    cssConfig["database"] = "qservCssData";
    return css::CssAccess::createFromConfig(cssConfig, config->controllerEmptyChunksDir());
}

}}}  // namespace lsst::qserv::replica
