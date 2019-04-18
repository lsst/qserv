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
#include "replica/ConfigurationTypes.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {
    
template <typename T>
json paramToJson(T const& struct_,
                 Configuration::Ptr const& config) {
    return json({
        {"parameter",   struct_.key},
        {"value",       struct_.get(config)},
        {"updatable",   struct_.updatable ? 1 : 0},
        {"description", struct_.description}});
}

}  // namespace


namespace lsst {
namespace qserv {
namespace replica {

json ConfigurationGeneralParams::toJson(Configuration::Ptr const& config) const {

    json result;

    result.push_back(::paramToJson(requestBufferSizeBytes,      config));
    result.push_back(::paramToJson(retryTimeoutSec,             config));
    result.push_back(::paramToJson(controllerThreads,           config));
    result.push_back(::paramToJson(controllerHttpPort,          config));
    result.push_back(::paramToJson(controllerHttpThreads,       config));
    result.push_back(::paramToJson(controllerRequestTimeoutSec, config));
    result.push_back(::paramToJson(jobTimeoutSec,               config));
    result.push_back(::paramToJson(jobHeartbeatTimeoutSec,      config));
    result.push_back(::paramToJson(xrootdAutoNotify,            config));
    result.push_back(::paramToJson(xrootdHost,                  config));
    result.push_back(::paramToJson(xrootdPort,                  config));
    result.push_back(::paramToJson(xrootdTimeoutSec,            config));
    result.push_back(::paramToJson(databaseTechnology,          config));
    result.push_back(::paramToJson(databaseHost,                config));
    result.push_back(::paramToJson(databasePort,                config));
    result.push_back(::paramToJson(databaseUser,                config));
    result.push_back(::paramToJson(databasePassword,            config));
    result.push_back(::paramToJson(databaseName,                config));
    result.push_back(::paramToJson(databaseServicesPoolSize,    config));
    result.push_back(::paramToJson(qservMasterDatabaseHost,     config));
    result.push_back(::paramToJson(qservMasterDatabasePort,     config));
    result.push_back(::paramToJson(qservMasterDatabaseUser,     config));
    result.push_back(::paramToJson(qservMasterDatabasePassword, config));
    result.push_back(::paramToJson(qservMasterDatabaseName,     config));
    result.push_back(::paramToJson(qservMasterDatabaseServicesPoolSize, config));
    result.push_back(::paramToJson(workerTechnology,            config));
    result.push_back(::paramToJson(workerNumProcessingThreads,  config));
    result.push_back(::paramToJson(fsNumProcessingThreads,      config));
    result.push_back(::paramToJson(workerFsBufferSizeBytes,     config));

    return result;
}

}}} // namespace lsst::qserv::replica
