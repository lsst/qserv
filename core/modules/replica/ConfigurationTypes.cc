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

namespace lsst {
namespace qserv {
namespace replica {

json ConfigurationGeneralParams::toJson(Configuration const& config) const {
    json result;
    result.push_back(metaVersion.toJson(config));
    result.push_back(requestBufferSizeBytes.toJson(config));
    result.push_back(retryTimeoutSec.toJson(config));
    result.push_back(controllerThreads.toJson(config));
    result.push_back(controllerRequestTimeoutSec.toJson(config));
    result.push_back(jobTimeoutSec.toJson(config));
    result.push_back(jobHeartbeatTimeoutSec.toJson(config));
    result.push_back(controllerHttpPort.toJson(config));
    result.push_back(controllerHttpThreads.toJson(config));
    result.push_back(controllerEmptyChunksDir.toJson(config));
    result.push_back(xrootdAutoNotify.toJson(config));
    result.push_back(xrootdHost.toJson(config));
    result.push_back(xrootdPort.toJson(config));
    result.push_back(xrootdTimeoutSec.toJson(config));
    result.push_back(databaseServicesPoolSize.toJson(config));
    result.push_back(databaseHost.toJson(config));
    result.push_back(databasePort.toJson(config));
    result.push_back(databaseUser.toJson(config));
    result.push_back(databaseName.toJson(config));
    result.push_back(qservMasterDatabaseServicesPoolSize.toJson(config));
    result.push_back(qservMasterDatabaseHost.toJson(config));
    result.push_back(qservMasterDatabasePort.toJson(config));
    result.push_back(qservMasterDatabaseUser.toJson(config));
    result.push_back(qservMasterDatabaseName.toJson(config));
    result.push_back(qservMasterDatabaseTmpDir.toJson(config));
    result.push_back(workerTechnology.toJson(config));
    result.push_back(workerNumProcessingThreads.toJson(config));
    result.push_back(fsNumProcessingThreads.toJson(config));
    result.push_back(workerFsBufferSizeBytes.toJson(config));
    result.push_back(loaderNumProcessingThreads.toJson(config));
    result.push_back(exporterNumProcessingThreads.toJson(config));
    result.push_back(httpLoaderNumProcessingThreads.toJson(config));
    result.push_back(workerDefaultSvcPort.toJson(config));
    result.push_back(workerDefaultFsPort.toJson(config));
    result.push_back(workerDefaultDataDir.toJson(config));
    result.push_back(workerDefaultDbPort.toJson(config));
    result.push_back(workerDefaultDbUser.toJson(config));
    result.push_back(workerDefaultLoaderPort.toJson(config));
    result.push_back(workerDefaultLoaderTmpDir.toJson(config));
    result.push_back(workerDefaultExporterPort.toJson(config));
    result.push_back(workerDefaultExporterTmpDir.toJson(config));
    result.push_back(workerDefaultHttpLoaderPort.toJson(config));
    result.push_back(workerDefaultHttpLoaderTmpDir.toJson(config));
    return result;
}

}}} // namespace lsst::qserv::replica
