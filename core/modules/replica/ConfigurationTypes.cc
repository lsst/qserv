/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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

using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

json ConfigurationGeneralParams::toJson(Configuration::Ptr const& config,
                                        bool const scrambleDbPassword) const {
    json generalJson;

    generalJson.push_back({
        {"parameter",   requestBufferSizeBytes.key},
        {"value",       requestBufferSizeBytes.get(config)},
        {"description", requestBufferSizeBytes.description}});

    generalJson.push_back({
        {"parameter",   retryTimeoutSec.key},
        {"value",       retryTimeoutSec.get(config)},
        {"description", retryTimeoutSec.description}});

    generalJson.push_back({
        {"parameter",   controllerThreads.key},
        {"value",       controllerThreads.get(config)},
        {"description", controllerThreads.description}});

    generalJson.push_back({
        {"parameter",   controllerHttpPort.key},
        {"value",       controllerHttpPort.get(config)},
        {"description", controllerHttpPort.description}});

    generalJson.push_back({
        {"parameter",   controllerHttpThreads.key},
        {"value",       controllerHttpThreads.get(config)},
        {"description", controllerHttpThreads.description}});

    generalJson.push_back({
        {"parameter",   controllerRequestTimeoutSec.key},
        {"value",       controllerRequestTimeoutSec.get(config)},
        {"description", controllerRequestTimeoutSec.description}});

    generalJson.push_back({
        {"parameter",   jobTimeoutSec.key},
        {"value",       jobTimeoutSec.get(config)},
        {"description", jobTimeoutSec.description}});

    generalJson.push_back({
        {"parameter",   jobHeartbeatTimeoutSec.key},
        {"value",       jobHeartbeatTimeoutSec.get(config)},
        {"description", jobHeartbeatTimeoutSec.description}});

    generalJson.push_back({
        {"parameter",   xrootdAutoNotify.key},
        {"value",       xrootdAutoNotify.get(config)},
        {"description", xrootdAutoNotify.description}});

    generalJson.push_back({
        {"parameter",   xrootdHost.key},
        {"value",       xrootdHost.get(config)},
        {"description", xrootdHost.description}});

    generalJson.push_back({
        {"parameter",   xrootdPort.key},
        {"value",       xrootdPort.get(config)},
        {"description", xrootdPort.description}});

    generalJson.push_back({
        {"parameter",   xrootdTimeoutSec.key},
        {"value",       xrootdTimeoutSec.get(config)},
        {"description", xrootdTimeoutSec.description}});

    generalJson.push_back({
        {"parameter",   databaseTechnology.key},
        {"value",       databaseTechnology.get(config)},
        {"description", databaseTechnology.description}});

    generalJson.push_back({
        {"parameter",   databaseHost.key},
        {"value",       databaseHost.get(config)},
        {"description", databaseHost.description}});

    generalJson.push_back({
        {"parameter",   databasePort.key},
        {"value",       databasePort.get(config)},
        {"description", databasePort.description}});

    generalJson.push_back({
        {"parameter",   databaseUser.key},
        {"value",       databaseUser.get(config)},
        {"description", databaseUser.description}});

    generalJson.push_back({
        {"parameter",   databasePassword.key},
        {"value",       databasePassword.get(config, scrambleDbPassword)},
        {"description", databasePassword.description}});

    generalJson.push_back({
        {"parameter",   databaseName.key},
        {"value",       databaseName.get(config)},
        {"description", databaseName.description}});

    generalJson.push_back({
        {"parameter",   databaseServicesPoolSize.key},
        {"value",       databaseServicesPoolSize.get(config)},
        {"description", databaseServicesPoolSize.description}});

    generalJson.push_back({
        {"parameter",   workerTechnology.key},
        {"value",       workerTechnology.get(config)},
        {"description", workerTechnology.description}});

    generalJson.push_back({
        {"parameter",   workerNumProcessingThreads.key},
        {"value",       workerNumProcessingThreads.get(config)},
        {"description", workerNumProcessingThreads.description}});

    generalJson.push_back({
        {"parameter",   fsNumProcessingThreads.key},
        {"value",       fsNumProcessingThreads.get(config)},
        {"description", fsNumProcessingThreads.description}});

    generalJson.push_back({
        {"parameter",   workerFsBufferSizeBytes.key},
        {"value",       workerFsBufferSizeBytes.get(config)},
        {"description", workerFsBufferSizeBytes.description}});

    return generalJson;
}

}}} // namespace lsst::qserv::replica
