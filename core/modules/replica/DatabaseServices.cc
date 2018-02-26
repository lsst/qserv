/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/DatabaseServices.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Controller.h"
#include "replica/DatabaseServicesMySQL.h"
#include "replica/Job.h"
#include "replica/ReplicaInfo.h"


namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServices");

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

DatabaseServices::pointer
DatabaseServices::create (Configuration::pointer const& configuration) {

    // If the configuration is pulled from a database then *try*
    // using the corresponding technology.

    if ("mysql" == configuration->databaseTechnology()) {
        try {
            return DatabaseServices::pointer (
                new DatabaseServicesMySQL (configuration));
        } catch (database::mysql::Error const& ex) {
            LOGS(_log, LOG_LVL_ERROR, "DatabaseServices::  failed to instantiate MySQL-based database services" <<
                                      ", error: " << ex.what() <<
                                      ", no such service will be available to the application.");
        }
    }

    // Otherwise assume the current 'dummy' implementation.
    return DatabaseServices::pointer (
            new DatabaseServices (configuration));
}

DatabaseServices::DatabaseServices (Configuration::pointer const& configuration)
    :   _configuration (configuration) {
}

void
DatabaseServices::saveState (ControllerIdentity const& identity,
                             uint64_t                  startTime) {

    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServices::saveState[Controller]");
}

void
DatabaseServices::saveState (Job_pointer const& job) {
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServices::saveState[Job::" << job->type() << "]");
}

void
DatabaseServices::saveState (Request_pointer const& request) {
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServices::saveState[Request::" << request->type() << "]");
}

bool
DatabaseServices::findOldestReplicas (std::vector<ReplicaInfo>& replicas,
                                      size_t                    maxReplicas,
                                      bool                      enabledWorkersOnly) const {
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServices::findOldestReplicas");

    return false;
}
 
bool
DatabaseServices::findReplicas (std::vector<ReplicaInfo>& replicas,
                                unsigned int              chunk,
                                std::string const&        database,
                                bool                      enabledWorkersOnly) const {
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServices::findReplicas "
         << " chunk: "    << chunk
         << " database: " << database);

    return false;
}

bool
DatabaseServices::findWorkerReplicas (std::vector<ReplicaInfo>& replicas,
                                      std::string const&        worker,
                                      std::string const&        database) const {
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServices::findWorkerReplicas(worker[,database]) "
         << " worker: "  << worker
         << " database:" << database);

    return false;
}

bool
DatabaseServices::findWorkerReplicas (std::vector<ReplicaInfo>& replicas,
                                      unsigned int              chunk,
                                      std::string const&        worker,
                                      std::string const&        databaseFamily) const {
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseServices::findWorkerReplicas(chunk,worker[,databaseFamily]) "
         << " worker: " << worker
         << " databaseFamily: " << databaseFamily);

    return false;
}
}}} // namespace lsst::qserv::replica