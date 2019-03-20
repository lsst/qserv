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
#include "replica/ConfigurationFile.h"

// System headers
#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <sstream>
#include <vector>

// Qserv headers
#include "util/ConfigStore.h"

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

/**
 * 
 * @param os
 *   a reference to the output stream
 *
 * @param names
 *   a collection of strings to be printed
 *
 * @return
 *   a reference to the output stream
 */
inline ostream& operator<<(ostream& os, vector<string> const& c) {
    copy(c.cbegin(), c.cend(), ostream_iterator<string>(os, " "));
    return os;
}


string ConfigurationFile::dump2init(Configuration::Ptr const& config) {

    using namespace std;

    if (config == nullptr) {
        throw invalid_argument(
                "ConfigurationFile::" + string(__func__) + "  the configuration can't be empty");
    }
    ostringstream str;

    str << "[common]\n"
        << "\n"
        << "workers                    = " <<           config->allWorkers()              << "\n"
        << "database_families          = " <<           config->databaseFamilies()        << "\n"
        << "databases                  = " <<           config->databases()               << "\n"
        << "request_buf_size_bytes     = " << to_string(config->requestBufferSizeBytes()) << "\n"
        << "request_retry_interval_sec = " << to_string(config->retryTimeoutSec())        << "\n"
        << "\n";

    str << "[controller]\n"
        << "\n"
        << "num_threads         = " << to_string(config->controllerThreads())           << "\n"
        << "http_server_port    = " << to_string(config->controllerHttpPort())          << "\n"
        << "http_server_threads = " << to_string(config->controllerHttpThreads())       << "\n"
        << "request_timeout_sec = " << to_string(config->controllerRequestTimeoutSec()) << "\n"
        << "job_timeout_sec     = " << to_string(config->jobTimeoutSec())               << "\n"
        << "job_heartbeat_sec   = " << to_string(config->jobHeartbeatTimeoutSec())      << "\n"
        << "\n";

    str << "[database]\n"
        << "\n"
        << "technology         = " <<           config->databaseTechnology()        << "\n"
        << "host               = " <<           config->databaseHost()              << "\n"
        << "port               = " << to_string(config->databasePort())             << "\n"
        << "user               = " <<           config->databaseUser()              << "\n"
        << "password           = " <<           config->databasePassword()          << "\n"
        << "name               = " <<           config->databaseName()              << "\n"
        << "services_pool_size = " << to_string(config->databaseServicesPoolSize()) << "\n"
        << "\n";

    str << "[xrootd]\n"
        << "\n"
        << "auto_notify         = " <<          (config->xrootdAutoNotify() ? "1" : "0") << "\n"
        << "host                = " <<           config->xrootdHost()                    << "\n"
        << "port                = " << to_string(config->xrootdPort())                   << "\n"
        << "request_timeout_sec = " << to_string(config->xrootdTimeoutSec())             << "\n"
        << "\n";

    str << "[worker]\n"
        << "\n"
        << "technology                 = " <<           config->workerTechnology()            << "\n"
        << "num_svc_processing_threads = " << to_string(config->workerNumProcessingThreads()) << "\n"
        << "num_fs_processing_threads  = " << to_string(config->fsNumProcessingThreads())     << "\n"
        << "fs_buf_size_bytes          = " << to_string(config->workerFsBufferSizeBytes())    << "\n"
        << "svc_host                   = " <<           defaultWorkerSvcHost                  << "\n"
        << "svc_port                   = " << to_string(defaultWorkerSvcPort)                 << "\n"
        << "fs_host                    = " <<           defaultWorkerFsHost                   << "\n"
        << "fs_port                    = " << to_string(defaultWorkerFsPort)                  << "\n"
        << "data_dir                   = " <<           defaultDataDir                        << "\n"
        << "db_host                    = " <<           defaultWorkerDbHost                   << "\n"
        << "db_port                    = " << to_string(defaultWorkerDbPort)                  << "\n"
        << "db_user                    = " <<           defaultWorkerDbUser                   << "\n"
        << "db_password                = " <<           defaultWorkerDbPassword               << "\n"
        << "\n";

    for (auto&& worker: config->allWorkers()) {
        auto&& info = config->workerInfo(worker);
        str << "[worker:" << info.name << "]\n"
            << "\n"
            << "is_enabled   = " <<          (info.isEnabled  ? "1" : "0") << "\n"
            << "is_read_only = " <<          (info.isReadOnly ? "1" : "0") << "\n"
            << "svc_host     = " <<           info.svcHost                 << "\n"
            << "svc_port     = " << to_string(info.svcPort)                << "\n"
            << "fs_host      = " <<           info.fsHost                  << "\n"
            << "fs_port      = " << to_string(info.fsPort)                 << "\n"
            << "data_dir     = " <<           info.dataDir                 << "\n"
            << "db_host      = " <<           info.dbHost                  << "\n"
            << "db_port      = " << to_string(info.dbPort)                 << "\n"
            << "db_user      = " <<           info.dbUser                  << "\n"
            << "db_password  = " <<           info.dbPassword              << "\n"
            << "\n";
    }
    for (auto&& family: config->databaseFamilies()) {
        auto&& info = config->databaseFamilyInfo(family);
        str << "[database_family:" << info.name << "]\n"
            << "\n"
            << "min_replication_level = " << to_string(info.replicationLevel) << "\n"
            << "num_stripes           = " << to_string(info.numStripes)       << "\n"
            << "num_sub_stripes       = " << to_string(info.numSubStripes)    << "\n"
            << "\n";
    }
    for (auto&& database: config->databases()) {
        auto&& info = config->databaseInfo(database);
        str << "[database:" << info.name << "]\n"
            << "\n"
            << "family             = " << info.family            << "\n"
            << "partitioned_tables = " << info.partitionedTables << "\n"
            << "regular_tables     = " << info.regularTables     << "\n"
            << "\n";
    }
    return str.str();
}

    
ConfigurationFile::ConfigurationFile(string const& configFile)
    :   ConfigurationStore(util::ConfigStore(configFile)),
        _configFile(configFile) {
}

}}} // namespace lsst::qserv::replica
