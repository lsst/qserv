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


string ConfigurationFile::dump2init(ConfigurationIFace::Ptr const& config) {

    using namespace std;

    if (config == nullptr) {
        throw invalid_argument(
                "ConfigurationFile::" + string(__func__) + "  the configuration can't be empty");
    }
    ostringstream str;

    string const noSpecificFamily;
    bool const allDatabases = true;

    str << "[common]\n"
        << "\n"
        << "workers                    = " << config->allWorkers() << "\n"
        << "database_families          = " << config->databaseFamilies() << "\n"
        << "databases                  = " << config->databases(noSpecificFamily, allDatabases) << "\n"
        << "request_buf_size_bytes     = " << config->requestBufferSizeBytes() << "\n"
        << "request_retry_interval_sec = " << config->retryTimeoutSec() << "\n"
        << "\n";

    str << "[controller]\n"
        << "\n"
        << "num_threads         = " << config->controllerThreads() << "\n"
        << "http_server_port    = " << config->controllerHttpPort() << "\n"
        << "http_server_threads = " << config->controllerHttpThreads() << "\n"
        << "request_timeout_sec = " << config->controllerRequestTimeoutSec() << "\n"
        << "empty_chunks_dir    = " << config->controllerEmptyChunksDir() << "\n"
        << "job_timeout_sec     = " << config->jobTimeoutSec() << "\n"
        << "job_heartbeat_sec   = " << config->jobHeartbeatTimeoutSec() << "\n"
        << "\n";

    str << "[database]\n"
        << "\n"
        << "technology         = " << config->databaseTechnology() << "\n"
        << "host               = " << config->databaseHost() << "\n"
        << "port               = " << config->databasePort() << "\n"
        << "user               = " << config->databaseUser() << "\n"
        << "password           = " << "" << "\n"
        << "name               = " << config->databaseName() << "\n"
        << "services_pool_size = " << config->databaseServicesPoolSize() << "\n"
        << "qserv_master_host  = " << config->qservMasterDatabaseHost() << "\n"
        << "qserv_master_port  = " << config->qservMasterDatabasePort() << "\n"
        << "qserv_master_user  = " << config->qservMasterDatabaseUser() << "\n"
        << "qserv_master_name  = " << config->qservMasterDatabaseName() << "\n"
        << "qserv_master_services_pool_size = " << config->qservMasterDatabaseServicesPoolSize() << "\n"
        << "qserv_master_tmp_dir = " << config->qservMasterDatabaseTmpDir() << "\n"

        << "\n";

    str << "[xrootd]\n"
        << "\n"
        << "auto_notify         = " << (config->xrootdAutoNotify() ? "1" : "0") << "\n"
        << "host                = " << config->xrootdHost() << "\n"
        << "port                = " << config->xrootdPort() << "\n"
        << "request_timeout_sec = " << config->xrootdTimeoutSec() << "\n"
        << "\n";

    str << "[worker]\n"
        << "\n"
        << "technology                 = " << config->workerTechnology() << "\n"
        << "num_svc_processing_threads = " << config->workerNumProcessingThreads() << "\n"
        << "num_fs_processing_threads  = " << config->fsNumProcessingThreads() << "\n"
        << "fs_buf_size_bytes          = " << config->workerFsBufferSizeBytes() << "\n"
        << "num_loader_processing_threads = " << config->loaderNumProcessingThreads() << "\n"
        << "svc_host                   = " << defaultWorkerSvcHost << "\n"
        << "svc_port                   = " << defaultWorkerSvcPort << "\n"
        << "fs_host                    = " << defaultWorkerFsHost << "\n"
        << "fs_port                    = " << defaultWorkerFsPort << "\n"
        << "data_dir                   = " << defaultDataDir << "\n"
        << "db_host                    = " << defaultWorkerDbHost << "\n"
        << "db_port                    = " << defaultWorkerDbPort << "\n"
        << "db_user                    = " << defaultWorkerDbUser << "\n"
        << "loader_host                = " << defaultWorkerLoaderHost << "\n"
        << "loader_port                = " << defaultWorkerLoaderPort << "\n"
        << "loader_tmp_dir             = " << defaultWorkerLoaderTmpDir << "\n"
        << "\n";

    for (auto&& worker: config->allWorkers()) {
        auto&& info = config->workerInfo(worker);
        str << "[worker:" << info.name << "]\n"
            << "\n"
            << "is_enabled   = " << (info.isEnabled  ? "1" : "0") << "\n"
            << "is_read_only = " << (info.isReadOnly ? "1" : "0") << "\n"
            << "svc_host     = " << info.svcHost << "\n"
            << "svc_port     = " << info.svcPort << "\n"
            << "fs_host      = " << info.fsHost << "\n"
            << "fs_port      = " << info.fsPort << "\n"
            << "data_dir     = " << info.dataDir << "\n"
            << "db_host      = " << info.dbHost << "\n"
            << "db_port      = " << info.dbPort << "\n"
            << "db_user      = " << info.dbUser << "\n"
            << "loader_host    = " << info.loaderHost << "\n"
            << "loader_port    = " << info.loaderPort << "\n"
            << "loader_tmp_dir = " << info.loaderTmpDir << "\n"
            << "\n";
    }
    for (auto&& family: config->databaseFamilies()) {
        auto&& info = config->databaseFamilyInfo(family);
        str << "[database_family:" << info.name << "]\n"
            << "\n"
            << "min_replication_level = " << info.replicationLevel << "\n"
            << "num_stripes           = " << info.numStripes << "\n"
            << "num_sub_stripes       = " << info.numSubStripes << "\n"
            << "overlap               = " << info.overlap << "\n"
            << "\n";
    }
    for (auto&& database: config->databases(noSpecificFamily, allDatabases)) {
        auto&& info = config->databaseInfo(database);
        str << "[database:" << info.name << "]\n"
            << "\n"
            << "family             = " << info.family << "\n"
            << "is_published       = " << (info.isPublished ? "1" : "0") << "\n"
            << "partitioned_tables = " << info.partitionedTables << "\n"
            << "regular_tables     = " << info.regularTables << "\n"
            << "director_table     = " << info.directorTable << "\n"
            << "director_table_key = " << info.directorTableKey << "\n"
            << "chunk_id_key       = " << info.chunkIdColName << "\n"
            << "sub_chunk_id_key   = " << info.subChunkIdColName << "\n"
            << "\n";
        for (auto&& table: info.partitionedTables) {
            str << "[table:" << info.name << "." << table << "]\n"
                << "\n"
                << "latitude_key  = " << info.latitudeColName[table] << "\n"
                << "longitude_key = " << info.longitudeColName[table] << "\n"
                << "\n";
        }
        for (auto&& table: info.regularTables) {
            str << "[table:" << info.name << "." << table << "]\n"
                << "\n"
                << "latitude_key  = \n"
                << "longitude_key = \n"
                << "\n";
        }
    }
    return str.str();
}

    
ConfigurationFile::ConfigurationFile(string const& configFile)
    :   ConfigurationStore(util::ConfigStore(configFile)),
        _configFile(configFile) {
}

}}} // namespace lsst::qserv::replica
