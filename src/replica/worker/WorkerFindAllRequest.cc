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
#include "replica/worker/WorkerFindAllRequest.h"

// System headers
#include <map>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/util/FileUtils.h"
#include "replica/services/ServiceProvider.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindAllRequest");

}  // namespace

namespace lsst::qserv::replica {

WorkerFindAllRequest::Ptr WorkerFindAllRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                                       string const& worker, string const& id, int priority,
                                                       ExpirationCallbackType const& onExpired,
                                                       unsigned int requestExpirationIvalSec,
                                                       ProtocolRequestFindAll const& request) {
    auto ptr = WorkerFindAllRequest::Ptr(new WorkerFindAllRequest(
            serviceProvider, worker, id, priority, onExpired, requestExpirationIvalSec, request));
    ptr->init();
    return ptr;
}

WorkerFindAllRequest::WorkerFindAllRequest(ServiceProvider::Ptr const& serviceProvider, string const& worker,
                                           string const& id, int priority,
                                           ExpirationCallbackType const& onExpired,
                                           unsigned int requestExpirationIvalSec,
                                           ProtocolRequestFindAll const& request)
        : WorkerRequest(serviceProvider, worker, "FIND-ALL", id, priority, onExpired,
                        requestExpirationIvalSec),
          _request(request) {}

void WorkerFindAllRequest::setInfo(ProtocolResponseFindAll& response) const {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    replica::Lock lock(_mtx, context(__func__));
    response.set_allocated_target_performance(performance().info().release());
    for (auto&& replicaInfo : _replicaInfoCollection) {
        replicaInfo.setInfo(response.add_replica_info_many());
    }
    *(response.mutable_request()) = _request;
}

bool WorkerFindAllRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  database: " << database());
    replica::Lock lock(_mtx, context(__func__));
    checkIfCancelling(lock, __func__);
    auto const config = _serviceProvider->config();
    DatabaseInfo const databaseInfo = config->databaseInfo(database());

    // Scan the data directory to find all files which match the expected pattern(s)
    // and group them by their chunk number
    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code ec;

    map<unsigned int, ReplicaInfo::FileInfoCollection> chunk2fileInfoCollection;
    {
        replica::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));
        fs::path const dataDir = fs::path(config->get<string>("worker", "data-dir")) / database();
        fs::file_status const stat = fs::status(dataDir, ec);
        errorContext = errorContext or
                       reportErrorIf(stat.type() == fs::status_error, ProtocolStatusExt::FOLDER_STAT,
                                     "failed to check the status of directory: " + dataDir.string()) or
                       reportErrorIf(not fs::exists(stat), ProtocolStatusExt::NO_FOLDER,
                                     "the directory does not exists: " + dataDir.string());
        try {
            for (fs::directory_entry& entry : fs::directory_iterator(dataDir)) {
                tuple<string, unsigned int, string> parsed;
                if (FileUtils::parsePartitionedFile(parsed, entry.path().filename().string(), databaseInfo)) {
                    LOGS(_log, LOG_LVL_DEBUG,
                         context(__func__) << "  database: " << database() << "  file: "
                                           << entry.path().filename() << "  table: " << get<0>(parsed)
                                           << "  chunk: " << get<1>(parsed) << "  ext: " << get<2>(parsed));

                    uint64_t const size = fs::file_size(entry.path(), ec);
                    errorContext = errorContext or
                                   reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_SIZE,
                                                 "failed to read file size: " + entry.path().string());

                    time_t const mtime = fs::last_write_time(entry.path(), ec);
                    errorContext = errorContext or
                                   reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_MTIME,
                                                 "failed to read file mtime: " + entry.path().string());

                    unsigned const chunk = get<1>(parsed);
                    chunk2fileInfoCollection[chunk].emplace_back(ReplicaInfo::FileInfo({
                            entry.path().filename().string(), size, mtime,
                            "",  /* cs is never computed for this type of requests */
                            0,   /* beginTransferTime */
                            0,   /* endTransferTime */
                            size /* inSize */
                    }));
                }
            }
        } catch (fs::filesystem_error const& ex) {
            errorContext = errorContext or reportErrorIf(true, ProtocolStatusExt::FOLDER_READ,
                                                         "failed to read the directory: " + dataDir.string() +
                                                                 ", error: " + string(ex.what()));
        }
    }
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return true;
    }

    // Analyze results to see which chunks are complete using chunk 0 as an example
    // of the total number of files which are normally associated with each chunk.
    size_t const numFilesPerChunkRequired = FileUtils::partitionedFiles(databaseInfo, 0).size();
    for (auto&& entry : chunk2fileInfoCollection) {
        unsigned int const chunk = entry.first;
        size_t const numFiles = entry.second.size();
        _replicaInfoCollection.emplace_back(
                numFiles < numFilesPerChunkRequired ? ReplicaInfo::INCOMPLETE : ReplicaInfo::COMPLETE,
                worker(), database(), chunk, util::TimeUtils::now(), chunk2fileInfoCollection[chunk]);
    }
    setStatus(lock, ProtocolStatus::SUCCESS);
    return true;
}

}  // namespace lsst::qserv::replica
