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
#include "replica/worker/WorkerFindAllReplicasHttpRequest.h"

// System headers
#include <filesystem>
#include <map>
#include <system_error>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/config/ConfigDatabase.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/proto/Protocol.h"
#include "replica/util/FileUtils.h"
#include "replica/services/ServiceProvider.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

#define CONTEXT context("WorkerFindAllReplicasHttpRequest", __func__)

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindAllReplicasHttpRequest");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<WorkerFindAllReplicasHttpRequest> WorkerFindAllReplicasHttpRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, json const& req, ExpirationCallbackType const& onExpired) {
    auto ptr = shared_ptr<WorkerFindAllReplicasHttpRequest>(
            new WorkerFindAllReplicasHttpRequest(serviceProvider, worker, hdr, req, onExpired));
    ptr->init();
    return ptr;
}

WorkerFindAllReplicasHttpRequest::WorkerFindAllReplicasHttpRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, json const& req, ExpirationCallbackType const& onExpired)
        : WorkerHttpRequest(serviceProvider, worker, "FIND-ALL", hdr, req, onExpired),
          _databaseName(reqParamString("database")) {}

void WorkerFindAllReplicasHttpRequest::getResult(json& result) const {
    result["replica_info_many"] = json::array();
    for (auto const& replicaInfo : _replicaInfoCollection) {
        result["replica_info_many"].push_back(replicaInfo.toJson());
    }
}

bool WorkerFindAllReplicasHttpRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, CONTEXT << " database: " << _databaseName);

    replica::Lock lock(mtx, CONTEXT);
    checkIfCancelling(lock, CONTEXT);

    // The method will throw ConfigUnknownDatabase if the database is invalid.
    DatabaseInfo const databaseInfo = serviceProvider()->config()->databaseInfo(_databaseName);

    // Scan the data directory to find all files which match the expected pattern(s)
    // and group them by their chunk number
    WorkerHttpRequest::ErrorContext errorContext;
    std::error_code ec;

    map<unsigned int, ReplicaInfo::FileInfoCollection> chunk2fileInfoCollection;
    {
        replica::Lock dataFolderLock(mtxDataFolderOperations, CONTEXT);
        fs::path const dataDir = fs::path(serviceProvider()->config()->get<string>("worker", "data-dir")) /
                                 database::mysql::obj2fs(_databaseName);
        fs::file_status const stat = fs::status(dataDir, ec);
        errorContext =
                errorContext ||
                reportErrorIf(stat.type() == fs::file_type::none, protocol::StatusExt::FOLDER_STAT,
                              "failed to check the status of directory: " + dataDir.string() +
                                      ", code: " + to_string(ec.value()) + ", error: " + ec.message()) ||
                reportErrorIf(!fs::exists(stat), protocol::StatusExt::NO_FOLDER,
                              "the directory does not exists: " + dataDir.string());
        try {
            for (fs::directory_entry const& entry : fs::directory_iterator(dataDir)) {
                tuple<string, unsigned int, string> parsed;
                if (FileUtils::parsePartitionedFile(parsed, entry.path().filename().string(), databaseInfo)) {
                    LOGS(_log, LOG_LVL_DEBUG,
                         CONTEXT << " database: " << _databaseName << " file: " << entry.path().filename()
                                 << " table: " << get<0>(parsed) << " chunk: " << get<1>(parsed)
                                 << " ext: " << get<2>(parsed));

                    uint64_t const size = fs::file_size(entry.path(), ec);
                    errorContext = errorContext ||
                                   reportErrorIf(ec.value() != 0, protocol::StatusExt::FILE_SIZE,
                                                 "failed to read file size: " + entry.path().string() +
                                                         ", code: " + to_string(ec.value()) +
                                                         ", error: " + ec.message());

                    time_t mtime = 0;
                    try {
                        mtime = replica::getMTime(entry.path().string());
                    } catch (exception const& ex) {
                        errorContext = errorContext ||
                                       reportErrorIf(true, protocol::StatusExt::FILE_MTIME,
                                                     "failed to read file mtime: " + entry.path().string() +
                                                             ", ex: " + ex.what());
                    }
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
            errorContext = errorContext || reportErrorIf(true, protocol::StatusExt::FOLDER_READ,
                                                         "failed to read the directory: " + dataDir.string() +
                                                                 ", code: " + to_string(ex.code().value()) +
                                                                 ", error: " + ex.code().message());
        }
    }
    if (errorContext.failed) {
        setStatus(lock, protocol::Status::FAILED, errorContext.extendedStatus);
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
                worker(), _databaseName, chunk, util::TimeUtils::now(), chunk2fileInfoCollection[chunk]);
    }
    setStatus(lock, protocol::Status::SUCCESS);
    return true;
}

}  // namespace lsst::qserv::replica
