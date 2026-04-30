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

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindAllReplicasHttpRequest");

}  // namespace

#define _CONTEXT context("WorkerFindAllReplicasHttpRequest::", __func__)
#define _SET_STATUS_FAILED(extendedStatus_, error_)       \
    LOGS(_log, LOG_LVL_ERROR, _CONTEXT << " " << error_); \
    setStatus(lock, protocol::Status::FAILED, extendedStatus_, error_);

namespace lsst::qserv::replica {

shared_ptr<WorkerFindAllReplicasHttpRequest> WorkerFindAllReplicasHttpRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, protocol::RequestParams const& params,
        ExpirationCallbackType const& onExpired) {
    auto ptr = shared_ptr<WorkerFindAllReplicasHttpRequest>(
            new WorkerFindAllReplicasHttpRequest(serviceProvider, worker, hdr, params, onExpired));
    ptr->init();
    return ptr;
}

WorkerFindAllReplicasHttpRequest::WorkerFindAllReplicasHttpRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, protocol::RequestParams const& params,
        ExpirationCallbackType const& onExpired)
        : WorkerHttpRequest(serviceProvider, worker, "FIND-ALL", hdr, params, onExpired),
          _databaseName(params.requiredString("database")) {}

json WorkerFindAllReplicasHttpRequest::getResult() const {
    json result = json::object({{"replica_info_many", json::array()}});
    for (auto const& replicaInfo : _replicaInfoCollection) {
        result["replica_info_many"].push_back(replicaInfo.toJson());
    }
    return result;
}

bool WorkerFindAllReplicasHttpRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, _CONTEXT << " database: " << _databaseName);

    replica::Lock lock(mtx, _CONTEXT);
    checkIfCancelling(lock, _CONTEXT);

    // The method will throw ConfigUnknownDatabase if the database is invalid.
    DatabaseInfo const databaseInfo = serviceProvider()->config()->databaseInfo(_databaseName);

    // Scan the data directory to find all files which match the expected pattern(s)
    // and group them by their chunk number
    std::error_code ec;
    map<unsigned int, ReplicaInfo::FileInfoCollection> chunk2fileInfoCollection;
    {
        replica::Lock dataFolderLock(mtxDataFolderOperations, _CONTEXT);
        fs::path const dataDir = fs::path(serviceProvider()->config()->get<string>("worker", "data-dir")) /
                                 database::mysql::obj2fs(_databaseName);
        fs::file_status const stat = fs::status(dataDir, ec);
        if (stat.type() == fs::file_type::none) {
            _SET_STATUS_FAILED(protocol::StatusExt::FOLDER_STAT,
                               "failed to check the status of directory: " + dataDir.string() +
                                       ", code: " + to_string(ec.value()) + ", error: " + ec.message());
            return true;
        }
        if (!fs::exists(stat)) {
            _SET_STATUS_FAILED(protocol::StatusExt::NO_FOLDER,
                               "the directory does not exists: " + dataDir.string());
            return true;
        }
        try {
            uint64_t const beginTransferTime = 0;  // unused but required for constructing the FileInfo object
            uint64_t const endTransferTime = 0;    // unused but required for constructing the FileInfo object
            string const emptyCs;                  // the sum is never computed for this type of requests

            for (fs::directory_entry const& entry : fs::directory_iterator(dataDir)) {
                tuple<string, unsigned int, string> parsed;
                if (FileUtils::parsePartitionedFile(parsed, entry.path().filename().string(), databaseInfo)) {
                    uint64_t const size = fs::file_size(entry.path(), ec);
                    if (ec.value() != 0) {
                        _SET_STATUS_FAILED(protocol::StatusExt::FILE_SIZE,
                                           "failed to read file size: " + entry.path().string() + ", code: " +
                                                   to_string(ec.value()) + ", error: " + ec.message());
                        return true;
                    }
                    time_t mtime = 0;
                    try {
                        mtime = replica::getMTime(entry.path().string());
                    } catch (exception const& ex) {
                        _SET_STATUS_FAILED(
                                protocol::StatusExt::FILE_MTIME,
                                "failed to read file mtime: " + entry.path().string() + ", ex: " + ex.what());
                        return true;
                    }
                    unsigned const chunk = get<1>(parsed);
                    uint64_t const inSize = size;  // the file size (bytes) at the remote worker
                    chunk2fileInfoCollection[chunk].emplace_back(entry.path().filename().string(), size,
                                                                 mtime, emptyCs, beginTransferTime,
                                                                 endTransferTime, inSize);
                }
            }
        } catch (fs::filesystem_error const& ex) {
            _SET_STATUS_FAILED(protocol::StatusExt::FOLDER_READ,
                               "failed to read the directory: " + dataDir.string() + ", code: " +
                                       to_string(ex.code().value()) + ", error: " + ex.code().message());
            return true;
        }
    }

    // Analyze results to see which chunks are complete using chunk 0 as an example
    // of the total number of files which are normally associated with each chunk.
    size_t const numFilesPerChunkRequired = FileUtils::partitionedFiles(databaseInfo, 0).size();
    for (auto const& entry : chunk2fileInfoCollection) {
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
