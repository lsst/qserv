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
#include "replica/worker/WorkerFindReplicaHttpRequest.h"

// System headers
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <system_error>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/config/ConfigDatabase.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/FileUtils.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindReplicaHttpRequest");

}  // namespace

#define _CONTEXT context("WorkerFindReplicaHttpRequest::", __func__)
#define _SET_STATUS_FAILED(extendedStatus_, error_)       \
    LOGS(_log, LOG_LVL_ERROR, _CONTEXT << " " << error_); \
    setStatus(lock, protocol::Status::FAILED, extendedStatus_, error_);

namespace lsst::qserv::replica {

shared_ptr<WorkerFindReplicaHttpRequest> WorkerFindReplicaHttpRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, protocol::RequestParams const& params,
        ExpirationCallbackType const& onExpired) {
    auto ptr = shared_ptr<WorkerFindReplicaHttpRequest>(
            new WorkerFindReplicaHttpRequest(serviceProvider, worker, hdr, params, onExpired));
    ptr->init();
    return ptr;
}

WorkerFindReplicaHttpRequest::WorkerFindReplicaHttpRequest(shared_ptr<ServiceProvider> const& serviceProvider,
                                                           string const& worker,
                                                           protocol::QueuedRequestHdr const& hdr,
                                                           protocol::RequestParams const& params,
                                                           ExpirationCallbackType const& onExpired)
        : WorkerHttpRequest(serviceProvider, worker, "FIND", hdr, params, onExpired),
          _databaseName(params.requiredString("database")),
          _chunkNumber(params.requiredUInt32("chunk")),
          _computeCheckSum(params.requiredBool("compute_cs")) {}

json WorkerFindReplicaHttpRequest::getResult() const {
    return json::object({{"replica_info", _replicaInfo.toJson()}});
}

bool WorkerFindReplicaHttpRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, _CONTEXT << " database: " << _databaseName << " chunk: " << _chunkNumber);

    replica::Lock lock(mtx, _CONTEXT);
    checkIfCancelling(lock, _CONTEXT);

    // The method will throw ConfigUnknownDatabase if the database is invalid.
    DatabaseInfo const databaseInfo = serviceProvider()->config()->databaseInfo(_databaseName);

    // There are two modes of operation of the code which would depend
    // on a presence (or a lack of that) to calculate control/check sums
    // for the found files.
    //
    // - if the control/check sum is NOT requested then the request will
    //   be executed immediately within this call.
    //
    // - otherwise the incremental approach will be used (which will require
    //   setting up the incremental engine if this is the first call to the method)
    //
    // Both methods are combined within the same code block to avoid
    // code duplication.

    std::error_code ec;
    uint64_t const beginTransferTime = 0;  // unsed but required for constructing the FileInfo object
    uint64_t const endTransferTime = 0;    // unsed but required for constructing the FileInfo object

    if (!_computeCheckSum || (_csComputeEnginePtr == nullptr)) {
        // Check if the data directory exists and it can be read
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

        // For each file associated with the chunk check if the file is present in
        // the data directory.
        //
        // - not finding a file is not a failure for this operation. Just reporting
        //   those files which are present.
        // - assume the request failure for any file system operation failure
        // - assume the successful completion otherwise and adjust the replica
        //   information record accordingly, depending on the findings.

        ReplicaInfo::FileInfoCollection fileInfoCollection;
        vector<string> filesForCsProcessing;

        for (auto const& file : FileUtils::partitionedFiles(databaseInfo, _chunkNumber)) {
            fs::path const path = dataDir / file;
            fs::file_status const stat = fs::status(path, ec);
            if (stat.type() == fs::file_type::none) {
                _SET_STATUS_FAILED(protocol::StatusExt::FILE_STAT,
                                   "failed to check the status of file: " + path.string() +
                                           ", code: " + to_string(ec.value()) + ", error: " + ec.message());
                return true;
            }
            if (fs::exists(stat)) {
                if (_computeCheckSum) {
                    // Register this file for the incremental processing.
                    // Note that file size and its mtime will be obtained as part of the incremental
                    // processing.
                    filesForCsProcessing.push_back(path.string());
                } else {
                    // Otherwise extract the file information immediately.
                    uint64_t const size = fs::file_size(path, ec);
                    if (ec.value() != 0) {
                        _SET_STATUS_FAILED(protocol::StatusExt::FILE_SIZE,
                                           "failed to read file size: " + path.string() + ", code: " +
                                                   to_string(ec.value()) + ", error: " + ec.message());
                        return true;
                    }
                    time_t mtime = 0;
                    try {
                        mtime = replica::getMTime(path.string());
                    } catch (exception const& ex) {
                        _SET_STATUS_FAILED(
                                protocol::StatusExt::FILE_MTIME,
                                "failed to read file mtime: " + path.string() + ", ex: " + ex.what());
                        return true;
                    }
                    string const emptyCs;          // the sum is never computed for this type of requests
                    uint64_t const inSize = size;  // the file size (bytes) at the remote worker
                    fileInfoCollection.emplace_back(file, size, mtime, emptyCs, beginTransferTime,
                                                    endTransferTime, inSize);
                }
            }
        }
        if (_computeCheckSum) {
            // Proceed to the incremental calculation of control/check sums for the found files.
            _csComputeEnginePtr.reset(new MultiFileCsComputeEngine(filesForCsProcessing));
        } else {
            // Otherwise finish processing the request.
            ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
            if (fileInfoCollection.size())
                status = FileUtils::partitionedFiles(databaseInfo, _chunkNumber).size() ==
                                         fileInfoCollection.size()
                                 ? ReplicaInfo::Status::COMPLETE
                                 : ReplicaInfo::Status::INCOMPLETE;

            // Fill in the info on the chunk before finishing the operation
            _replicaInfo = ReplicaInfo(status, worker(), _databaseName, _chunkNumber, util::TimeUtils::now(),
                                       fileInfoCollection);
            setStatus(lock, protocol::Status::SUCCESS);
            return true;
        }
    }

    // Next (or the first) iteration in the incremental calculation
    try {
        bool const finished = _csComputeEnginePtr->execute();
        if (!finished) return false;

        // Extract statistics
        ReplicaInfo::FileInfoCollection fileInfoCollection;
        auto const fileNames = _csComputeEnginePtr->fileNames();
        for (auto const& file : fileNames) {
            const fs::path path(file);
            uint64_t const size = _csComputeEnginePtr->bytes(file);
            time_t mtime = 0;
            try {
                mtime = replica::getMTime(path.string());
            } catch (exception const& ex) {
                _SET_STATUS_FAILED(protocol::StatusExt::FILE_MTIME,
                                   "failed to read file mtime: " + path.string() + ", ex: " + ex.what());
                _csComputeEnginePtr.reset();
                return true;
            }
            uint64_t const inSize = size;  // the file size (bytes) at the remote worker
            fileInfoCollection.emplace_back(path.filename().string(), size, mtime,
                                            to_string(_csComputeEnginePtr->cs(file)), beginTransferTime,
                                            endTransferTime, inSize);
        }

        // Finalize the operation
        ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
        if (fileInfoCollection.size())
            status = FileUtils::partitionedFiles(databaseInfo, _chunkNumber).size() == fileNames.size()
                             ? ReplicaInfo::Status::COMPLETE
                             : ReplicaInfo::Status::INCOMPLETE;

        // Fill in the info on the chunk before finishing the operation
        _replicaInfo = ReplicaInfo(status, worker(), _databaseName, _chunkNumber, util::TimeUtils::now(),
                                   fileInfoCollection);
        setStatus(lock, protocol::Status::SUCCESS);
    } catch (exception const& ex) {
        _SET_STATUS_FAILED(
                protocol::StatusExt::FILE_READ,
                string(ex.what()) + ", code: " + to_string(ec.value()) + ", error: " + ec.message());
    }
    _csComputeEnginePtr.reset();
    return true;
}

}  // namespace lsst::qserv::replica
