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

#define CONTEXT context("WorkerFindReplicaHttpRequest", __func__)

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindReplicaHttpRequest");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<WorkerFindReplicaHttpRequest> WorkerFindReplicaHttpRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, json const& req, ExpirationCallbackType const& onExpired) {
    auto ptr = shared_ptr<WorkerFindReplicaHttpRequest>(
            new WorkerFindReplicaHttpRequest(serviceProvider, worker, hdr, req, onExpired));
    ptr->init();
    return ptr;
}

WorkerFindReplicaHttpRequest::WorkerFindReplicaHttpRequest(shared_ptr<ServiceProvider> const& serviceProvider,
                                                           string const& worker,
                                                           protocol::QueuedRequestHdr const& hdr,
                                                           json const& req,
                                                           ExpirationCallbackType const& onExpired)
        : WorkerHttpRequest(serviceProvider, worker, "FIND", hdr, req, onExpired),
          _databaseName(req.at("database")),
          _chunkNumber(req.at("chunk")),
          _computeCheckSum(req.at("compute_cs")) {}

void WorkerFindReplicaHttpRequest::getResult(json& result) const {
    result["replica_info"] = _replicaInfo.toJson();
}

bool WorkerFindReplicaHttpRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, CONTEXT << " database: " << _databaseName << " chunk: " << _chunkNumber);

    replica::Lock lock(_mtx, CONTEXT);
    checkIfCancelling(lock, CONTEXT);

    // The method will throw ConfigUnknownDatabase if the database is invalid.
    DatabaseInfo const databaseInfo = _serviceProvider->config()->databaseInfo(_databaseName);

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
    WorkerHttpRequest::ErrorContext errorContext;
    std::error_code ec;

    if (!_computeCheckSum || (_csComputeEnginePtr == nullptr)) {
        // Check if the data directory exists and it can be read
        replica::Lock dataFolderLock(_mtxDataFolderOperations, CONTEXT);
        fs::path const dataDir = fs::path(_serviceProvider->config()->get<string>("worker", "data-dir")) /
                                 database::mysql::obj2fs(_databaseName);
        fs::file_status const stat = fs::status(dataDir, ec);
        errorContext =
                errorContext ||
                reportErrorIf(stat.type() == fs::file_type::none, protocol::StatusExt::FOLDER_STAT,
                              "failed to check the status of directory: " + dataDir.string() +
                                      ", code: " + to_string(ec.value()) + ", error: " + ec.message()) ||
                reportErrorIf(!fs::exists(stat), protocol::StatusExt::NO_FOLDER,
                              "the directory does not exists: " + dataDir.string());
        if (errorContext.failed) {
            setStatus(lock, protocol::Status::FAILED, errorContext.extendedStatus);
            return true;
        }

        // For each file associated with the chunk check if the file is present in
        // the data directory.
        //
        // - not finding a file is not a failure for this operation. Just reporting
        //   those files which are present.
        //
        // - assume the request failure for any file system operation failure
        //
        // - assume the successful completion otherwise and adjust the replica
        //   information record accordingly, depending on the findings.
        ReplicaInfo::FileInfoCollection
                fileInfoCollection;  // file info if not using the incremental processing
        vector<string> files;        // file paths registered for the incremental processing

        for (auto&& file : FileUtils::partitionedFiles(databaseInfo, _chunkNumber)) {
            fs::path const path = dataDir / file;
            fs::file_status const stat = fs::status(path, ec);
            errorContext = errorContext ||
                           reportErrorIf(stat.type() == fs::file_type::none, protocol::StatusExt::FILE_STAT,
                                         "failed to check the status of file: " + path.string() + ", code: " +
                                                 to_string(ec.value()) + ", error: " + ec.message());
            if (fs::exists(stat)) {
                if (!_computeCheckSum) {
                    // Get file size & mtime right away
                    uint64_t const size = fs::file_size(path, ec);
                    errorContext = errorContext ||
                                   reportErrorIf(ec.value() != 0, protocol::StatusExt::FILE_SIZE,
                                                 "failed to read file size: " + path.string() + ", code: " +
                                                         to_string(ec.value()) + ", error: " + ec.message());
                    time_t mtime = 0;
                    try {
                        mtime = replica::getMTime(path.string());
                    } catch (exception const& ex) {
                        errorContext =
                                errorContext || reportErrorIf(true, protocol::StatusExt::FILE_MTIME,
                                                              "failed to read file mtime: " + path.string() +
                                                                      ", ex: " + ex.what());
                    }
                    fileInfoCollection.emplace_back(ReplicaInfo::FileInfo({
                            file, size, mtime, "", /* cs */
                            0,                     /* beginTransferTime */
                            0,                     /* endTransferTime */
                            size                   /* inSize */
                    }));
                } else {
                    // Register this file for the incremental processing
                    files.push_back(path.string());
                }
            }
        }
        if (errorContext.failed) {
            setStatus(lock, protocol::Status::FAILED, errorContext.extendedStatus);
            return true;
        }

        // If that's so then finalize the operation right away
        if (!_computeCheckSum) {
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

        // Otherwise proceed with the incremental approach
        _csComputeEnginePtr.reset(new MultiFileCsComputeEngine(files));
    }

    // Next (or the first) iteration in the incremental approach
    bool finished = true;
    try {
        finished = _csComputeEnginePtr->execute();
        if (finished) {
            // Extract statistics
            ReplicaInfo::FileInfoCollection fileInfoCollection;
            auto const fileNames = _csComputeEnginePtr->fileNames();
            for (auto&& file : fileNames) {
                const fs::path path(file);
                uint64_t const size = _csComputeEnginePtr->bytes(file);
                time_t mtime = 0;
                try {
                    mtime = replica::getMTime(path.string());
                } catch (exception const& ex) {
                    errorContext =
                            errorContext || reportErrorIf(true, protocol::StatusExt::FILE_MTIME,
                                                          "failed to read file mtime: " + path.string() +
                                                                  ", ex: " + ex.what());
                }
                fileInfoCollection.emplace_back(ReplicaInfo::FileInfo({
                        path.filename().string(), size, mtime, to_string(_csComputeEnginePtr->cs(file)),
                        0,   /* beginTransferTime */
                        0,   /* endTransferTime */
                        size /* inSize */
                }));
            }
            if (errorContext.failed) {
                setStatus(lock, protocol::Status::FAILED, errorContext.extendedStatus);
                return true;
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
        }
    } catch (exception const& ex) {
        WorkerHttpRequest::ErrorContext errorContext;
        errorContext = errorContext || reportErrorIf(true, protocol::StatusExt::FILE_READ,
                                                     string(ex.what()) + ", code: " + to_string(ec.value()) +
                                                             ", error: " + ec.message());
        setStatus(lock, protocol::Status::FAILED, errorContext.extendedStatus);
    }

    // If done (either way) then get rid of the engine right away because
    // it may still have allocated buffers
    if (finished) _csComputeEnginePtr.reset();
    return finished;
}

}  // namespace lsst::qserv::replica
