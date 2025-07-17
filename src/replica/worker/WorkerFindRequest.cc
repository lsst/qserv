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
#include "replica/worker/WorkerFindRequest.h"

// System headers

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/FileUtils.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindRequest");

}  // namespace

namespace lsst::qserv::replica {

WorkerFindRequest::Ptr WorkerFindRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                                 string const& worker, string const& id, int priority,
                                                 ExpirationCallbackType const& onExpired,
                                                 unsigned int requestExpirationIvalSec,
                                                 ProtocolRequestFind const& request) {
    auto ptr = WorkerFindRequest::Ptr(new WorkerFindRequest(serviceProvider, worker, id, priority, onExpired,
                                                            requestExpirationIvalSec, request));
    ptr->init();
    return ptr;
}

WorkerFindRequest::WorkerFindRequest(ServiceProvider::Ptr const& serviceProvider, string const& worker,
                                     string const& id, int priority, ExpirationCallbackType const& onExpired,
                                     unsigned int requestExpirationIvalSec,
                                     ProtocolRequestFind const& request)
        : WorkerRequest(serviceProvider, worker, "FIND", id, priority, onExpired, requestExpirationIvalSec),
          _request(request) {
    serviceProvider->config()->assertDatabaseIsValid(request.database());
}

void WorkerFindRequest::setInfo(ProtocolResponseFind& response) const {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    replica::Lock lock(_mtx, context(__func__));
    response.set_allocated_target_performance(performance().info().release());
    response.set_allocated_replica_info(_replicaInfo.info().release());
    *(response.mutable_request()) = _request;
}

bool WorkerFindRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  database: " << database() << "  chunk: " << chunk());

    replica::Lock lock(_mtx, context(__func__));
    checkIfCancelling(lock, __func__);

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
    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code ec;
    if (not computeCheckSum() or not _csComputeEnginePtr) {
        auto const config = _serviceProvider->config();
        DatabaseInfo const databaseInfo = config->databaseInfo(database());

        // Check if the data directory exists and it can be read

        replica::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));
        fs::path const dataDir =
                fs::path(config->get<string>("worker", "data-dir")) / database::mysql::obj2fs(database());
        fs::file_status const stat = fs::status(dataDir, ec);
        errorContext = errorContext or
                       reportErrorIf(stat.type() == fs::status_error, ProtocolStatusExt::FOLDER_STAT,
                                     "failed to check the status of directory: " + dataDir.string()) or
                       reportErrorIf(not fs::exists(stat), ProtocolStatusExt::NO_FOLDER,
                                     "the directory does not exists: " + dataDir.string());
        if (errorContext.failed) {
            setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
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

        for (auto&& file : FileUtils::partitionedFiles(databaseInfo, chunk())) {
            fs::path const path = dataDir / file;
            fs::file_status const stat = fs::status(path, ec);
            errorContext = errorContext or
                           reportErrorIf(stat.type() == fs::status_error, ProtocolStatusExt::FILE_STAT,
                                         "failed to check the status of file: " + path.string());
            if (fs::exists(stat)) {
                if (not computeCheckSum()) {
                    // Get file size & mtime right away

                    uint64_t const size = fs::file_size(path, ec);
                    errorContext =
                            errorContext or reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_SIZE,
                                                          "failed to read file size: " + path.string());
                    const time_t mtime = fs::last_write_time(path, ec);
                    errorContext =
                            errorContext or reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_MTIME,
                                                          "failed to read file mtime: " + path.string());
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
            setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
            return true;
        }

        // If that's so then finalize the operation right away
        if (not computeCheckSum()) {
            ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
            if (fileInfoCollection.size())
                status =
                        FileUtils::partitionedFiles(databaseInfo, chunk()).size() == fileInfoCollection.size()
                                ? ReplicaInfo::Status::COMPLETE
                                : ReplicaInfo::Status::INCOMPLETE;

            // Fill in the info on the chunk before finishing the operation
            _replicaInfo = ReplicaInfo(status, worker(), database(), chunk(), util::TimeUtils::now(),
                                       fileInfoCollection);
            setStatus(lock, ProtocolStatus::SUCCESS);
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
                time_t const mtime = fs::last_write_time(path, ec);
                errorContext = errorContext or reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_MTIME,
                                                             "failed to read file mtime: " + path.string());
                fileInfoCollection.emplace_back(ReplicaInfo::FileInfo({
                        path.filename().string(), size, mtime, to_string(_csComputeEnginePtr->cs(file)),
                        0,   /* beginTransferTime */
                        0,   /* endTransferTime */
                        size /* inSize */
                }));
            }
            if (errorContext.failed) {
                setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
                return true;
            }

            // Fnalize the operation
            DatabaseInfo const databaseInfo = _serviceProvider->config()->databaseInfo(database());
            ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
            if (fileInfoCollection.size())
                status = FileUtils::partitionedFiles(databaseInfo, chunk()).size() == fileNames.size()
                                 ? ReplicaInfo::Status::COMPLETE
                                 : ReplicaInfo::Status::INCOMPLETE;

            // Fill in the info on the chunk before finishing the operation
            _replicaInfo = ReplicaInfo(status, worker(), database(), chunk(), util::TimeUtils::now(),
                                       fileInfoCollection);
            setStatus(lock, ProtocolStatus::SUCCESS);
        }
    } catch (exception const& ex) {
        WorkerRequest::ErrorContext errorContext;
        errorContext = errorContext or reportErrorIf(true, ProtocolStatusExt::FILE_READ, ex.what());
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
    }

    // If done (either way) then get rid of the engine right away because
    // it may still have allocated buffers
    if (finished) _csComputeEnginePtr.reset();
    return finished;
}

}  // namespace lsst::qserv::replica
