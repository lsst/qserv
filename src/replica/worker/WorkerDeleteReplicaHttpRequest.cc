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
#include "replica/worker/WorkerDeleteReplicaHttpRequest.h"

// System headers
#include <filesystem>
#include <system_error>

// Third party headers

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/config/ConfigDatabase.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/proto/Protocol.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/FileUtils.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

#define CONTEXT context("WorkerDeleteReplicaHttpRequest", __func__)

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerDeleteReplicaHttpRequest");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<WorkerDeleteReplicaHttpRequest> WorkerDeleteReplicaHttpRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, json const& req, ExpirationCallbackType const& onExpired) {
    auto ptr = shared_ptr<WorkerDeleteReplicaHttpRequest>(
            new WorkerDeleteReplicaHttpRequest(serviceProvider, worker, hdr, req, onExpired));
    ptr->init();
    return ptr;
}

WorkerDeleteReplicaHttpRequest::WorkerDeleteReplicaHttpRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, json const& req, ExpirationCallbackType const& onExpired)
        : WorkerHttpRequest(serviceProvider, worker, "DELETE", hdr, req, onExpired),
          _databaseName(req.at("database")),
          _chunkNumber(req.at("chunk")),
          // This status will be returned in all contexts
          _replicaInfo(ReplicaInfo::Status::NOT_FOUND, worker, _databaseName, _chunkNumber,
                       util::TimeUtils::now(), ReplicaInfo::FileInfoCollection{}) {}

void WorkerDeleteReplicaHttpRequest::getResult(json& result) const {
    result["replica_info"] = _replicaInfo.toJson();
}

bool WorkerDeleteReplicaHttpRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, CONTEXT << " db: " << _databaseName << " chunk: " << _chunkNumber);

    replica::Lock lock(_mtx, CONTEXT);
    checkIfCancelling(lock, CONTEXT);

    // The method will throw ConfigUnknownDatabase if the database is invalid.
    DatabaseInfo const databaseInfo = serviceProvider()->config()->databaseInfo(_databaseName);

    // The collection of short names of files to be deleted.
    vector<string> const files = FileUtils::partitionedFiles(databaseInfo, _chunkNumber);

    // The data folder will be locked while performing the operation
    int numFilesDeleted = 0;
    WorkerHttpRequest::ErrorContext errorContext;
    std::error_code ec;
    {
        replica::Lock dataFolderLock(_mtxDataFolderOperations, CONTEXT);
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
        for (auto const& name : files) {
            fs::path const file = dataDir / fs::path(name);
            if (fs::remove(file, ec)) ++numFilesDeleted;
            errorContext = errorContext || reportErrorIf(ec.value() != 0, protocol::StatusExt::FILE_DELETE,
                                                         "failed to delete file: " + file.string() +
                                                                 ", code: " + to_string(ec.value()) +
                                                                 ", error: " + ec.message());
        }
    }
    if (errorContext.failed) {
        setStatus(lock, protocol::Status::FAILED, errorContext.extendedStatus);
        return true;
    }
    setStatus(lock, protocol::Status::SUCCESS);
    return true;
}

}  // namespace lsst::qserv::replica
