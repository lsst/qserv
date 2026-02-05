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
#include "replica/worker/WorkerDeleteRequest.h"

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerDeleteRequest");

}  // namespace

namespace lsst::qserv::replica {

WorkerDeleteRequest::Ptr WorkerDeleteRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                                     string const& worker, string const& id, int priority,
                                                     ExpirationCallbackType const& onExpired,
                                                     unsigned int requestExpirationIvalSec,
                                                     ProtocolRequestDelete const& request) {
    auto ptr = WorkerDeleteRequest::Ptr(new WorkerDeleteRequest(
            serviceProvider, worker, id, priority, onExpired, requestExpirationIvalSec, request));
    ptr->init();
    return ptr;
}

WorkerDeleteRequest::WorkerDeleteRequest(ServiceProvider::Ptr const& serviceProvider, string const& worker,
                                         string const& id, int priority,
                                         ExpirationCallbackType const& onExpired,
                                         unsigned int requestExpirationIvalSec,
                                         ProtocolRequestDelete const& request)
        : WorkerRequest(serviceProvider, worker, "DELETE", id, priority, onExpired, requestExpirationIvalSec),
          _request(request),
          // This status will be returned in all contexts
          _replicaInfo(ReplicaInfo::Status::NOT_FOUND, worker, request.database(), request.chunk(),
                       util::TimeUtils::now(), ReplicaInfo::FileInfoCollection{}) {}

void WorkerDeleteRequest::setInfo(ProtocolResponseDelete& response) const {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    replica::Lock lock(_mtx, context(__func__));
    response.set_allocated_target_performance(performance().info().release());
    response.set_allocated_replica_info(_replicaInfo.info().release());
    *(response.mutable_request()) = _request;
}

bool WorkerDeleteRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__) << "  db: " << database() << "  chunk: " << chunk());

    // The method will throw ConfigUnknownDatabase if the database is invalid.
    DatabaseInfo const databaseInfo = _serviceProvider->config()->databaseInfo(database());

    replica::Lock lock(_mtx, context(__func__));
    checkIfCancelling(lock, __func__);

    vector<string> const files = FileUtils::partitionedFiles(databaseInfo, chunk());

    // The data folder will be locked while performing the operation
    int numFilesDeleted = 0;
    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code ec;
    {
        replica::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));
        fs::path const dataDir = fs::path(_serviceProvider->config()->get<string>("worker", "data-dir")) /
                                 database::mysql::obj2fs(database());
        fs::file_status const stat = fs::status(dataDir, ec);
        errorContext = errorContext or
                       reportErrorIf(stat.type() == fs::status_error, ProtocolStatusExt::FOLDER_STAT,
                                     "failed to check the status of directory: " + dataDir.string()) or
                       reportErrorIf(!fs::exists(stat), ProtocolStatusExt::NO_FOLDER,
                                     "the directory does not exists: " + dataDir.string());
        for (const auto& name : files) {
            const fs::path file = dataDir / fs::path(name);
            if (fs::remove(file, ec)) ++numFilesDeleted;
            errorContext = errorContext or reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_DELETE,
                                                         "failed to delete file: " + file.string());
        }
    }
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return true;
    }
    setStatus(lock, ProtocolStatus::SUCCESS);
    return true;
}

}  // namespace lsst::qserv::replica
