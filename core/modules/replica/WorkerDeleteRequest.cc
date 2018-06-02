/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/WorkerDeleteRequest.h"

// System headers

// Third party headers
#include <boost/filesystem.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/FileUtils.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"


namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerDeleteRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

//////////////////////////////////////////////////////////////
///////////////////// WorkerDeleteRequest ////////////////////
//////////////////////////////////////////////////////////////

WorkerDeleteRequest::Ptr WorkerDeleteRequest::create(
                                    ServiceProvider::Ptr const& serviceProvider,
                                    std::string const& worker,
                                    std::string const& id,
                                    int                priority,
                                    std::string const& database,
                                    unsigned int       chunk) {
    return WorkerDeleteRequest::Ptr(
        new WorkerDeleteRequest(serviceProvider,
                                worker,
                                id,
                                priority,
                                database,
                                chunk));
}

WorkerDeleteRequest::WorkerDeleteRequest(ServiceProvider::Ptr const& serviceProvider,
                                         std::string const& worker,
                                         std::string const& id,
                                         int                priority,
                                         std::string const& database,
                                         unsigned int       chunk)
    :   WorkerRequest (serviceProvider,
                       worker,
                       "DELETE",
                       id,
                       priority),
        _database(database),
        _chunk(chunk),
        // This status will be returned in all contexts
        _replicaInfo(ReplicaInfo::Status::NOT_FOUND,
                     worker,
                     database,
                     chunk,
                     PerformanceUtils::now(),
                     ReplicaInfo::FileInfoCollection{}) {
}

void WorkerDeleteRequest::setInfo(proto::ReplicationResponseDelete& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setInfo");

    util::Lock lock(_mtx, context() + "setInfo");

    // Return the performance of the target request

    response.set_allocated_target_performance(performance().info());

    // Note the ownership transfer of an intermediate protobuf object obtained
    // from ReplicaInfo object in the call below. The protobuf runtime will take
    // care of deleting the intermediate object.

    response.set_allocated_replica_info(_replicaInfo.info());

    // Same comment on the ownership transfer applies here

    auto protoRequestPtr = new proto::ReplicationRequestDelete();

    protoRequestPtr->set_priority(priority());
    protoRequestPtr->set_database(database());
    protoRequestPtr->set_chunk(   chunk());

    response.set_allocated_request(protoRequestPtr);
}

bool WorkerDeleteRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
        << "  db: "    << database()
        << "  chunk: " << chunk());

    return WorkerRequest::execute();
}

///////////////////////////////////////////////////////////////////
///////////////////// WorkerDeleteRequestPOSIX ////////////////////
///////////////////////////////////////////////////////////////////

WorkerDeleteRequestPOSIX::Ptr WorkerDeleteRequestPOSIX::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        std::string const& worker,
                                        std::string const& id,
                                        int                priority,
                                        std::string const& database,
                                        unsigned int       chunk) {

    return WorkerDeleteRequestPOSIX::Ptr(
        new WorkerDeleteRequestPOSIX(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk));
}

WorkerDeleteRequestPOSIX::WorkerDeleteRequestPOSIX(
                                ServiceProvider::Ptr const& serviceProvider,
                                std::string const& worker,
                                std::string const& id,
                                int                priority,
                                std::string const& database,
                                unsigned int       chunk)
    :   WorkerDeleteRequest(
            serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk) {
}

bool WorkerDeleteRequestPOSIX::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  db: "    << database()
         << "  chunk: " << chunk());

    util::Lock lock(_mtx, context() + "execute");

    WorkerInfo   const& workerInfo    = _serviceProvider->config()->workerInfo(worker());
    DatabaseInfo const& databaseInfo  = _serviceProvider->config()->databaseInfo(database());

    std::vector<std::string> const files =
        FileUtils::partitionedFiles(databaseInfo, chunk());

    // The data folder will be locked while performing the operation

    int numFilesDeleted = 0;

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;
    {
        util::Lock dataFolderLock(_mtxDataFolderOperations, context() + "execute");

        fs::path        const dataDir = fs::path(workerInfo.dataDir) / database();
        fs::file_status const stat    = fs::status(dataDir, ec);
        errorContext = errorContext
            or reportErrorIf(
                    stat.type() == fs::status_error,
                    ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT,
                    "failed to check the status of directory: " + dataDir.string())
            or reportErrorIf(
                    !fs::exists(stat),
                    ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER,
                    "the directory does not exists: " + dataDir.string());

        for (const auto &name: files) {
            const fs::path file = dataDir / fs::path(name);
            if (fs::remove(file, ec)) ++numFilesDeleted;
            errorContext = errorContext
                or reportErrorIf(
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE,
                        "failed to delete file: " + file.string());
        }
    }
    if (errorContext.failed) {
        setStatus(lock, STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    setStatus(lock, STATUS_SUCCEEDED);
    return true;
}

}}} // namespace lsst::qserv::replica
