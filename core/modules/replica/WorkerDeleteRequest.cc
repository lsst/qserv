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
#include "replica/FileUtils.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"


// This macro to appear witin each block which requires thread safety
#define LOCK_DATA_FOLDER \
std::lock_guard<std::mutex> lock(_mtxDataFolderOperations)

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

WorkerDeleteRequest::pointer
WorkerDeleteRequest::create (ServiceProvider   &serviceProvider,
                             const std::string &worker,
                             const std::string &id,
                             int                priority,
                             const std::string &database,
                             unsigned int       chunk) {

    return WorkerDeleteRequest::pointer (
        new WorkerDeleteRequest (serviceProvider,
                                 worker,
                                 id,
                                 priority,
                                 database,
                                 chunk));
}

WorkerDeleteRequest::WorkerDeleteRequest (ServiceProvider   &serviceProvider,
                                          const std::string &worker,
                                          const std::string &id,
                                          int                priority,
                                          const std::string &database,
                                          unsigned int       chunk)
    :   WorkerRequest (serviceProvider,
                       worker,
                       "DELETE",
                       id,
                       priority),

        _database (database),
        _chunk    (chunk),

        // This status will be returned in all contexts
        _replicaInfo (ReplicaInfo::Status::NOT_FOUND,
                      worker,
                      database,
                      chunk,
                      PerformanceUtils::now(),
                      ReplicaInfo::FileInfoCollection{}) {
}

bool
WorkerDeleteRequest::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
        << "  db: "    << database()
        << "  chunk: " << chunk());

    return WorkerRequest::execute();
}


///////////////////////////////////////////////////////////////////
///////////////////// WorkerDeleteRequestPOSIX ////////////////////
///////////////////////////////////////////////////////////////////

WorkerDeleteRequestPOSIX::pointer
WorkerDeleteRequestPOSIX::create (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk) {

    return WorkerDeleteRequestPOSIX::pointer (
        new WorkerDeleteRequestPOSIX (
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk));
}

WorkerDeleteRequestPOSIX::WorkerDeleteRequestPOSIX (
        ServiceProvider   &serviceProvider,
        const std::string &worker,
        const std::string &id,
        int                priority,
        const std::string &database,
        unsigned int       chunk)

    :   WorkerDeleteRequest (
            serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk) {
}

bool
WorkerDeleteRequestPOSIX::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  db: "    << database()
         << "  chunk: " << chunk());

    const WorkerInfo   &workerInfo    = _serviceProvider.config()->workerInfo  (worker());
    const DatabaseInfo &databaseInfo  = _serviceProvider.config()->databaseInfo(database());
    
    const std::vector<std::string> files =
        FileUtils::partitionedFiles (databaseInfo, chunk());

    // The data folder will be locked while performing the operation

    int numFilesDeleted = 0;

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;
    {
        LOCK_DATA_FOLDER;

        const fs::path dataDir = fs::path(workerInfo.dataDir) / database();
        const fs::file_status stat = fs::status(dataDir, ec);
        errorContext = errorContext
            || reportErrorIf (
                    stat.type() == fs::status_error,
                    ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT,
                    "failed to check the status of directory: " + dataDir.string())
            || reportErrorIf (
                    !fs::exists(stat),
                    ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER,
                    "the directory does not exists: " + dataDir.string());

        for (const auto &name: files) {
            const fs::path file = dataDir / fs::path(name);
            if (fs::remove(file, ec)) ++numFilesDeleted;
            errorContext = errorContext
                || reportErrorIf (
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE,
                        "failed to delete file: " + file.string());
        }
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    setStatus(STATUS_SUCCEEDED);
    return true;
}

}}} // namespace lsst::qserv::replica

