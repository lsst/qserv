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
#include "replica/WorkerFindAllRequest.h"

// System headers
#include <map>

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

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindAllRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////////////////////////////////
///////////////////// WorkerFindAllRequest ////////////////////
///////////////////////////////////////////////////////////////

WorkerFindAllRequest::Ptr WorkerFindAllRequest::create(
                                    ServiceProvider::Ptr const& serviceProvider,
                                    std::string const& worker,
                                    std::string const& id,
                                    int                priority,
                                    std::string const& database) {
    return WorkerFindAllRequest::Ptr(
        new WorkerFindAllRequest(
                serviceProvider,
                worker,
                id,
                priority,
                database));
}

WorkerFindAllRequest::WorkerFindAllRequest(
                            ServiceProvider::Ptr const& serviceProvider,
                            std::string const& worker,
                            std::string const& id,
                            int                priority,
                            std::string const& database)
    :   WorkerRequest(
            serviceProvider,
            worker,
            "FIND-ALL",
            id,
            priority),
        _database(database),
        _replicaInfoCollection() {
}

ReplicaInfoCollection const& WorkerFindAllRequest::replicaInfoCollection() const {
    return _replicaInfoCollection;
}

bool WorkerFindAllRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute" << "  database: " << database());

    // Set up the result if the operation is over

    bool completed = WorkerRequest::execute();
    if (completed) {

        // Simulate the request processing by making an arbitrary number of
        // datasets.

        for (unsigned int chunk=0; chunk<8; ++chunk) {
            _replicaInfoCollection.emplace_back(
                ReplicaInfo::COMPLETE,
                _worker,
                database(),
                chunk,
                PerformanceUtils::now(),
                ReplicaInfo::FileInfoCollection());
        }
    }
    return completed;
}

////////////////////////////////////////////////////////////////////
///////////////////// WorkerFindAllRequestPOSIX ////////////////////
////////////////////////////////////////////////////////////////////

WorkerFindAllRequestPOSIX::Ptr WorkerFindAllRequestPOSIX::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        std::string const& worker,
                                        std::string const& id,
                                        int                priority,
                                        std::string const& database) {
    return WorkerFindAllRequestPOSIX::Ptr(
        new WorkerFindAllRequestPOSIX(
                serviceProvider,
                worker,
                id,
                priority,
                database));
}

WorkerFindAllRequestPOSIX::WorkerFindAllRequestPOSIX(
                                ServiceProvider::Ptr const& serviceProvider,
                                std::string const& worker,
                                std::string const& id,
                                int                priority,
                                std::string const& database)
    :   WorkerFindAllRequest(
            serviceProvider,
            worker,
            id,
            priority,
            database) {
}

bool WorkerFindAllRequestPOSIX::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute" << "  database: " << database());

    WorkerInfo   const& workerInfo    = _serviceProvider->config()->workerInfo(worker());
    DatabaseInfo const& databaseInfo  = _serviceProvider->config()->databaseInfo(database());

    // Scan the data directory to find all files which match the expected pattern(s)
    // and group them by their chunk number

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;

    std::map<unsigned int, ReplicaInfo::FileInfoCollection> chunk2fileInfoCollection;
    {
        util::Lock lock(_mtxDataFolderOperations, context() + "execute");

        fs::path        const dataDir = fs::path(workerInfo.dataDir) / database();
        fs::file_status const stat    = fs::status(dataDir, ec);
        errorContext = errorContext
            or reportErrorIf(
                    stat.type() == fs::status_error,
                    ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT,
                    "failed to check the status of directory: " + dataDir.string())
            or reportErrorIf(
                    not fs::exists(stat),
                    ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER,
                    "the directory does not exists: " + dataDir.string());
        try {
            for (fs::directory_entry &entry: fs::directory_iterator(dataDir)) {
                std::tuple<std::string, unsigned int, std::string> parsed;
                if (FileUtils::parsePartitionedFile(
                        parsed,
                        entry.path().filename().string(),
                        databaseInfo)) {

                    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
                        << "  database: " << database()
                        << "  file: "     << entry.path().filename()
                        << "  table: "    << std::get<0>(parsed)
                        << "  chunk: "    << std::get<1>(parsed)
                        << "  ext: "      << std::get<2>(parsed));

                    uint64_t const size = fs::file_size(entry.path(), ec);
                    errorContext = errorContext
                        or reportErrorIf(
                                ec,
                                ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE,
                                "failed to read file size: " + entry.path().string());

                    std::time_t const mtime = fs::last_write_time(entry.path(), ec);
                    errorContext = errorContext
                        or reportErrorIf(
                                ec,
                                ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME,
                                "failed to read file mtime: " + entry.path().string());

                    unsigned const chunk = std::get<1>(parsed);

                    chunk2fileInfoCollection[chunk].emplace_back(
                        ReplicaInfo::FileInfo({
                            entry.path().filename().string(),
                            size,
                            mtime,
                            "",     /* cs is never computed for this type of requests */
                            0,      /* beginTransferTime */
                            0,      /* endTransferTime */
                            size    /* inSize */
                        })
                    );
                }
            }
        } catch (fs::filesystem_error const& ex) {
            errorContext = errorContext
                or reportErrorIf(
                        true,
                        ExtendedCompletionStatus::EXT_STATUS_FOLDER_READ,
                        "failed to read the directory: " + dataDir.string() +
                        ", error: " + std::string(ex.what()));
        }
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // Analyze results to see which chunks are complete using chunk 0 as an example
    // of the total number of files which are normally associated with each chunk.

    size_t const numFilesPerChunkRequired =
        FileUtils::partitionedFiles(databaseInfo, 0).size();

    for (auto&& entry: chunk2fileInfoCollection) {
        unsigned int const chunk    = entry.first;
        size_t       const numFiles = entry.second.size();
        _replicaInfoCollection.emplace_back(
                numFiles < numFilesPerChunkRequired ? ReplicaInfo::INCOMPLETE : ReplicaInfo::COMPLETE,
                worker(),
                database(),
                chunk,
                PerformanceUtils::now(),
                chunk2fileInfoCollection[chunk]);
    }
    setStatus(STATUS_SUCCEEDED);
    return true;
}

}}} // namespace lsst::qserv::replica
