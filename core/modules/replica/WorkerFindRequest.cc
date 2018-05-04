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
#include "replica/WorkerFindRequest.h"

// System headers

// Third party headers
#include <boost/filesystem.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/FileUtils.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"

// This macro to appear witin each block which requires thread safety
#define LOCK_DATA_FOLDER std::lock_guard<std::mutex> lock(_mtxDataFolderOperations)

namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerFindRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

////////////////////////////////////////////////////////////
///////////////////// WorkerFindRequest ////////////////////
////////////////////////////////////////////////////////////

WorkerFindRequest::Ptr WorkerFindRequest::create(
                                ServiceProvider::Ptr const& serviceProvider,
                                std::string const& worker,
                                std::string const& id,
                                int                priority,
                                std::string const& database,
                                unsigned int       chunk,
                                bool               computeCheckSum) {
    return WorkerFindRequest::Ptr(
        new WorkerFindRequest(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                computeCheckSum));
}

WorkerFindRequest::WorkerFindRequest(
                        ServiceProvider::Ptr const& serviceProvider,
                        std::string const& worker,
                        std::string const& id,
                        int                priority,
                        std::string const& database,
                        unsigned int       chunk,
                        bool               computeCheckSum)
    :   WorkerRequest(
            serviceProvider,
            worker,
            "FIND",
            id,
            priority),
        _database(database),
        _chunk(chunk),
        _computeCheckSum(computeCheckSum),
        _replicaInfo() {

    serviceProvider->assertDatabaseIsValid(database);
}

bool WorkerFindRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  database: " << database()
         << "  chunk: "    << chunk());

    // Set up the result if the operation is over

    bool completed = WorkerRequest::execute();
    if (completed) {
        _replicaInfo = ReplicaInfo(ReplicaInfo::COMPLETE,
                                   worker(),
                                   database(),
                                   chunk(),
                                   PerformanceUtils::now(),
                                   ReplicaInfo::FileInfoCollection());
    }
    return completed;
}

/////////////////////////////////////////////////////////////////
///////////////////// WorkerFindRequestPOSIX ////////////////////
/////////////////////////////////////////////////////////////////

WorkerFindRequestPOSIX::Ptr WorkerFindRequestPOSIX::create(
                                    ServiceProvider::Ptr const& serviceProvider,
                                    std::string const& worker,
                                    std::string const& id,
                                    int                priority,
                                    std::string const& database,
                                    unsigned int       chunk,
                                    bool               computeCheckSum) {
    return WorkerFindRequestPOSIX::Ptr(
        new WorkerFindRequestPOSIX(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                computeCheckSum));
}

WorkerFindRequestPOSIX::WorkerFindRequestPOSIX(
                            ServiceProvider::Ptr const& serviceProvider,
                            std::string const& worker,
                            std::string const& id,
                            int                priority,
                            std::string const& database,
                            unsigned int       chunk,
                            bool               computeCheckSum)
    :   WorkerFindRequest(
            serviceProvider,
            worker,
            id,
            priority,
            database,
            chunk,
            computeCheckSum) {
}

bool WorkerFindRequestPOSIX::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  database: " << database()
         << "  chunk: "    << chunk());

    // Abort the operation right away if that's the case

    if (_status == STATUS_IS_CANCELLING) {
        setStatus(STATUS_CANCELLED);
        throw WorkerRequestCancelled();
    }

    // There are two modes of operation of the code which would depend
    // on a presence (or a lack of that) to calculate control/check sums
    // for the found files.
    //
    // - if the control/check sum is NOT requested then the request will
    //   be executed immediattely within this call.
    //
    // - otherwise the incremental approach will be used (which will require
    //   setting up the incremental engine if this is the first call to the method)
    //
    // Both methods are combined witghin the same code block to avoid
    // code duplication.

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;

    if (not _computeCheckSum or not _csComputeEnginePtr) {

        WorkerInfo   const& workerInfo   = _serviceProvider->config()->workerInfo(worker());
        DatabaseInfo const& databaseInfo = _serviceProvider->config()->databaseInfo(database());

        // Check if the data directory exists and it can be read

        LOCK_DATA_FOLDER;

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

        if (errorContext.failed) {
            setStatus(STATUS_FAILED, errorContext.extendedStatus);
            return true;
        }

        // For each file associated with the chunk check if the file is present in
        // the data directory.
        //
        // - not finding a file is not a failrure for this operation. Just reporting
        //   those files which are present.
        //
        // - assume the request failure for any file system operation failure
        //
        // - assume the successfull completion otherwise and adjust the replica
        //   information record accordingly, depending on the findings.


        ReplicaInfo::FileInfoCollection fileInfoCollection; // file info if not using the incremental processing
        std::vector<std::string>        files;              // file paths registered for the incremental processing

        for (auto&& file: FileUtils::partitionedFiles(databaseInfo, chunk())) {

            fs::path        const path = dataDir / file;
            fs::file_status const stat = fs::status(path, ec);

            errorContext = errorContext
                or reportErrorIf(
                        stat.type() == fs::status_error,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                        "failed to check the status of file: " + path.string());

            if (fs::exists(stat)) {

                if (not _computeCheckSum) {

                    // Get file size & mtime right away

                    uint64_t const size = fs::file_size(path, ec);
                    errorContext = errorContext
                        or reportErrorIf(
                                ec,
                                ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE,
                                "failed to read file size: " + path.string());

                    const std::time_t mtime = fs::last_write_time(path, ec);
                    errorContext = errorContext
                        or reportErrorIf(
                                ec,
                                ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME,
                                "failed to read file mtime: " + path.string());

                    fileInfoCollection.emplace_back(
                        ReplicaInfo::FileInfo({
                            file,
                            size,
                            mtime,
                            "",     /* cs */
                            0,      /* beginTransferTime */
                            0,      /* endTransferTime */
                            size    /* inSize */
                        })
                    );
                } else {

                    // Register this file for the incremental processing
                    files.push_back(path.string());
                }
            }
        }
        if (errorContext.failed) {
            setStatus(STATUS_FAILED, errorContext.extendedStatus);
            return true;
        }

        // If that's so then finalize the operation right away
        if (not _computeCheckSum) {

            ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
            if (fileInfoCollection.size())
                status = FileUtils::partitionedFiles(
                                        databaseInfo,
                                        chunk()).size() == fileInfoCollection.size() ?
                                            ReplicaInfo::Status::COMPLETE :
                                            ReplicaInfo::Status::INCOMPLETE;

            // Fill in the info on the chunk before finishing the operation
            _replicaInfo = ReplicaInfo(
                status,
                worker(),
                database(),
                chunk(),
                PerformanceUtils::now(),
                fileInfoCollection);

            setStatus(STATUS_SUCCEEDED);

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
            for (auto&& file: fileNames) {

                const fs::path path(file);

                uint64_t const size = _csComputeEnginePtr->bytes(file);

                std::time_t const mtime = fs::last_write_time(path, ec);
                errorContext = errorContext
                    or reportErrorIf(
                            ec,
                            ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME,
                            "failed to read file mtime: " + path.string());

                fileInfoCollection.emplace_back(
                    ReplicaInfo::FileInfo({
                        path.filename().string(),
                        size,
                        mtime,
                        std::to_string(_csComputeEnginePtr->cs(file)),
                        0,      /* beginTransferTime */
                        0,      /* endTransferTime */
                        size    /* inSize */
                    })
                );
            }
            if (errorContext.failed) {
                setStatus(STATUS_FAILED, errorContext.extendedStatus);
                return true;
            }

            // Fnalize the operation

            DatabaseInfo const& databaseInfo =
                _serviceProvider->config()->databaseInfo(database());

            ReplicaInfo::Status status = ReplicaInfo::Status::NOT_FOUND;
            if (fileInfoCollection.size())
                status = FileUtils::partitionedFiles(
                                        databaseInfo,
                                        chunk()).size() == fileNames.size() ?
                                            ReplicaInfo::Status::COMPLETE :
                                            ReplicaInfo::Status::INCOMPLETE;

            // Fill in the info on the chunk before finishing the operation
            _replicaInfo = ReplicaInfo(
                status,
                worker(),
                database(),
                chunk(),
                PerformanceUtils::now(),
                fileInfoCollection);

            setStatus(STATUS_SUCCEEDED);
        }

    } catch (std::exception const& ex) {
        WorkerRequest::ErrorContext errorContext;
        errorContext = errorContext
            or reportErrorIf(
                    true,
                    ExtendedCompletionStatus::EXT_STATUS_FILE_READ,
                    ex.what());

        setStatus(STATUS_FAILED, errorContext.extendedStatus);
    }

    // If done (either way) then get rid of the engine right away because
    // it may still have allocated buffers
    if (finished) { _csComputeEnginePtr.reset(); }

    return finished;
}

}}} // namespace lsst::qserv::replica
