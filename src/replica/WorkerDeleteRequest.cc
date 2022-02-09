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
#include "replica/WorkerDeleteRequest.h"

// System headers

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/FileUtils.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
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
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestDelete const& request) {
    return WorkerDeleteRequest::Ptr(new WorkerDeleteRequest(
        serviceProvider,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
    ));
}


WorkerDeleteRequest::WorkerDeleteRequest(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestDelete const& request)
    :   WorkerRequest(
            serviceProvider,
            worker,
            "DELETE",
            id,
            priority,
            onExpired,
            requestExpirationIvalSec),
        _request(request),
        // This status will be returned in all contexts
        _replicaInfo(
            ReplicaInfo::Status::NOT_FOUND,
            worker,
            request.database(),
            request.chunk(),
            PerformanceUtils::now(),
            ReplicaInfo::FileInfoCollection{}) {
}
                     

void WorkerDeleteRequest::setInfo(ProtocolResponseDelete& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    response.set_allocated_target_performance(performance().info().release());
    response.set_allocated_replica_info(_replicaInfo.info().release());

    *(response.mutable_request()) = _request;
}


bool WorkerDeleteRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
        << "  db: "    << database()
        << "  chunk: " << chunk());

    return WorkerRequest::execute();
}


///////////////////////////////////////////////////////////////////
///////////////////// WorkerDeleteRequestPOSIX ////////////////////
///////////////////////////////////////////////////////////////////

WorkerDeleteRequestPOSIX::Ptr WorkerDeleteRequestPOSIX::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestDelete const& request) {
    return WorkerDeleteRequestPOSIX::Ptr(new WorkerDeleteRequestPOSIX(
        serviceProvider,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
    ));
}


WorkerDeleteRequestPOSIX::WorkerDeleteRequestPOSIX(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestDelete const& request)
    :   WorkerDeleteRequest(
            serviceProvider,
            worker,
            id,
            priority,
            onExpired,
            requestExpirationIvalSec,
            request) {
}


bool WorkerDeleteRequestPOSIX::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << "  db: "    << database()
         << "  chunk: " << chunk());

    util::Lock lock(_mtx, context(__func__));

    auto const config = _serviceProvider->config();
    DatabaseInfo const databaseInfo = config->databaseInfo(database());

    vector<string> const files =  FileUtils::partitionedFiles(databaseInfo, chunk());

    // The data folder will be locked while performing the operation

    int numFilesDeleted = 0;

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;
    {
        util::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));

        fs::path        const dataDir = fs::path(config->get<string>("worker", "data-dir")) / database();
        fs::file_status const stat    = fs::status(dataDir, ec);
        errorContext = errorContext
            or reportErrorIf(
                    stat.type() == fs::status_error,
                    ProtocolStatusExt::FOLDER_STAT,
                    "failed to check the status of directory: " + dataDir.string())
            or reportErrorIf(
                    !fs::exists(stat),
                    ProtocolStatusExt::NO_FOLDER,
                    "the directory does not exists: " + dataDir.string());

        for (const auto &name: files) {
            const fs::path file = dataDir / fs::path(name);
            if (fs::remove(file, ec)) ++numFilesDeleted;
            errorContext = errorContext
                or reportErrorIf(
                        ec.value() != 0,
                        ProtocolStatusExt::FILE_DELETE,
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

}}} // namespace lsst::qserv::replica
