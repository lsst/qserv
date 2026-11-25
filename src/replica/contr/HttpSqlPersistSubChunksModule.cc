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
#include "replica/contr/HttpSqlPersistSubChunksModule.h"

// System headers
#include <algorithm>
#include <limits>
#include <map>
#include <stdexcept>

// Qserv headers
#include "css/CssAccess.h"
#include "http/Exceptions.h"
#include "http/RequestBodyJSON.h"
#include "http/RequestQuery.h"
#include "replica/config/Config.h"
#include "replica/contr/Controller.h"
#include "replica/jobs/SqlPersistSubChunksJob.h"
#include "replica/jobs/SqlRemoveTablePartitionsJob.h"
#include "replica/requests/SqlResultSet.h"
#include "replica/services/ServiceProvider.h"

using namespace std;
using namespace nlohmann;

namespace lsst::qserv::replica {

void HttpSqlPersistSubChunksModule::process(shared_ptr<Controller> const& controller, string const& taskName,
                                            shared_ptr<qhttp::Request> const& req,
                                            shared_ptr<qhttp::Response> const& resp,
                                            string const& subModuleName, http::AuthType const authType) {
    HttpSqlPersistSubChunksModule module(controller, taskName, req, resp);
    module.execute(subModuleName, authType);
}

HttpSqlPersistSubChunksModule::HttpSqlPersistSubChunksModule(shared_ptr<Controller> const& controller,
                                                             string const& taskName,
                                                             shared_ptr<qhttp::Request> const& req,
                                                             shared_ptr<qhttp::Response> const& resp)
        : HttpModule(controller, taskName, req, resp) {}

json HttpSqlPersistSubChunksModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "CREATE-SUB-CHUNKS")
        return _createSubChunks();
    else if (subModuleName == "DROP-SUB-CHUNKS")
        return _dropSubChunks();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpSqlPersistSubChunksModule::_createSubChunks() { return _manageSubChunks(__func__, true); }

json HttpSqlPersistSubChunksModule::_dropSubChunks() { return _manageSubChunks(__func__, false); }

json HttpSqlPersistSubChunksModule::_manageSubChunks(string const& func, bool persist) {
    debug(func);
    checkApiVersion(func, 65);

    auto const database = getDatabaseFromParamOrThrow404(func);
    auto const table = getTableFromParamOrThrow404(func, database);

    // ------------------------------------------------------------------------------------------
    // IMPORTANT: The implementation of these operations requires the database to be unpublished.
    //            The operation can be lenghy, and it can confuse replica management algorithms.
    // ------------------------------------------------------------------------------------------

    // The safeguards are needed here because the index management job launched
    // doesn't have these restriction.
    if (database.isPublished) throw http::Error(func, "table should not be published");
    if (!(table.isDirector() || table.isRefMatch())) {
        throw http::Error(func, "table should be either a director or a ref-match");
    }

    bool const allWorkers = true;
    string const noParentJobId;
    int const priorityLevel = controller()->serviceProvider()->config()->get<int>(
            "controller", "catalog-management-priority-level");
    json extendedErrorReport;
    if (persist) {
        auto const job = SqlPersistSubChunksJob::create(database.name, table.name, allWorkers, controller(),
                                                        noParentJobId, nullptr, priorityLevel);
        job->start();
        logJobStartedEvent(SqlPersistSubChunksJob::typeName(), job, database.family);
        job->wait();
        logJobFinishedEvent(SqlPersistSubChunksJob::typeName(), job, database.family);
        extendedErrorReport = job->getExtendedErrorReport();
    } else {
        bool const ignoreNonPartitioned = true;
        auto const job = SqlRemoveTablePartitionsJob::create(database.name, table.name, allWorkers,
                                                             ignoreNonPartitioned, controller(),
                                                             noParentJobId, nullptr, priorityLevel);
        job->start();
        logJobStartedEvent(SqlRemoveTablePartitionsJob::typeName(), job, database.family);
        job->wait();
        logJobFinishedEvent(SqlRemoveTablePartitionsJob::typeName(), job, database.family);
        extendedErrorReport = job->getExtendedErrorReport();
    }
    if (!extendedErrorReport.is_null()) {
        throw http::Error(func, "The operation failed. See details in the extended report.",
                          extendedErrorReport);
    }

    // Update table configuraton in the Replication system
    controller()->serviceProvider()->config()->updateTableSubChunks(database.name, table.name, persist);

    // Update CSS
    auto const cssAccess = qservCssAccess();
    cssAccess->setSubChunkMode(database.name, table.name, persist);

    return json::object();
}

}  // namespace lsst::qserv::replica
