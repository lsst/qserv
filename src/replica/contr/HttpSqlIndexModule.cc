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
#include "replica/contr/HttpSqlIndexModule.h"

// System headers
#include <algorithm>
#include <limits>
#include <map>
#include <stdexcept>

// Qserv headers
#include "http/Exceptions.h"
#include "http/RequestBody.h"
#include "replica/config/Configuration.h"
#include "replica/jobs/SqlCreateIndexesJob.h"
#include "replica/jobs/SqlDropIndexesJob.h"
#include "replica/jobs/SqlGetIndexesJob.h"
#include "replica/requests/SqlResultSet.h"

using namespace std;
using namespace nlohmann;

namespace lsst::qserv::replica {

void HttpSqlIndexModule::process(Controller::Ptr const& controller, string const& taskName,
                                 HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp, string const& subModuleName,
                                 http::AuthType const authType) {
    HttpSqlIndexModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpSqlIndexModule::HttpSqlIndexModule(Controller::Ptr const& controller, string const& taskName,
                                       HttpProcessorConfig const& processorConfig,
                                       qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpSqlIndexModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty())
        return _getIndexes();
    else if (subModuleName == "CREATE-INDEXES")
        return _createIndexes();
    else if (subModuleName == "DROP-INDEXES")
        return _dropIndexes();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpSqlIndexModule::_getIndexes() {
    debug(__func__);
    checkApiVersion(__func__, 17);

    string const databaseName = params().at("database");
    string const tableName = params().at("table");
    bool const overlap = query().optionalInt("overlap", 0) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    debug(__func__, "overlap=" + bool2str(overlap));

    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(tableName);

    // This safeguard is needed here because the index management job launched
    // doesn't have this restriction.
    if (!table.isPublished) throw http::Error(__func__, "table is not published");

    bool const allWorkers = true;
    string const noParentJobId;
    auto const job = SqlGetIndexesJob::create(
            database.name, table.name, overlap, allWorkers, controller(), noParentJobId, nullptr,
            config->get<int>("controller", "catalog-management-priority-level"));
    job->start();
    logJobStartedEvent(SqlGetIndexesJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(SqlGetIndexesJob::typeName(), job, database.family);

    auto const extendedErrorReport = job->getExtendedErrorReport();
    if (!extendedErrorReport.is_null()) {
        throw http::Error(__func__, "The operation failed. See details in the extended report.",
                          extendedErrorReport);
    }
    json result;
    result["status"] = job->indexes().toJson();
    return result;
}

json HttpSqlIndexModule::_createIndexes() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const databaseName = body().required<string>("database");
    string const tableName = body().required<string>("table");
    string const indexName = body().required<string>("index");
    string const comment = body().optional<string>("comment", string());
    SqlRequestParams::IndexSpec const spec = SqlRequestParams::IndexSpec(
            body().optional<string>("spec", "DEFAULT", {"DEFAULT", "UNIQUE", "FULLTEXT", "SPATIAL"}));
    json const columnsJson = body().required<json>("columns");
    bool const overlap = body().optional<int>("overlap", 0) != 0;
    bool const ignoreDuplicateKey = body().optional<int>("ignore_duplicate_key", 1) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    debug(__func__, "index=" + indexName);
    debug(__func__, "comment=" + comment);
    debug(__func__, "spec=" + spec.str());
    debug(__func__, "columns.size()=" + columnsJson.size());
    debug(__func__, "overlap=" + bool2str(overlap));
    debug(__func__, "ignore_duplicate_key=" + bool2str(ignoreDuplicateKey));

    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(tableName);

    // This safeguard is needed here because the index management job launched
    // doesn't have this restriction.
    if (!table.isPublished) throw http::Error(__func__, "table is not published");

    // Process the input collection of the column specifications.
    //
    // At this step an optional (if the table schema is available) effort
    // to evaluate the column specification will be made to ensure the columns
    // are present in the schema.
    //
    // TODO: another possibility would be to either pull the schema from
    // the information schema of the Qserv czar's database or to "pre-flight"
    // the index creation against the table instance. Though, the later idea
    // has potential complications - the index may already exist in that table.

    if (!columnsJson.is_array()) {
        throw invalid_argument(context() + "::" + string(__func__) +
                               "  parameter 'columns' is not a simple JSON array.");
    }
    vector<SqlIndexColumn> indexColumns;
    for (auto&& columnJson : columnsJson) {
        string const column = http::RequestBody::required<string>(columnJson, "column");
        if (!table.columns.empty() and
            table.columns.cend() == find_if(table.columns.cbegin(), table.columns.cend(),
                                            [&column](auto&& c) { return c.name == column; })) {
            throw invalid_argument(context() + "::" + string(__func__) + "  requested column '" + column +
                                   "' has not been found in the table schema.");
        }
        indexColumns.emplace_back(column, http::RequestBody::required<size_t>(columnJson, "length"),
                                  http::RequestBody::required<int>(columnJson, "ascending"));
    }

    bool const allWorkers = true;
    string const noParentJobId;
    auto const job = SqlCreateIndexesJob::create(
            database.name, table.name, overlap, spec, indexName, comment, indexColumns, allWorkers,
            ignoreDuplicateKey, controller(), noParentJobId, nullptr,
            config->get<int>("controller", "catalog-management-priority-level"));
    job->start();
    logJobStartedEvent(SqlCreateIndexesJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(SqlCreateIndexesJob::typeName(), job, database.family);

    auto const extendedErrorReport = job->getExtendedErrorReport();
    if (!extendedErrorReport.is_null()) {
        throw http::Error(__func__, "The operation failed. See details in the extended report.",
                          extendedErrorReport);
    }
    return json::object();
}

json HttpSqlIndexModule::_dropIndexes() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const databaseName = body().required<string>("database");
    string const tableName = body().required<string>("table");
    string const indexName = body().required<string>("index");
    bool const overlap = body().optional<int>("overlap", 0) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    debug(__func__, "index=" + indexName);
    debug(__func__, "overlap=" + bool2str(overlap));

    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(tableName);

    // This safeguard is needed here because the index management job launched
    // doesn't have this restriction.
    if (!table.isPublished) throw http::Error(__func__, "table is not published");

    bool const allWorkers = true;
    string const noParentJobId;
    auto const job = SqlDropIndexesJob::create(
            database.name, table.name, overlap, indexName, allWorkers, controller(), noParentJobId, nullptr,
            config->get<int>("controller", "catalog-management-priority-level"));
    job->start();
    logJobStartedEvent(SqlDropIndexesJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(SqlDropIndexesJob::typeName(), job, database.family);

    auto const extendedErrorReport = job->getExtendedErrorReport();
    if (!extendedErrorReport.is_null()) {
        throw http::Error(__func__, "The operation failed. See details in the extended report.",
                          extendedErrorReport);
    }
    return json::object();
}

}  // namespace lsst::qserv::replica
