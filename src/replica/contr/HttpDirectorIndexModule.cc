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
#include "replica/contr/HttpDirectorIndexModule.h"

// Qserv headers
#include "http/Exceptions.h"
#include "replica/config/Configuration.h"
#include "replica/jobs/DirectorIndexJob.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

using namespace database::mysql;

void HttpDirectorIndexModule::process(Controller::Ptr const& controller, string const& taskName,
                                      HttpProcessorConfig const& processorConfig,
                                      qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                      string const& subModuleName, http::AuthType const authType) {
    HttpDirectorIndexModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpDirectorIndexModule::HttpDirectorIndexModule(Controller::Ptr const& controller, string const& taskName,
                                                 HttpProcessorConfig const& processorConfig,
                                                 qhttp::Request::Ptr const& req,
                                                 qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpDirectorIndexModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "BUILD") return _buildDirectorIndex();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpDirectorIndexModule::_buildDirectorIndex() {
    debug(__func__);
    checkApiVersion(__func__, 22);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    string const databaseName = body().required<string>("database");
    string const directorTableName = body().required<string>("director_table");
    if (body().has("allow_for_published")) {
        warn("Option 'allow_for_published' is obsolete as of the version 22 of the API.");
    }
    bool const rebuild = body().optional<int>("rebuild", 0) != 0;
    if (body().has("local")) {
        warn("Option 'local' is obsolete as of the version 20 of the API.");
    }

    debug(__func__, "database=" + databaseName);
    debug(__func__, "director_table=" + directorTableName);
    debug(__func__, "rebuild=" + bool2str(rebuild));

    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(directorTableName);
    if (!table.isDirector()) {
        string const msg = "table '" + table.name + "' is not configured as a director table in database '" +
                           database.name + "'";
        throw http::Error(__func__, msg);
    }
    if (!table.isPublished) {
        string const msg = " the director table '" + table.name + "' of the database '" + database.name +
                           "' is not published.";
        throw http::Error(__func__, msg);
    }
    // Look for the optional parameter defining the uniqueness of the index's keys
    // assuming the current configuration of the table as the default.
    bool const uniquePrimaryKey =
            body().optional<int>("unique_primary_key", table.uniquePrimaryKey ? 1 : 0) != 0;
    string const primaryKeyType = uniquePrimaryKey ? "UNIQUE KEY" : "KEY";

    debug(__func__, "unique_primary_key=" + bool2str(uniquePrimaryKey));

    // Pre-screen parameters of the table.
    if (table.directorTable.primaryKeyColumn().empty()) {
        string const msg =
                "director table has not been properly configured in database '" + database.name + "'";
        throw http::Error(__func__, msg);
    }
    string const primaryKeyColumn = table.directorTable.primaryKeyColumn();
    string primaryKeyColumnType = string();
    string const chunkIdColNameType = "INT";
    string subChunkIdColNameType = string();
    if (table.columns.empty()) {
        string const msg =
                "no schema found for director table '" + table.name + "' of database '" + database.name + "'";
        throw http::Error(__func__, msg);
    }
    for (auto&& column : table.columns) {
        if (column.name == primaryKeyColumn) {
            primaryKeyColumnType = column.type;
        } else if (column.name == lsst::qserv::SUB_CHUNK_COLUMN) {
            subChunkIdColNameType = column.type;
        }
    }
    if (primaryKeyColumnType.empty() || subChunkIdColNameType.empty()) {
        throw http::Error(__func__,
                          "column definitions for the director key or sub-chunk identifier"
                          " columns are missing in the director table schema for table '" +
                                  table.name + "' of database '" + database.name + "'");
    }

    // Build/rebuild the index(es).
    bool const noTransactions = false;
    bool const allWorkers = true;
    TransactionId const noTransactionId = 0;
    bool failed = false;

    // The object will stay empty if no problems will be encountered during the construction
    // of the index.
    json extError = json::object();

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.
    ConnectionHandler const connHandler(qservMasterDbConnection("qservMeta"));
    QueryGenerator const queryGen(connHandler.conn);
    auto const indexTableName = directorIndexTableName(database.name, directorTableName);

    // (Re-)create the index table. Note that the table creation statement (the way it's
    // written below) would fail if the table already exists. Hence, dropping it in
    // the 'rebuild' mode should be explicitly requested by a client to avoid the problem.
    vector<string> queries;
    if (rebuild) {
        bool const ifExists = true;
        queries.push_back(queryGen.dropTable(indexTableName, ifExists));
    }
    bool const ifNotExists = false;
    list<SqlColDef> const columns = {SqlColDef{primaryKeyColumn, primaryKeyColumnType},
                                     SqlColDef{lsst::qserv::CHUNK_COLUMN, chunkIdColNameType},
                                     SqlColDef{lsst::qserv::SUB_CHUNK_COLUMN, subChunkIdColNameType}};
    list<string> const keys = {queryGen.packTableKey(primaryKeyType, "", primaryKeyColumn)};
    string const query = queryGen.createTable(indexTableName, ifNotExists, columns, keys,
                                              config->get<string>("controller", "director-index-engine"));
    queries.push_back(query);
    connHandler.conn->executeInOwnTransaction([&queries](decltype(connHandler.conn) conn) {
        for (auto&& query : queries) {
            conn->execute(query);
        }
    });
    string const noParentJobId;
    auto const job =
            DirectorIndexJob::create(database.name, directorTableName, noTransactions, noTransactionId,
                                     allWorkers, controller(), noParentJobId,
                                     nullptr,  // no callback
                                     config->get<int>("controller", "catalog-management-priority-level"));
    job->start();
    logJobStartedEvent(DirectorIndexJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(DirectorIndexJob::typeName(), job, database.family);

    // Extended error reporting in case of failures
    if (job->extendedState() != Job::SUCCESS) {
        failed = true;
        DirectorIndexJob::Result const& jobResultData = job->getResultData();
        for (auto&& workerItr : jobResultData.error) {
            string const& workerName = workerItr.first;
            extError[workerName] = json::object();
            json& workerError = extError[workerName];
            for (auto&& chunkItr : workerItr.second) {
                // JSON library requires string type keys
                workerError[to_string(chunkItr.first)] = chunkItr.second;
            }
        }
    }
    if (failed) throw http::Error(__func__, "index creation failed", extError);
    return json::object();
}

}  // namespace lsst::qserv::replica
