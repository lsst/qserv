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
#include "replica/HttpIngestIndexModule.h"

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/IndexJob.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

using namespace database::mysql;

void HttpIngestIndexModule::process(Controller::Ptr const& controller, string const& taskName,
                                    HttpProcessorConfig const& processorConfig,
                                    qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                    string const& subModuleName, HttpAuthType const authType) {
    HttpIngestIndexModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpIngestIndexModule::HttpIngestIndexModule(Controller::Ptr const& controller, string const& taskName,
                                             HttpProcessorConfig const& processorConfig,
                                             qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpIngestIndexModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "BUILD-SECONDARY-INDEX") return _buildSecondaryIndex();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpIngestIndexModule::_buildSecondaryIndex() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    string const databaseName = body().required<string>("database");
    string const directorTableName = body().optional<string>("director_table", string());
    bool const allowForPublished = body().optional<int>("allow_for_published", 0) != 0;
    bool const rebuild = body().optional<int>("rebuild", 0) != 0;
    bool const localFile = body().optional<int>("local", 0) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "director_table=" + directorTableName);
    debug(__func__, "allow_for_published=" + bool2str(allowForPublished));
    debug(__func__, "rebuild=" + bool2str(rebuild));
    debug(__func__, "local=" + bool2str(localFile));

    auto const database = config->databaseInfo(databaseName);
    if (database.isPublished and not allowForPublished) {
        throw HttpError(__func__, "database '" + database.name +
                                          "' is already published. Use 'allow_for_published' option to "
                                          "override the restriction.");
    }
    vector<string> const directorTables =
            directorTableName.empty() ? database.directorTables() : vector<string>({directorTableName});

    // Pre-screen parameters of the table(s).
    map<string, string> primaryKeyColumn;
    map<string, string> primaryKeyColumnType;
    map<string, string> chunkIdColNameType;
    map<string, string> subChunkIdColNameType;

    for (auto&& tableName : directorTables) {
        auto const table = database.findTable(tableName);
        if (!table.isDirector) {
            string const msg = "table '" + table.name +
                               "' is not configured as the director table in database '" + database.name +
                               "'";
            throw HttpError(__func__, msg);
        }
        if (table.directorTable.primaryKeyColumn().empty()) {
            string const msg =
                    "director table has not been properly configured in database '" + database.name + "'";
            throw HttpError(__func__, msg);
        }
        primaryKeyColumn[table.name] = table.directorTable.primaryKeyColumn();

        if (table.columns.empty()) {
            string const msg = "no schema found for director table '" + table.name + "' of database '" +
                               database.name + "'";
            throw HttpError(__func__, msg);
        }

        // Find types of the secondary index table's columns
        primaryKeyColumnType[table.name] = string();
        chunkIdColNameType[table.name] = "INT";
        subChunkIdColNameType[table.name] = string();
        for (auto&& column : table.columns) {
            if (column.name == primaryKeyColumn[table.name]) {
                primaryKeyColumnType[table.name] = column.type;
            } else if (column.name == lsst::qserv::SUB_CHUNK_COLUMN) {
                subChunkIdColNameType[table.name] = column.type;
            }
        }
        if (primaryKeyColumnType[table.name].empty() || subChunkIdColNameType[table.name].empty()) {
            throw HttpError(__func__,
                            "column definitions for the director key or sub-chunk identifier"
                            " columns are missing in the director table schema for table '" +
                                    table.name + "' of database '" + database.name + "'");
        }
    }

    // Build/rebuild the index(es).

    bool const noTransactions = false;
    bool const allWorkers = true;
    TransactionId const noTransactionId = 0;
    json extError = json::object();
    bool failed = false;
    for (auto&& tableName : directorTables) {
        // The entry will have the empty object for the tables if no problems will be
        // encountered during the construction of the index.
        extError[tableName] = json::object();

        // Manage the new connection via the RAII-style handler to ensure the transaction
        // is automatically rolled-back in case of exceptions.
        ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
        QueryGenerator const g(h.conn);
        auto const indexTableName = database.name + "__" + tableName;

        // (Re-)create the index table. Note that the table creation statement (the way it's
        // written below) would fail if the table already exists. Hence, dropping it in
        // the 'rebuild' mode should be explicitly requested by a client to avoid the problem.
        vector<string> queries;
        if (rebuild) {
            bool const ifExists = true;
            queries.push_back(g.dropTable(indexTableName, ifExists));
        }
        bool const ifNotExists = false;
        list<SqlColDef> const columns = {
                SqlColDef{primaryKeyColumn[tableName], primaryKeyColumnType[tableName]},
                SqlColDef{lsst::qserv::CHUNK_COLUMN, chunkIdColNameType[tableName]},
                SqlColDef{lsst::qserv::SUB_CHUNK_COLUMN, subChunkIdColNameType[tableName]}};
        list<string> const keys = {g.packTableKey("UNIQUE KEY", "", primaryKeyColumn[tableName])};
        string const query = g.createTable(indexTableName, ifNotExists, columns, keys, "InnoDB");
        queries.push_back(query);
        h.conn->executeInOwnTransaction([&queries](decltype(h.conn) conn) {
            for (auto&& query : queries) {
                conn->execute(query);
            }
        });
        string const noParentJobId;
        auto const job =
                IndexJob::create(database.name, tableName, noTransactions, noTransactionId, allWorkers,
                                 IndexJob::TABLE, indexTableName, localFile, controller(), noParentJobId,
                                 nullptr,  // no callback
                                 config->get<int>("controller", "catalog-management-priority-level"));
        job->start();
        logJobStartedEvent(IndexJob::typeName(), job, database.family);
        job->wait();
        logJobFinishedEvent(IndexJob::typeName(), job, database.family);

        // Extended error reporting in case of failures
        if (job->extendedState() != Job::SUCCESS) {
            failed = true;
            IndexJobResult const& jobResultData = job->getResultData();
            for (auto&& workerItr : jobResultData.error) {
                string const& workerName = workerItr.first;
                extError[tableName][workerName] = json::object();
                json& workerError = extError[tableName][workerName];
                for (auto&& chunkItr : workerItr.second) {
                    // JSON library requires string type keys
                    workerError[to_string(chunkItr.first)] = chunkItr.second;
                }
            }
        }
    }
    if (failed) throw HttpError(__func__, "index creation failed", extError);
    return json::object();
}

}  // namespace lsst::qserv::replica
