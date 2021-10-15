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

namespace lsst {
namespace qserv {
namespace replica {

void HttpIngestIndexModule::process(Controller::Ptr const& controller,
                                    string const& taskName,
                                    HttpProcessorConfig const& processorConfig,
                                    qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp,
                                    string const& subModuleName,
                                    HttpModule::AuthType const authType) {
    HttpIngestIndexModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpIngestIndexModule::HttpIngestIndexModule(Controller::Ptr const& controller,
                                             string const& taskName,
                                             HttpProcessorConfig const& processorConfig,
                                             qhttp::Request::Ptr const& req,
                                             qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpIngestIndexModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "BUILD-SECONDARY-INDEX") return _buildSecondaryIndex();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpIngestIndexModule::_buildSecondaryIndex() {
    debug(__func__);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    string const database = body().required<string>("database");
    string const directorTable = body().optional<string>("director_table", string());
    bool const allowForPublished = body().optional<int>("allow_for_published", 0) != 0;
    bool const rebuild = body().optional<int>("rebuild", 0) != 0;
    bool const localFile = body().optional<int>("local", 0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "director_table=" + directorTable);
    debug(__func__, "allow_for_published=" + bool2str(allowForPublished));
    debug(__func__, "rebuild=" + bool2str(rebuild));
    debug(__func__, "local=" + bool2str(localFile));

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished and not allowForPublished) {
        throw HttpError(__func__, "database '" + databaseInfo.name +
                "' is already published. Use 'allow_for_published' option to override the restriction.");
    }
    vector<string> const directorTables = directorTable.empty() ?
            databaseInfo.directorTables() : vector<string>({directorTable});

    // Pre-screen parameters of the table(s).
    map<string, string> directorTableKey;
    map<string, string> directorTableKeyType;
    map<string, string> chunkIdColNameType;
    map<string, string> subChunkIdColNameType;

    for (auto&& table: directorTables) {
        if (!databaseInfo.isDirector(table)) {
            throw HttpError(
                    __func__,
                    "table '" + table + "' is not configured as the director table in database '" +
                    databaseInfo.name + "'");
        }
        if ((databaseInfo.directorTableKey.count(table) == 0) || databaseInfo.directorTableKey.at(table).empty()) {
            throw HttpError(
                    __func__,
                    "director table has not been properly configured in database '" +
                    databaseInfo.name + "'");
        }
        directorTableKey[table] = databaseInfo.directorTableKey.at(table);

        if (0 == databaseInfo.columns.count(table)) {
            throw HttpError(
                    __func__,
                    "no schema found for director table '" + table +
                    "' of database '" + databaseInfo.name + "'");
        }

        // Find types of the secondary index table's columns
        directorTableKeyType[table] = string();
        chunkIdColNameType[table] = "INT";
        subChunkIdColNameType[table] = string();
        for (auto&& coldef: databaseInfo.columns.at(table)) {
            if      (coldef.name == directorTableKey[table]) directorTableKeyType[table]  = coldef.type;
            else if (coldef.name == lsst::qserv::SUB_CHUNK_COLUMN) subChunkIdColNameType[table] = coldef.type;
        }
        if (directorTableKeyType[table].empty() || subChunkIdColNameType[table].empty()) {
            throw HttpError(
                    __func__,
                    "column definitions for the director key or sub-chunk identifier"
                    " columns are missing in the director table schema for table '" +
                    table + "' of database '" + databaseInfo.name + "'");
        }
    }

    // Build/rebuild the index(es).

    bool const noTransactions = false;
    bool const allWorkers = true;
    TransactionId const noTransactionId = 0;
    json extError = json::object();
    bool failed = false;
    for (auto&& table: directorTables) {

        // The entry will have the empty object for the tables if no problems will be
        // encountered during the construction of the index.
        extError[table] = json::object();

        // Manage the new connection via the RAII-style handler to ensure the transaction
        // is automatically rolled-back in case of exceptions.
        database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
        auto const indexTableName = databaseInfo.name + "__" + table;
        auto const escapedIndexTableName = h.conn->sqlId(indexTableName);

        // (Re-)create the index table. Note that the table creation statement (the way it's
        // written below) would fail if the table already exists. Hence, dropping it in
        // the 'rebuild' mode should be explicitly requested by a client to avoid the problem.
        vector<string> queries;
        if (rebuild) queries.push_back("DROP TABLE IF EXISTS " + escapedIndexTableName);
        queries.push_back(
            "CREATE TABLE " + escapedIndexTableName +
            " (" + h.conn->sqlId(directorTableKey[table]) + " " + directorTableKeyType[table] + "," +
                h.conn->sqlId(lsst::qserv::CHUNK_COLUMN) + " " + chunkIdColNameType[table] + "," +
                h.conn->sqlId(lsst::qserv::SUB_CHUNK_COLUMN) + " " + subChunkIdColNameType[table] + ","
                " UNIQUE KEY (" + h.conn->sqlId(directorTableKey[table]) + "),"
                " KEY (" + h.conn->sqlId(directorTableKey[table]) + ")"
            ") ENGINE=InnoDB"
        );
        h.conn->executeInOwnTransaction([&queries](decltype(h.conn) conn) {
            for (auto&& query: queries) {
                conn->execute(query);
            }
        });
        auto const job = IndexJob::create(
            databaseInfo.name,
            table,
            noTransactions,
            noTransactionId,
            allWorkers,
            IndexJob::TABLE,
            indexTableName,
            localFile,
            controller()
        );
        job->start();
        logJobStartedEvent(IndexJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(IndexJob::typeName(), job, databaseInfo.family);

        // Extended error reporting in case of failures
        if (job->extendedState() != Job::SUCCESS) {
            failed = true;
            IndexJobResult const& jobResultData = job->getResultData();
            for (auto&& workerItr: jobResultData.error) {
                string const& worker = workerItr.first;
                extError[table][worker] = json::object();
                json& workerError = extError[table][worker];
                for (auto&& chunkItr: workerItr.second) {
                    // JSON library requires string type keys
                    workerError[to_string(chunkItr.first)] = chunkItr.second;
                }
            }
        }
    }
    if (failed) throw HttpError(__func__, "index creation failed", extError);
    return json::object();
}

}}}  // namespace lsst::qserv::replica
