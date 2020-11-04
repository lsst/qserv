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
    bool const allowForPublished = body().optional<int>("allow_for_published", 0) != 0;
    bool const rebuild = body().optional<int>("rebuild", 0) != 0;
    bool const localFile = body().optional<int>("local", 0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "allow_for_published=" + bool2str(allowForPublished));
    debug(__func__, "rebuild=" + bool2str(rebuild));
    debug(__func__, "local=" + bool2str(localFile));

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished and not allowForPublished) {
        throw HttpError(__func__, "database '" + databaseInfo.name +
                "' is already published. Use 'allow_for_published' option to override the restriction.");
    }
    if (databaseInfo.directorTable.empty() or databaseInfo.directorTableKey.empty() or
        databaseInfo.chunkIdColName.empty() or databaseInfo.subChunkIdColName.empty()) {
        throw HttpError(
                __func__,
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }
    if (0 == databaseInfo.columns.count(databaseInfo.directorTable)) {
        throw HttpError(
                __func__,
                "no schema found for director table '" + databaseInfo.directorTable +
                "' of database '" + databaseInfo.name + "'");
    }

    // Find types of the secondary index table's columns

    string directorTableKeyType;
    string chunkIdColNameType;
    string subChunkIdColNameType;

    for (auto&& coldef: databaseInfo.columns.at(databaseInfo.directorTable)) {
        if      (coldef.name == databaseInfo.directorTableKey)  directorTableKeyType  = coldef.type;
        else if (coldef.name == databaseInfo.chunkIdColName)    chunkIdColNameType    = coldef.type;
        else if (coldef.name == databaseInfo.subChunkIdColName) subChunkIdColNameType = coldef.type;
    }
    if (directorTableKeyType.empty() or chunkIdColNameType.empty() or subChunkIdColNameType.empty()) {
        throw HttpError(
                __func__,
                "column definitions for the Object identifier or chunk/sub-chunk identifier"
                " columns are missing in the director table schema for table '" +
                databaseInfo.directorTable + "' of database '" + databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    auto const tableName = databaseInfo.name + "__" + databaseInfo.directorTable;
    auto const escapedTableName = h.conn->sqlId(tableName);

    // (Re-)create the index table. Note that the table creation statement (the way it's
    // written below) would fail if the table already exists. Hence, dropping it in
    // the 'rebuild' mode should be explicitly requested by a client to avoid the problem.
    vector<string> queries;
    if (rebuild) queries.push_back("DROP TABLE IF EXISTS " + escapedTableName);
    queries.push_back(
        "CREATE TABLE " + escapedTableName +
        " (" + h.conn->sqlId(databaseInfo.directorTableKey) + " " + directorTableKeyType + "," +
               h.conn->sqlId(databaseInfo.chunkIdColName) + " " + chunkIdColNameType + "," +
               h.conn->sqlId(databaseInfo.subChunkIdColName) + " " + subChunkIdColNameType + ","
               " UNIQUE KEY (" + h.conn->sqlId(databaseInfo.directorTableKey) + "),"
               " KEY (" + h.conn->sqlId(databaseInfo.directorTableKey) + ")"
        ") ENGINE=InnoDB"
    );
    h.conn->execute([&queries](decltype(h.conn) conn) {
        conn->begin();
        for (auto&& query: queries) {
            conn->execute(query);
        }
        conn->commit();
    });

    bool const noTransactions = false;
    bool const allWorkers = true;
    TransactionId const noTransactionId = 0;
    auto const job = IndexJob::create(
        databaseInfo.name,
        noTransactions,
        noTransactionId,
        allWorkers,
        IndexJob::TABLE,
        tableName,
        localFile,
        controller()
    );
    job->start();
    logJobStartedEvent(IndexJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(IndexJob::typeName(), job, databaseInfo.family);

    // Nothing to report in case of success
    if (job->extendedState() == Job::SUCCESS) return json::object();

    // Extended error reporting in case of failures
    IndexJobResult const& jobResultData = job->getResultData();

    json extError = json::object();
    for (auto&& workerItr: jobResultData.error) {
        string const& worker = workerItr.first;
        extError[worker] = json::object();
        json& workerError = extError[worker];
        for (auto&& chunkItr: workerItr.second) {
            // JSON library requires string type keys
            workerError[to_string(chunkItr.first)] = chunkItr.second;
        }
    }
    throw HttpError(__func__, "index creation failed", extError);
}

}}}  // namespace lsst::qserv::replica
