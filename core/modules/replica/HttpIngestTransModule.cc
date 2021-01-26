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
#include "replica/HttpIngestTransModule.h"

// System headers
#include <limits>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/AbortTransactionJob.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/IndexJob.h"
#include "replica/ServiceProvider.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace lsst {
namespace qserv {
namespace replica {


void HttpIngestTransModule::process(Controller::Ptr const& controller,
                                    string const& taskName,
                                    HttpProcessorConfig const& processorConfig,
                                    qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp,
                                    string const& subModuleName,
                                    HttpModule::AuthType const authType) {
    HttpIngestTransModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpIngestTransModule::HttpIngestTransModule(
            Controller::Ptr const& controller,
            string const& taskName,
            HttpProcessorConfig const& processorConfig,
            qhttp::Request::Ptr const& req,
            qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpIngestTransModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "TRANSACTIONS") return _getTransactions();
    else if (subModuleName == "SELECT-TRANSACTION-BY-ID") return _getTransaction();
    else if (subModuleName == "BEGIN-TRANSACTION") return _beginTransaction();
    else if (subModuleName == "END-TRANSACTION") return _endTransaction();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpIngestTransModule::_getTransactions() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    auto const database     = query().optionalString("database");
    auto const family       = query().optionalString("family");
    auto const allDatabases = query().optionalUInt64("all_databases", 0) != 0;
    auto const isPublished  = query().optionalUInt64("is_published",  0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "family=" + family);
    debug(__func__, "all_databases=" + bool2str(allDatabases));
    debug(__func__, "is_published=" + bool2str(isPublished));

    vector<string> databases;
    if (database.empty()) {
        databases = config->databases(family, allDatabases, isPublished);
    } else {
        databases.push_back(database);
    }

    json result;
    result["databases"] = json::object();
    for (auto&& database: databases) {

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database, allWorkers);

        result["databases"][database]["num_chunks"] = chunks.size();
        result["databases"][database]["transactions"] = json::array();
        for (auto&& transaction: databaseServices->transactions(database)) {
            result["databases"][database]["transactions"].push_back(transaction.toJson());
        }
    }
    return result;
}


json HttpIngestTransModule::_getTransaction() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const id = stoul(params().at("id"));

    debug(__func__, "id=" + to_string(id));

    auto const transaction = databaseServices->transaction(id);

    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, transaction.database, allWorkers);

    json result;
    result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
    result["databases"][transaction.database]["num_chunks"] = chunks.size();
    return result;
}


json HttpIngestTransModule::_beginTransaction() {
    debug(__func__);

    // Keep the transaction object in this scope to allow logging a status
    // of the operation regardless if it succeeds or fails. The name of a database
    // encoded in the object will get initialized from the REST request's
    // parameter. And the rest will be set up after attempting to actually start
    // the transaction.
    TransactionInfo transaction;

    auto const logBeginTransaction = [&](string const& status, string const& msg=string()) {
        ControllerEvent event;
        event.operation = "BEGIN TRANSACTION";
        event.status = status;
        event.kvInfo.emplace_back("id", to_string(transaction.id));
        event.kvInfo.emplace_back("database", transaction.database);
        if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
        logEvent(event);
    };
    
    // Intercept standard exceptions just to report the failure, then
    // let a caller to do the rest (post error messages into the Logger,
    // return errors to clients).
    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        transaction.database = body().required<string>("database");

        debug(__func__, "database=" + transaction.database);

        auto const databaseInfo = config->databaseInfo(transaction.database);
        if (databaseInfo.isPublished) {
            throw HttpError(__func__, "the database is already published");
        }
        if (databaseInfo.directorTable.empty()) {
            throw HttpError(__func__, "director table has not been configured in database '" +
                            databaseInfo.name + "'");
        }

        // Get chunks stats to be reported with the request's result object
        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, databaseInfo.name, allWorkers);

        // Any problems during the secondary index creation will result in
        // automatically aborting the transaction. Otherwise ingest workflows
        // may be screwed/confused by the presence of the "invisible" transaction.
        transaction = databaseServices->beginTransaction(databaseInfo.name);
        try {
            // This operation can be vetoed by a catalog ingest workflow at the database
            // registration time.
            if (autoBuildSecondaryIndex(databaseInfo.name)) {
                _addPartitionToSecondaryIndex(databaseInfo, transaction.id);
            }
        } catch (...) {
            bool const abort = true;
            transaction = databaseServices->endTransaction(transaction.id, abort);
            throw;
        }
        logBeginTransaction("SUCCESS");

        json result;
        result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
        result["databases"][transaction.database]["num_chunks"] = chunks.size();
        return result;

    } catch (invalid_argument const& ex) {
        logBeginTransaction("FAILED", "invalid parameters of the request, ex: " + string(ex.what()));
        throw;
    } catch (exception const& ex) {
        logBeginTransaction("FAILED", "operation failed due to: " + string(ex.what()));
        throw;
    }
}


json HttpIngestTransModule::_endTransaction() {
    debug(__func__);

    TransactionId id = 0;
    string database;
    bool abort = false;

    auto const logEndTransaction = [&](string const& status, string const& msg=string()) {
        ControllerEvent event;
        event.operation = "END TRANSACTION";
        event.status = status;
        event.kvInfo.emplace_back("id", to_string(id));
        event.kvInfo.emplace_back("database", database);
        event.kvInfo.emplace_back("abort", abort ? "true" : "false");
        if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
        logEvent(event);
    };

    // Intercept standard exceptions just to report the failure, then
    // let a caller to do the rest (post error messages into the Logger,
    // return errors to clients).
    try {
        auto const config = controller()->serviceProvider()->config();
        auto const databaseServices = controller()->serviceProvider()->databaseServices();

        id = stoul(params().at("id"));

        abort = query().requiredBool("abort");

        debug(__func__, "id="    + to_string(id));
        debug(__func__, "abort=" + to_string(abort ? 1 : 0));

        auto const transaction = databaseServices->endTransaction(id, abort);
        auto const databaseInfo = config->databaseInfo(transaction.database);
        database = transaction.database;

        bool const allWorkers = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, transaction.database, allWorkers);

        json result;
        result["databases"][transaction.database]["transactions"].push_back(transaction.toJson());
        result["databases"][transaction.database]["num_chunks"] = chunks.size();
        result["secondary-index-build-success"] = 0;

        if (abort) {

            // Drop the transaction-specific MySQL partition from the relevant tables
            auto const job = AbortTransactionJob::create(transaction.id, allWorkers, controller());
            job->start();
            logJobStartedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            job->wait();
            logJobFinishedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);

            // This operation in a context of the "secondary index" table can be vetoed by
            // a catalog ingest workflow at the database registration time.
            if (autoBuildSecondaryIndex(databaseInfo.name)) {
                _removePartitionFromSecondaryIndex(databaseInfo, transaction.id);
            }

        } else {

            // Make the best attempt to build a layer at the "secondary index" if requested
            // by a catalog ingest workflow at the database registration time.
            if (autoBuildSecondaryIndex(databaseInfo.name)) {
                bool const hasTransactions = true;
                string const destinationPath = transaction.database + "__" + databaseInfo.directorTable;
                auto const job = IndexJob::create(
                    transaction.database,
                    hasTransactions,
                    transaction.id,
                    allWorkers,
                    IndexJob::TABLE,
                    destinationPath,
                    localLoadSecondaryIndex(databaseInfo.name),
                    controller()
                );
                job->start();
                logJobStartedEvent(IndexJob::typeName(), job, databaseInfo.family);
                job->wait();
                logJobFinishedEvent(IndexJob::typeName(), job, databaseInfo.family);
                result["secondary-index-build-success"] = job->extendedState() == Job::SUCCESS ? 1 : 0;
            }

            // TODO: replicate MySQL partition associated with the transaction
            info(__func__, "replication stage is not implemented");
        }
        logEndTransaction("SUCCESS");

        return result;

    } catch (invalid_argument const& ex) {
        logEndTransaction("FAILED", "invalid parameters of the request, ex: " + string(ex.what()));
        throw;
    } catch (exception const& ex) {
        logEndTransaction("FAILED", "operation failed due to: " + string(ex.what()));
        throw;
    }
}


void HttpIngestTransModule::_addPartitionToSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                          TransactionId transactionId) const {
    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " ADD PARTITION (PARTITION `p" + to_string(transactionId) + "` VALUES IN (" + to_string(transactionId) +
        ") ENGINE=InnoDB)";

    debug(__func__, query);

    h.conn->execute([&query](decltype(h.conn) conn) {
        conn->begin();
        conn->execute(query);
        conn->commit();
    });
}


void HttpIngestTransModule::_removePartitionFromSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                               TransactionId transactionId) const {
    if (databaseInfo.directorTable.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + databaseInfo.directorTable) +
        " DROP PARTITION `p" + to_string(transactionId) + "`";

    debug(__func__, query);

    // Not having the specified partition is still fine as it couldn't be properly
    // created after the transaction was created.
    try {
        h.conn->execute([&query](decltype(h.conn) conn) {
            conn->begin();
            conn->execute(query);
            conn->commit();
        });
    } catch (database::mysql::DropPartitionNonExistent const&) {
        ;
    }
}

}}}  // namespace lsst::qserv::replica
