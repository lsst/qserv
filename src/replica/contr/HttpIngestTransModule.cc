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
#include "replica/contr/HttpIngestTransModule.h"

// System headers
#include <limits>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>
#include <unordered_map>

// Qserv headers
#include "http/Exceptions.h"
#include "http/RequestQuery.h"
#include "replica/config/Configuration.h"
#include "replica/jobs/AbortTransactionJob.h"
#include "replica/jobs/DirectorIndexJob.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Mutex.h"
#include "replica/util/NamedMutexRegistry.h"
#include "util/String.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {
float const GiB = 1024 * 1024 * 1024;

template <typename T>
void incrementBy(json& obj, string const& key, T const val) {
    T const prev = obj[key].get<T>();
    obj[key] = prev + val;
}
}  // namespace

namespace lsst::qserv::replica {

void HttpIngestTransModule::process(Controller::Ptr const& controller,
                                    NamedMutexRegistry& transactionMutexRegistry, string const& taskName,
                                    HttpProcessorConfig const& processorConfig,
                                    qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                                    string const& subModuleName, http::AuthType const authType) {
    HttpIngestTransModule module(controller, transactionMutexRegistry, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpIngestTransModule::HttpIngestTransModule(Controller::Ptr const& controller,
                                             NamedMutexRegistry& transactionMutexRegistry,
                                             string const& taskName,
                                             HttpProcessorConfig const& processorConfig,
                                             qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp),
          _transactionMutexRegistry(transactionMutexRegistry) {}

json HttpIngestTransModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "TRANSACTIONS")
        return _getTransactions();
    else if (subModuleName == "SELECT-TRANSACTION-BY-ID")
        return _getTransaction();
    else if (subModuleName == "BEGIN-TRANSACTION")
        return _beginTransaction();
    else if (subModuleName == "END-TRANSACTION")
        return _endTransaction();
    else if (subModuleName == "GET-CONTRIBUTION-BY-ID")
        return _getContribution();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpIngestTransModule::_getTransactions() {
    debug(__func__);
    checkApiVersion(__func__, 37);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    auto const databaseName = query().optionalString("database");
    auto const family = query().optionalString("family");
    auto const allDatabases = query().optionalUInt64("all_databases", 0) != 0;
    auto const isPublished = query().optionalUInt64("is_published", 0) != 0;
    auto const includeContributions = query().optionalUInt64("contrib", 0) != 0;
    auto const longContribFormat = query().optionalUInt64("contrib_long", 0) != 0;
    bool const includeContext = query().optionalUInt64("include_context", 0) != 0;
    bool const includeLog = query().optionalUInt64("include_log", 0) != 0;
    bool const includeExtensions = query().optionalUInt64("include_extensions", 0) != 0;
    bool const includeWarnings = query().optionalUInt64("include_warnings", 0) != 0;
    bool const includeRetries = query().optionalUInt64("include_retries", 0) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "family=" + family);
    debug(__func__, "all_databases=" + bool2str(allDatabases));
    debug(__func__, "is_published=" + bool2str(isPublished));
    debug(__func__, "contrib=" + bool2str(includeContributions));
    debug(__func__, "contrib_long=" + bool2str(longContribFormat));
    debug(__func__, "include_context=" + bool2str(includeContext));
    debug(__func__, "include_log=" + bool2str(includeLog));
    debug(__func__, "include_extensions=" + bool2str(includeExtensions));
    debug(__func__, "include_warnings=" + bool2str(includeWarnings));
    debug(__func__, "include_retries=" + bool2str(includeRetries));

    auto const transStateSelector = _parseTransStateSelector("trans_state");
    auto const contribStatusSelector = _parseContribStatusSelector("contrib_status");

    vector<string> databases;
    if (databaseName.empty()) {
        databases = config->databases(family, allDatabases, isPublished);
    } else {
        databases.push_back(databaseName);
    }

    string const anyTableSelector;
    string const anyWorkerSelector;
    bool const allWorkers = true;
    int const anyChunkSelector = -1;
    json result;
    result["databases"] = json::object();
    for (auto&& databaseName : databases) {
        auto const database = config->databaseInfo(databaseName);
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database.name, allWorkers);

        result["databases"][database.name]["is_published"] = database.isPublished ? 1 : 0;
        result["databases"][database.name]["num_chunks"] = chunks.size();
        result["databases"][database.name]["transactions"] = json::array();
        for (auto&& transaction :
             databaseServices->transactions(database.name, includeContext, includeLog, transStateSelector)) {
            json transJson = transaction.toJson();
            if (includeContributions) {
                transJson["contrib"] = _getTransactionContributions(
                        transaction, anyTableSelector, anyWorkerSelector, contribStatusSelector,
                        anyChunkSelector, longContribFormat, includeExtensions, includeWarnings,
                        includeRetries);
            }
            result["databases"][database.name]["transactions"].push_back(transJson);
        }
    }
    return result;
}

json HttpIngestTransModule::_getTransaction() {
    debug(__func__);
    checkApiVersion(__func__, 37);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    TransactionId const transactionId = stoul(params().at("id"));
    auto const databaseName = query().optionalString("database");
    auto const tableName = query().optionalString("table");
    auto const workerName = query().optionalString("worker");
    int const chunkSelector = query().optionalInt("chunk", -1);
    auto const includeContributions = query().optionalUInt64("contrib", 0) != 0;
    auto const longContribFormat = query().optionalUInt64("contrib_long", 0) != 0;
    bool const includeContext = query().optionalUInt64("include_context", 0) != 0;
    bool const includeLog = query().optionalUInt64("include_log", 0) != 0;
    bool const includeExtensions = query().optionalUInt64("include_extensions", 0) != 0;
    bool const includeWarnings = query().optionalUInt64("include_warnings", 0) != 0;
    bool const includeRetries = query().optionalUInt64("include_retries", 0) != 0;
    size_t const minRetries = query().optionalUInt64("min_retries", 0);
    size_t const minWarnings = query().optionalUInt64("min_warnings", 0);
    size_t const maxEntries = query().optionalUInt64("max_entries", 0);

    debug(__func__, "id=" + to_string(transactionId));
    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    debug(__func__, "worker=" + workerName);
    debug(__func__, "chunk=" + to_string(chunkSelector));
    debug(__func__, "contrib=" + bool2str(includeContributions));
    debug(__func__, "contrib_long=" + bool2str(longContribFormat));
    debug(__func__, "include_context=" + bool2str(includeContext));
    debug(__func__, "include_log=" + bool2str(includeLog));
    debug(__func__, "include_extensions=" + bool2str(includeExtensions));
    debug(__func__, "include_warnings=" + bool2str(includeWarnings));
    debug(__func__, "include_retries=" + bool2str(includeRetries));
    debug(__func__, "min_retries=" + to_string(minRetries));
    debug(__func__, "min_warnings=" + to_string(minWarnings));
    debug(__func__, "max_entries=" + to_string(maxEntries));

    auto const transStateSelector = _parseTransStateSelector("trans_state");
    auto const contribStatusSelector = _parseContribStatusSelector("contrib_status");

    if (databaseName.empty() && (transactionId == 0)) {
        throw http::Error(__func__, "either 'id' or 'database' query parameter must be specified");
    }

    DatabaseInfo database;
    vector<TransactionInfo> transactions;
    if (transactionId != 0) {
        auto transaction = databaseServices->transaction(transactionId, includeContext, includeLog);
        database = config->databaseInfo(transaction.database);
        if (!databaseName.empty() && (databaseName != database.name)) {
            throw http::Error(__func__, "transaction id=" + to_string(transactionId) +
                                                " is associated with database '" + database.name +
                                                "' which is different from the requested database '" +
                                                databaseName + "'");
        }
        transactions.push_back(move(transaction));
    } else {
        database = config->databaseInfo(databaseName);
        transactions =
                databaseServices->transactions(database.name, includeContext, includeLog, transStateSelector);
    }

    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database.name, allWorkers);

    json result;
    result["databases"][database.name]["is_published"] = database.isPublished ? 1 : 0;
    result["databases"][database.name]["num_chunks"] = chunks.size();
    for (auto&& transaction : transactions) {
        json transJson = transaction.toJson();
        if (includeContributions) {
            transJson["contrib"] = _getTransactionContributions(
                    transaction, tableName, workerName, contribStatusSelector, chunkSelector,
                    longContribFormat, includeExtensions, includeWarnings, includeRetries, minRetries,
                    minWarnings, maxEntries);
        }
        result["databases"][database.name]["transactions"].push_back(transJson);
    }
    return result;
}

json HttpIngestTransModule::_beginTransaction() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    auto const databaseName = body().required<string>("database");
    auto const context = body().optional<json>("context", json::object());

    debug(__func__, "database=" + databaseName);

    auto const database = config->databaseInfo(databaseName);
    if (database.isPublished) {
        throw http::Error(__func__, "the database is already published");
    }

    // Get chunks stats to be reported with the request's result object
    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database.name, allWorkers);

    // Keep the transaction object in this scope to allow logging a status
    // of the operation regardless if it succeeds or fails.
    TransactionInfo transaction;

    // The transient lock on the named mutex will be initialized upon creation of
    // the transaction. This mechanism prevents race conditions in the transaction
    // management operations performed by the module.
    unique_ptr<replica::Lock> lock;

    // Any problems during the "director" index creation will result in
    // failing the transaction.
    try {
        // Upon creation, the transaction will be put into the transitional
        // state IS_STARTING.
        transaction =
                databaseServices->createTransaction(database.name, _transactionMutexRegistry, lock, context);

        // This operation can be vetoed by a catalog ingest workflow at the database
        // registration time.
        if (autoBuildDirectorIndex(database.name)) {
            string const transEvent = "add dir idx part";
            for (auto&& tableName : database.directorTables()) {
                auto const table = database.findTable(tableName);
                if (table.isPublished) continue;
                json transEventData = {{"table", table.name}};
                transaction = databaseServices->updateTransaction(transaction.id, "begin " + transEvent,
                                                                  transEventData);
                transEventData["success"] = 1;
                transEventData["error"] = string();
                try {
                    _addPartitionToDirectorIndex(database, transaction.id, table.name);
                    transaction = databaseServices->updateTransaction(transaction.id, "end " + transEvent,
                                                                      transEventData);
                } catch (exception const& ex) {
                    transEventData["success"] = 0;
                    transEventData["error"] = string(ex.what());
                    transaction = databaseServices->updateTransaction(transaction.id, "end " + transEvent,
                                                                      transEventData);
                    throw;
                }
            }
        }
        transaction = databaseServices->updateTransaction(transaction.id, TransactionInfo::State::STARTED);

        _logTransactionMgtEvent("BEGIN TRANSACTION", "SUCCESS", transaction.id, database.name);

        json result;
        result["databases"][database.name]["transactions"].push_back(transaction.toJson());
        result["databases"][database.name]["num_chunks"] = chunks.size();
        return result;

    } catch (exception const& ex) {
        // The safety check is needed to ensure the transaction object was created
        // and recorded in the database.
        if (transaction.isValid()) {
            transaction =
                    databaseServices->updateTransaction(transaction.id, TransactionInfo::State::START_FAILED);
        }
        _logTransactionMgtEvent("BEGIN TRANSACTION", "FAILED", transaction.id, database.name,
                                "operation failed due to: " + string(ex.what()));
        throw;
    }
}

json HttpIngestTransModule::_endTransaction() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    TransactionId const transactionId = stoul(params().at("id"));
    bool const abort = query().requiredBool("abort");
    const bool hasContext = body().has("context");
    json const context = body().optional<json>("context", json::object());

    debug(__func__, "id=" + to_string(transactionId));
    debug(__func__, "abort=" + to_string(abort ? 1 : 0));

    // The transient lock on the named mutex will be acquired to guarantee exclusive control
    // over transaction states. This mechanism prevents race conditions in the transaction
    // management operations performed by the module.
    string const lockName = "transaction:" + to_string(transactionId);
    debug(__func__, "begin acquiring transient management lock on mutex '" + lockName = "'");
    replica::Lock const lock(_transactionMutexRegistry.get(lockName));
    debug(__func__, "transient management lock on mutex '" + lockName + "' acquired");

    // At this point the transaction state is guaranteed not to be changed by others.
    TransactionInfo transaction = databaseServices->transaction(transactionId);
    auto const operationIsAllowed = TransactionInfo::stateTransitionIsAllowed(
            transaction.state,
            abort ? TransactionInfo::State::IS_ABORTING : TransactionInfo::State::IS_FINISHING);
    if (!operationIsAllowed) {
        throw http::Error(__func__, "transaction id=" + to_string(transactionId) +
                                            " can't be ended at this time"
                                            " because of state=" +
                                            TransactionInfo::state2string(transaction.state) + ".");
    }

    string const databaseName = transaction.database;
    auto const database = config->databaseInfo(databaseName);

    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database.name, allWorkers);

    // Exceptions thrown on operations affecting the persistent state of Qserv
    // or the Replication/Ingest system would result in transitioning the transaction
    // into a failed state ABORT_FAILED or FINISH_FAILED.
    try {
        transaction = databaseServices->updateTransaction(
                transactionId,
                abort ? TransactionInfo::State::IS_ABORTING : TransactionInfo::State::IS_FINISHING);
        if (hasContext) {
            transaction = databaseServices->updateTransaction(transactionId, context);
        }

        bool directorIndexBuildSuccess = false;
        string const noParentJobId;
        if (abort) {
            // Drop the transaction-specific MySQL partition from the relevant tables.
            auto const job = AbortTransactionJob::create(
                    transactionId, allWorkers, controller(), noParentJobId, nullptr,
                    config->get<int>("controller", "ingest-priority-level"));

            chrono::milliseconds const jobMonitoringIval(
                    1000 * config->get<unsigned int>("controller", "ingest-job-monitor-ival-sec"));
            string const transEvent = "del table part";
            json transEventData = {{"job", job->id()}};
            transaction =
                    databaseServices->updateTransaction(transactionId, "begin " + transEvent, transEventData);

            job->start();
            logJobStartedEvent(AbortTransactionJob::typeName(), job, database.family);
            job->wait(jobMonitoringIval, [&](Job::Ptr const& job) {
                auto data = transEventData;
                data["progress"] = job->progress().toJson();
                transaction =
                        databaseServices->updateTransaction(transactionId, "progress " + transEvent, data);
            });
            logJobFinishedEvent(AbortTransactionJob::typeName(), job, database.family);

            bool const success = job->extendedState() == Job::ExtendedState::SUCCESS;
            auto const error = success ? json::object() : job->getResultData().toJson();
            transEventData["success"] = success ? 1 : 0;
            transEventData["error"] = error;
            transaction =
                    databaseServices->updateTransaction(transactionId, "end " + transEvent, transEventData);

            if (!success) throw http::Error(__func__, "failed to drop table partitions", error);

            // This operation in a context of the "director" index table can be vetoed by
            // a catalog ingest workflow at the database registration time.
            if (autoBuildDirectorIndex(database.name)) {
                string const transEvent = "del dir idx part";
                for (auto&& tableName : database.directorTables()) {
                    auto const table = database.findTable(tableName);
                    if (table.isPublished) continue;
                    json transEventData = {{"table", table.name}};
                    transaction = databaseServices->updateTransaction(transactionId, "begin " + transEvent,
                                                                      transEventData);
                    transEventData["success"] = 1;
                    transEventData["error"] = string();
                    try {
                        _removePartitionFromDirectorIndex(database, transactionId, table.name);
                        transaction = databaseServices->updateTransaction(transactionId, "end " + transEvent,
                                                                          transEventData);
                    } catch (exception const& ex) {
                        transEventData["success"] = 0;
                        transEventData["error"] = string(ex.what());
                        transaction = databaseServices->updateTransaction(transactionId, "end " + transEvent,
                                                                          transEventData);
                        throw;
                    }
                }
            }
        } else {
            // Make the best attempt to build a layer at the "director" index if requested
            // by a catalog ingest workflow at the database registration time.
            if (autoBuildDirectorIndex(database.name)) {
                directorIndexBuildSuccess = true;
                chrono::milliseconds const jobMonitoringIval(
                        1000 * config->get<unsigned int>("controller", "ingest-job-monitor-ival-sec"));
                string const transEvent = "bld dir idx";
                for (auto&& tableName : database.directorTables()) {
                    auto const table = database.findTable(tableName);
                    if (table.isPublished) continue;
                    bool const hasTransactions = true;
                    auto const job =
                            DirectorIndexJob::create(database.name, table.name, hasTransactions,
                                                     transactionId, allWorkers, controller(), noParentJobId,
                                                     nullptr,  // no callback
                                                     config->get<int>("controller", "ingest-priority-level"));
                    json transEventData = {{"job", job->id()}, {"table", table.name}};
                    transaction = databaseServices->updateTransaction(transactionId, "begin " + transEvent,
                                                                      transEventData);

                    job->start();
                    logJobStartedEvent(DirectorIndexJob::typeName(), job, database.family);
                    job->wait(jobMonitoringIval, [&](Job::Ptr const& job) {
                        auto data = transEventData;
                        data["progress"] = job->progress().toJson();
                        transaction = databaseServices->updateTransaction(transactionId,
                                                                          "progress " + transEvent, data);
                    });
                    logJobFinishedEvent(DirectorIndexJob::typeName(), job, database.family);
                    directorIndexBuildSuccess =
                            directorIndexBuildSuccess && (job->extendedState() == Job::SUCCESS);

                    transEventData["success"] = job->extendedState() == Job::ExtendedState::SUCCESS ? 1 : 0;
                    transEventData["error"] = job->getResultData().toJson();
                    transaction = databaseServices->updateTransaction(transactionId, "end " + transEvent,
                                                                      transEventData);
                }
            }
        }
        transaction = databaseServices->updateTransaction(
                transactionId, abort ? TransactionInfo::State::ABORTED : TransactionInfo::State::FINISHED);

        _logTransactionMgtEvent(abort ? "ABORT TRANSACTION" : "COMMIT TRANSACTION", "SUCCESS", transactionId,
                                database.name);

        json result;
        result["secondary-index-build-success"] = directorIndexBuildSuccess ? 1 : 0;
        result["databases"][database.name]["num_chunks"] = chunks.size();
        result["databases"][database.name]["transactions"].push_back(transaction.toJson());
        return result;

    } catch (exception const& ex) {
        _logTransactionMgtEvent(abort ? "ABORT TRANSACTION" : "COMMIT TRANSACTION", "FAILED", transactionId,
                                database.name, "operation failed due to: " + string(ex.what()));
        transaction = databaseServices->updateTransaction(
                transactionId,
                abort ? TransactionInfo::State::ABORT_FAILED : TransactionInfo::State::FINISH_FAILED);
        throw;
    }
}

json HttpIngestTransModule::_getContribution() {
    debug(__func__);
    checkApiVersion(__func__, 37);

    unsigned int const id = stoul(params().at("id"));
    bool const includeExtensions = query().optionalUInt64("include_extensions", 1) != 0;
    bool const includeWarnings = query().optionalUInt64("include_warnings", 0) != 0;
    bool const includeRetries = query().optionalUInt64("include_retries", 0) != 0;

    debug(__func__, "id=" + to_string(id));
    debug(__func__, "include_extensions=" + bool2str(includeExtensions));
    debug(__func__, "include_warnings=" + bool2str(includeWarnings));
    debug(__func__, "include_retries=" + bool2str(includeRetries));

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const contrib =
            databaseServices->transactionContrib(id, includeExtensions, includeWarnings, includeRetries);

    json result;
    result["contribution"] = contrib.toJson();
    return result;
}

void HttpIngestTransModule::_logTransactionMgtEvent(string const& operation, string const& status,
                                                    TransactionId transactionId, string const& databaseName,
                                                    string const& msg) const {
    ControllerEvent event;
    event.operation = operation;
    event.status = status;
    event.kvInfo.emplace_back("id", to_string(transactionId));
    event.kvInfo.emplace_back("database", databaseName);
    if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
    logEvent(event);
}

void HttpIngestTransModule::_addPartitionToDirectorIndex(DatabaseInfo const& database,
                                                         TransactionId transactionId,
                                                         string const& directorTableName) const {
    auto const table = database.findTable(directorTableName);
    if (!table.isDirector()) {
        throw logic_error("table '" + table.name + "' is not configured in database '" + database.name +
                          "' as the director table");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    database::mysql::QueryGenerator const g(h.conn);
    bool const ifNotExists = false;
    string const query = g.alterTable(directorIndexTableName(database.name, table.name)) +
                         g.addPartition(transactionId, ifNotExists);
    h.conn->executeInOwnTransaction([&query](decltype(h.conn) conn) { conn->execute(query); });
}

void HttpIngestTransModule::_removePartitionFromDirectorIndex(DatabaseInfo const& database,
                                                              TransactionId transactionId,
                                                              string const& directorTableName) const {
    auto const table = database.findTable(directorTableName);
    if (!table.isDirector()) {
        throw logic_error("table '" + table.name + "' is not configured in database '" + database.name +
                          "' as the director table");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    database::mysql::QueryGenerator const g(h.conn);
    bool const ifExists = true;
    string const query = g.alterTable(directorIndexTableName(database.name, table.name)) +
                         g.dropPartition(transactionId, ifExists);

    // Not having the specified partition is still fine as it couldn't be properly
    // created after the transaction was created.
    try {
        h.conn->executeInOwnTransaction([&query](decltype(h.conn) conn) { conn->execute(query); });
    } catch (database::mysql::ER_DROP_PARTITION_NON_EXISTENT_ const&) {
        ;
    }
}

json HttpIngestTransModule::_getTransactionContributions(
        TransactionInfo const& transaction, string const& tableName, string const& workerName,
        set<TransactionContribInfo::Status> const& contribStatusSelector, int chunkSelector,
        bool longContribFormat, bool includeExtensions, bool includeWarnings, bool includeRetries,
        size_t minRetries, size_t minWarnings, size_t maxEntries) const {
    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    DatabaseInfo const database = config->databaseInfo(transaction.database);

    set<string> uniqueWorkers;
    unsigned int numRegularFiles = 0;
    unsigned int numChunkFiles = 0;
    unsigned int numChunkOverlapFiles = 0;
    float dataSizeGb = 0;
    uint64_t numFailedRetries = 0;
    uint64_t numWarnings = 0;
    uint64_t numRows = 0;
    uint64_t numRowsLoaded = 0;
    uint64_t firstContribBeginTime = numeric_limits<uint64_t>::max();
    uint64_t lastContribEndTime = 0;

    json tableContribJson = json::object();
    json workerContribJson = json::object();
    json transContribFilesJson = json::array();

    json numFilesByStatusJson = json::object();
    for (auto status : TransactionContribInfo::statusCodes()) {
        numFilesByStatusJson[TransactionContribInfo::status2str(status)] = 0;
    }

    // Default selectors for contributions imply pulling all contributions
    // attempted in a scope of the transaction.
    TransactionContribInfo::TypeSelector const anyTypeSelector =
            TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC;

    vector<TransactionContribInfo> const contribs = databaseServices->transactionContribs(
            transaction.id, tableName, workerName, contribStatusSelector, anyTypeSelector, chunkSelector,
            longContribFormat && includeExtensions, longContribFormat && includeWarnings,
            longContribFormat && includeRetries, minRetries, minWarnings, maxEntries);

    for (auto&& contrib : contribs) {
        if (longContribFormat) {
            transContribFilesJson.push_back(contrib.toJson());
        }

        // Count numbers of files in any state
        numFilesByStatusJson[TransactionContribInfo::status2str(contrib.status)] =
                numFilesByStatusJson[TransactionContribInfo::status2str(contrib.status)].get<int>() + 1;

        // Don't count incomplete or non-successful contributions for the summary
        // statistics, except retries as those could be useful for the diagnostic
        // or the monitoring purposes.
        bool const isFinished = contrib.status == TransactionContribInfo::Status::FINISHED;

        uniqueWorkers.insert(contrib.worker);
        float const contribDataSizeGb = contrib.numBytes / GiB;

        // IMPLEMENTATION NOTE: Though the nlohmann JSON API makes a good stride to look
        //   like STL the API implementation still leaves many holes resulting in traps when
        //   relying on the default allocations of the dictionary keys. Hence the code below
        //   addresses this issue by explicitly adding keys to the dictionary where it's needed.
        //   The second issue is related to operator '+=' that's implemented in the library
        //   as container's 'push_back'! The code below avoid using this operator.
        if (!tableContribJson.contains(contrib.table)) {
            tableContribJson[contrib.table] = json::object({{"data_size_gb", 0},
                                                            {"num_failed_retries", 0},
                                                            {"num_warnings", 0},
                                                            {"num_rows", 0},
                                                            {"num_rows_loaded", 0},
                                                            {"num_files", 0}});
        }
        if (!workerContribJson.contains(contrib.worker)) {
            workerContribJson[contrib.worker] = json::object({{"data_size_gb", 0},
                                                              {"num_failed_retries", 0},
                                                              {"num_warnings", 0},
                                                              {"num_rows", 0},
                                                              {"num_rows_loaded", 0},
                                                              {"num_chunk_overlap_files", 0},
                                                              {"num_chunk_files", 0},
                                                              {"num_regular_files", 0}});
        }
        json& objWorker = workerContribJson[contrib.worker];
        auto const table = database.findTable(contrib.table);
        if (table.isPartitioned) {
            if (contrib.isOverlap) {
                if (!tableContribJson[contrib.table].contains("overlap")) {
                    tableContribJson[contrib.table]["overlap"] = json::object({{"data_size_gb", 0},
                                                                               {"num_failed_retries", 0},
                                                                               {"num_warnings", 0},
                                                                               {"num_rows", 0},
                                                                               {"num_rows_loaded", 0},
                                                                               {"num_files", 0}});
                }
                json& objTable = tableContribJson[contrib.table]["overlap"];
                if (isFinished) {
                    incrementBy<float>(objTable, "data_size_gb", contribDataSizeGb);
                    incrementBy<uint64_t>(objTable, "num_warnings", contrib.numWarnings);
                    incrementBy<uint64_t>(objTable, "num_rows", contrib.numRows);
                    incrementBy<uint64_t>(objTable, "num_rows_loaded", contrib.numRowsLoaded);
                    incrementBy<unsigned int>(objTable, "num_files", 1);
                    incrementBy<unsigned int>(objWorker, "num_chunk_overlap_files", 1);
                    numChunkOverlapFiles++;
                }
                incrementBy<uint64_t>(objTable, "num_failed_retries", contrib.numFailedRetries);
            } else {
                json& objTable = tableContribJson[contrib.table];
                if (isFinished) {
                    incrementBy<float>(objTable, "data_size_gb", contribDataSizeGb);
                    incrementBy<uint64_t>(objTable, "num_warnings", contrib.numWarnings);
                    incrementBy<uint64_t>(objTable, "num_rows", contrib.numRows);
                    incrementBy<uint64_t>(objTable, "num_rows_loaded", contrib.numRowsLoaded);
                    incrementBy<unsigned int>(objTable, "num_files", 1);
                    incrementBy<unsigned int>(objWorker, "num_chunk_files", 1);
                    numChunkFiles++;
                }
                incrementBy<uint64_t>(objTable, "num_failed_retries", contrib.numFailedRetries);
            }
        } else {
            json& objTable = tableContribJson[contrib.table];
            if (isFinished) {
                incrementBy<float>(objTable, "data_size_gb", contribDataSizeGb);
                incrementBy<uint64_t>(objTable, "num_failed_retries", contrib.numFailedRetries);
                incrementBy<uint64_t>(objTable, "num_warnings", contrib.numWarnings);
                incrementBy<uint64_t>(objTable, "num_rows", contrib.numRows);
                incrementBy<uint64_t>(objTable, "num_rows_loaded", contrib.numRowsLoaded);
                incrementBy<unsigned int>(objTable, "num_files", 1);
                incrementBy<unsigned int>(objWorker, "num_regular_files", 1);
                numRegularFiles++;
            }
            incrementBy<uint64_t>(objTable, "num_failed_retries", contrib.numFailedRetries);
        }
        if (isFinished) {
            dataSizeGb += contribDataSizeGb;
            incrementBy<float>(objWorker, "data_size_gb", contribDataSizeGb);

            numWarnings += contrib.numWarnings;
            incrementBy<uint64_t>(objWorker, "num_warnings", contrib.numWarnings);

            numRows += contrib.numRows;
            incrementBy<uint64_t>(objWorker, "num_rows", contrib.numRows);

            numRowsLoaded += contrib.numRowsLoaded;
            incrementBy<uint64_t>(objWorker, "num_rows_loaded", contrib.numRowsLoaded);

            firstContribBeginTime = min(firstContribBeginTime, contrib.createTime);
            lastContribEndTime = max(lastContribEndTime, contrib.loadTime);
        }
        numFailedRetries += contrib.numFailedRetries;
        incrementBy<uint64_t>(objWorker, "num_failed_retries", contrib.numFailedRetries);
    }
    json resultJson;
    resultJson["summary"] = json::object(
            {{"num_workers", uniqueWorkers.size()},
             {"num_files_by_status", numFilesByStatusJson},
             {"num_regular_files", numRegularFiles},
             {"num_chunk_files", numChunkFiles},
             {"num_chunk_overlap_files", numChunkOverlapFiles},
             {"data_size_gb", dataSizeGb},
             {"num_failed_retries", numFailedRetries},
             {"num_warnings", numWarnings},
             {"num_rows", numRows},
             {"num_rows_loaded", numRowsLoaded},
             // Force 0 if no contribution has been made
             {"first_contrib_begin",
              firstContribBeginTime == numeric_limits<uint64_t>::max() ? 0 : firstContribBeginTime},
             // Will be 0 if none of the contributions has finished yet, or all have failed.
             {"last_contrib_end", lastContribEndTime},
             {"table", tableContribJson},
             {"worker", workerContribJson}});
    resultJson["files"] = transContribFilesJson;
    return resultJson;
}

set<TransactionInfo::State> HttpIngestTransModule::_parseTransStateSelector(string const& param) const {
    set<TransactionInfo::State> result;
    auto const stateStr = query().optionalString(param);
    debug(__func__, param + "=" + stateStr);
    if (stateStr == "!STARTED") {
        result = TransactionInfo::allStates;
        result.erase(TransactionInfo::State::STARTED);
    } else if (stateStr == "!FINISHED") {
        result = TransactionInfo::allStates;
        result.erase(TransactionInfo::State::FINISHED);
    } else {
        bool const skipEmpty = true;
        for (auto const& str : util::String::split(stateStr, ",", skipEmpty)) {
            result.insert(TransactionInfo::string2state(str));
        }
    }
    return result;
}

set<TransactionContribInfo::Status> HttpIngestTransModule::_parseContribStatusSelector(
        string const& param) const {
    set<TransactionContribInfo::Status> result;
    auto const statusStr = query().optionalString(param);
    debug(__func__, param + "=" + statusStr);
    if (statusStr == "!IN_PROGRESS") {
        result = TransactionContribInfo::allStatuses;
        result.erase(TransactionContribInfo::Status::IN_PROGRESS);
    } else if (statusStr == "!FINISHED") {
        result = TransactionContribInfo::allStatuses;
        result.erase(TransactionContribInfo::Status::FINISHED);
    } else {
        bool const skipEmpty = true;
        for (auto const& str : util::String::split(statusStr, ",", skipEmpty)) {
            result.insert(TransactionContribInfo::str2status(str));
        }
    }
    return result;
}

}  // namespace lsst::qserv::replica
