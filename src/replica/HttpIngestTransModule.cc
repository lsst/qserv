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
#include <unordered_map>

// Qserv headers
#include "replica/AbortTransactionJob.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/HttpExceptions.h"
#include "replica/IndexJob.h"
#include "replica/NamedMutexRegistry.h"
#include "replica/ServiceProvider.h"
#include "util/Mutex.h"

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
}

namespace lsst {
namespace qserv {
namespace replica {


void HttpIngestTransModule::process(Controller::Ptr const& controller,
                                    NamedMutexRegistry& transactionMutexRegistry,
                                    string const& taskName,
                                    HttpProcessorConfig const& processorConfig,
                                    qhttp::Request::Ptr const& req,
                                    qhttp::Response::Ptr const& resp,
                                    string const& subModuleName,
                                    HttpAuthType const authType) {
    HttpIngestTransModule module(controller, transactionMutexRegistry, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpIngestTransModule::HttpIngestTransModule(
            Controller::Ptr const& controller,
            NamedMutexRegistry& transactionMutexRegistry,
            string const& taskName,
            HttpProcessorConfig const& processorConfig,
            qhttp::Request::Ptr const& req,
            qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp),
        _transactionMutexRegistry(transactionMutexRegistry) {
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
    auto const includeContributions = query().optionalUInt64("contrib", 0) != 0;
    auto const longContribFormat = query().optionalUInt64("contrib_long", 0) != 0;
    bool const includeContext = query().optionalUInt64("include_context", 0) != 0;
    bool const includeLog = query().optionalUInt64("include_log", 0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "family=" + family);
    debug(__func__, "all_databases=" + bool2str(allDatabases));
    debug(__func__, "is_published=" + bool2str(isPublished));
    debug(__func__, "contrib=" + bool2str(includeContributions));
    debug(__func__, "contrib_long=" + bool2str(longContribFormat));
    debug(__func__, "include_context=" + bool2str(includeContext));
    debug(__func__, "include_log=" + bool2str(includeLog));

    vector<string> databases;
    if (database.empty()) {
        databases = config->databases(family, allDatabases, isPublished);
    } else {
        databases.push_back(database);
    }

    bool const allWorkers = true;
    json result;
    result["databases"] = json::object();
    for (auto&& database: databases) {
        auto const databaseInfo = config->databaseInfo(database);
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database, allWorkers);

        result["databases"][database]["is_published"] = databaseInfo.isPublished ? 1 : 0;
        result["databases"][database]["num_chunks"] = chunks.size();
        result["databases"][database]["transactions"] = json::array();
        for (auto&& transaction: databaseServices->transactions(database, includeContext, includeLog)) {
            json transJson = transaction.toJson();
            if (includeContributions) {
                transJson["contrib"] =
                    _getTransactionContributions(transaction, longContribFormat);
            }
            result["databases"][database]["transactions"].push_back(transJson);
        }
    }
    return result;
}


json HttpIngestTransModule::_getTransaction() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const id = stoul(params().at("id"));
    auto const includeContributions = query().optionalUInt64("contrib", 0) != 0;
    auto const longContribFormat = query().optionalUInt64("contrib_long", 0) != 0;
    bool const includeContext = query().optionalUInt64("include_context", 0) != 0;
    bool const includeLog = query().optionalUInt64("include_log", 0) != 0;

    debug(__func__, "id=" + to_string(id));
    debug(__func__, "contrib=" + bool2str(includeContributions));
    debug(__func__, "contrib_long=" + bool2str(longContribFormat));
    debug(__func__, "include_context=" + bool2str(includeContext));
    debug(__func__, "include_log=" + bool2str(includeLog));

    auto const transaction = databaseServices->transaction(id, includeContext, includeLog);
    auto const databaseInfo = config->databaseInfo(transaction.database);
    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, transaction.database, allWorkers);

    json transJson = transaction.toJson();
    if (includeContributions) {
        transJson["contrib"] =
            _getTransactionContributions(transaction, longContribFormat);
    }
    json result;
    result["databases"][transaction.database]["is_published"] = databaseInfo.isPublished ? 1 : 0;
    result["databases"][transaction.database]["num_chunks"] = chunks.size();
    result["databases"][transaction.database]["transactions"].push_back(transJson);
    return result;
}


json HttpIngestTransModule::_beginTransaction() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    auto const database = body().required<string>("database");
    auto const context = body().optional<json>("context", json::object());

    debug(__func__, "database=" + database);

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
        throw HttpError(__func__, "the database is already published");
    }

    // Get chunks stats to be reported with the request's result object
    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database, allWorkers);

    // Keep the transaction object in this scope to allow logging a status
    // of the operation regardless if it succeeds or fails.
    TransactionInfo transaction;

    // The transient lock on the named mutex will be initialized upon creation of
    // the transaction. This mechanism prevents race conditions in the transaction
    // management operations performed by the module.
    unique_ptr<util::Lock> lock;

    // Any problems during the secondary index creation will result in
    // failing the transaction.
    try {
        // Upon creation, the transaction will be put into the transitional
        // state IS_STARTING.
        transaction = databaseServices->createTransaction(database, _transactionMutexRegistry, lock, context);

        // This operation can be vetoed by a catalog ingest workflow at the database
        // registration time.
        if (autoBuildSecondaryIndex(database)) {
            string const transEvent = "add dir idx part";
            for (auto&& directorTable: databaseInfo.directorTables()) {
                json transEventData = {{"table", directorTable}};
                transaction = databaseServices->updateTransaction(transaction.id, "begin " + transEvent, transEventData);
                transEventData["success"] = 1;
                transEventData["error"] = string();
                try {
                    _addPartitionToSecondaryIndex(databaseInfo, transaction.id, directorTable);
                    transaction = databaseServices->updateTransaction(transaction.id, "end " + transEvent, transEventData);
                } catch (exception const& ex) {
                    transEventData["success"] = 0;
                    transEventData["error"] = string(ex.what());
                    transaction = databaseServices->updateTransaction(transaction.id, "end " + transEvent, transEventData);
                    throw;
                }
            }
        }
        transaction = databaseServices->updateTransaction(transaction.id, TransactionInfo::State::STARTED);

        _logTransactionMgtEvent("BEGIN TRANSACTION", "SUCCESS", transaction.id, database);

        json result;
        result["databases"][database]["transactions"].push_back(transaction.toJson());
        result["databases"][database]["num_chunks"] = chunks.size();
        return result;

    } catch (exception const& ex) {
        // The safety check is needed to ensure the transaction object was created
        // and recorded in the database.
        if (transaction.isValid()) {
            transaction = databaseServices->updateTransaction(transaction.id, TransactionInfo::State::START_FAILED);
        }
        _logTransactionMgtEvent("BEGIN TRANSACTION", "FAILED", transaction.id, database,
                                "operation failed due to: " + string(ex.what()));
        throw;
    }
}


json HttpIngestTransModule::_endTransaction() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    TransactionId const id = stoul(params().at("id"));
    bool const abort = query().requiredBool("abort");
    const bool hasContext = body().has("context");
    json const context = body().optional<json>("context", json::object());

    debug(__func__, "id="    + to_string(id));
    debug(__func__, "abort=" + to_string(abort ? 1 : 0));

    // The transient lock on the named mutex will be acquired to guarantee exclusive control
    // over transaction states. This mechanism prevents race conditions in the transaction
    // management operations performed by the module.
    string const lockName = "transaction:" + to_string(id);
    debug(__func__, "begin acquiring transient management lock on mutex '" + lockName = "'");
    util::Lock const lock(_transactionMutexRegistry.get(lockName));
    debug(__func__, "transient management lock on mutex '" + lockName + "' acquired");

    // At this point the transaction state is guaranteed not to be changed by others.
    TransactionInfo transaction = databaseServices->transaction(id);
    auto const operationIsAllowed = TransactionInfo::stateTransitionIsAllowed(transaction.state, abort ?
            TransactionInfo::State::IS_ABORTING : TransactionInfo::State::IS_FINISHING);
    if (!operationIsAllowed) {
        throw HttpError(
                __func__, "transaction id=" + to_string(id) + " can't be ended at this time"
                " because of state=" + TransactionInfo::state2string(transaction.state) + ".");
    }

    string const database = transaction.database;
    auto const databaseInfo = config->databaseInfo(database);

    bool const allWorkers = true;
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database, allWorkers);

    // Exceptions thrown on operations affecting the persistent state of Qserv
    // or the Replication/Ingest system would result in transitioning the transaction
    // into a failed state ABORT_FAILED or FINISH_FAILED.
    try {
        transaction = databaseServices->updateTransaction(id, abort ?
                TransactionInfo::State::IS_ABORTING : TransactionInfo::State::IS_FINISHING);
        if (hasContext) {
            transaction = databaseServices->updateTransaction(id, context);
        }

        bool secondaryIndexBuildSuccess = false;
        string const noParentJobId;
        if (abort) {
            // Drop the transaction-specific MySQL partition from the relevant tables.
            auto const job = AbortTransactionJob::create(
                    id, allWorkers, controller(), noParentJobId, nullptr,
                    config->get<int>("controller", "ingest-priority-level"));

            chrono::milliseconds const jobMonitoringIval(1000 * config->get<unsigned int>("controller", "ingest-job-monitor-ival-sec"));
            string const transEvent = "del table part";
            json transEventData = {{"job", job->id()}};
            transaction = databaseServices->updateTransaction(id, "begin " + transEvent, transEventData);

            job->start();
            logJobStartedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);
            job->wait(jobMonitoringIval, [&](Job::Ptr const& job) {
                auto data = transEventData;
                data["progress"] = job->progress().toJson();
                transaction = databaseServices->updateTransaction(id, "progress " + transEvent, data);
            });
            logJobFinishedEvent(AbortTransactionJob::typeName(), job, databaseInfo.family);

            bool const success = job->extendedState() == Job::ExtendedState::SUCCESS;
            auto const error = success ? json::object() : job->getResultData().toJson();
            transEventData["success"] = success ? 1 : 0;
            transEventData["error"] = error;
            transaction = databaseServices->updateTransaction(id, "end " + transEvent, transEventData);

            if (!success) throw HttpError(__func__, "failed to drop table partitions", error);

            // This operation in a context of the "secondary index" table can be vetoed by
            // a catalog ingest workflow at the database registration time.
            if (autoBuildSecondaryIndex(database)) {
                string const transEvent = "del dir idx part";
                for (auto&& directorTable: databaseInfo.directorTables()) {;
                    json transEventData = {{"table", directorTable}};
                    transaction = databaseServices->updateTransaction(id, "begin " + transEvent, transEventData);
                    transEventData["success"] = 1;
                    transEventData["error"] = string();
                    try {
                        _removePartitionFromSecondaryIndex(databaseInfo, id, directorTable);
                        transaction = databaseServices->updateTransaction(id, "end " + transEvent, transEventData);
                    } catch (exception const& ex) {
                        transEventData["success"] = 0;
                        transEventData["error"] = string(ex.what());
                        transaction = databaseServices->updateTransaction(id, "end " + transEvent, transEventData);
                        throw;
                    }
                }
            }
        } else {
            // Make the best attempt to build a layer at the "secondary index" if requested
            // by a catalog ingest workflow at the database registration time.
            if (autoBuildSecondaryIndex(database)) {
                secondaryIndexBuildSuccess = true;
                chrono::milliseconds const jobMonitoringIval(1000 * config->get<unsigned int>("controller", "ingest-job-monitor-ival-sec"));
                string const transEvent = "bld dir idx";
                for (auto&& directorTable: databaseInfo.directorTables()) {
                    bool const hasTransactions = true;
                    string const destinationPath = database + "__" + directorTable;
                    auto const job = IndexJob::create(
                        database,
                        directorTable,
                        hasTransactions,
                        id,
                        allWorkers,
                        IndexJob::TABLE,
                        destinationPath,
                        localLoadSecondaryIndex(database),
                        controller(),
                        noParentJobId,
                        nullptr,        // no callback
                        config->get<int>("controller", "ingest-priority-level")
                    );
                    json transEventData = {{"job", job->id()}, {"table", directorTable}};
                    transaction = databaseServices->updateTransaction(id, "begin " + transEvent, transEventData);

                    job->start();
                    logJobStartedEvent(IndexJob::typeName(), job, databaseInfo.family);
                    job->wait(jobMonitoringIval, [&](Job::Ptr const& job) {
                        auto data = transEventData;
                        data["progress"] = job->progress().toJson();
                        transaction = databaseServices->updateTransaction(id, "progress " + transEvent, data);
                    });
                    logJobFinishedEvent(IndexJob::typeName(), job, databaseInfo.family);
                    secondaryIndexBuildSuccess = secondaryIndexBuildSuccess && (job->extendedState() == Job::SUCCESS);

                    transEventData["success"] = job->extendedState() == Job::ExtendedState::SUCCESS ? 1 : 0;
                    transEventData["error"] = job->getResultData().toJson();
                    transaction = databaseServices->updateTransaction(id, "end " + transEvent, transEventData);
                }
            }
        }
        transaction = databaseServices->updateTransaction(id, abort ?
                TransactionInfo::State::ABORTED : TransactionInfo::State::FINISHED);

        _logTransactionMgtEvent(abort ? "ABORT TRANSACTION" : "COMMIT TRANSACTION",
                "SUCCESS", id, database);

        json result;
        result["secondary-index-build-success"] = secondaryIndexBuildSuccess ? 1 : 0;
        result["databases"][database]["num_chunks"] = chunks.size();
        result["databases"][database]["transactions"].push_back(transaction.toJson());
        return result;

    } catch (exception const& ex) {
        _logTransactionMgtEvent(abort ? "ABORT TRANSACTION" : "COMMIT TRANSACTION",
                "FAILED", id, database,
                "operation failed due to: " + string(ex.what()));
        transaction = databaseServices->updateTransaction(id, abort ?
                TransactionInfo::State::ABORT_FAILED : TransactionInfo::State::FINISH_FAILED);
        throw;
    }
}

void HttpIngestTransModule::_logTransactionMgtEvent(
        string const& operation, string const& status, TransactionId id,
        string const& database, string const& msg) const {
    ControllerEvent event;
    event.operation = operation;
    event.status = status;
    event.kvInfo.emplace_back("id", to_string(id));
    event.kvInfo.emplace_back("database", database);
    if (not msg.empty()) event.kvInfo.emplace_back("error", msg);
    logEvent(event);
}


void HttpIngestTransModule::_addPartitionToSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                          TransactionId transactionId,
                                                          string const& directorTable) const {
    if (!databaseInfo.isDirector(directorTable)) {
        throw logic_error(
                "table '" + directorTable + "' is not configured in database '" +
                databaseInfo.name + "' as the director table");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + directorTable) +
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
                                                               TransactionId transactionId,
                                                               string const& directorTable) const {
    if (!databaseInfo.isDirector(directorTable)) {
        throw logic_error(
                "table '" + directorTable + "' is not configured in database '" +
                databaseInfo.name + "' as the director table");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    string const query =
        "ALTER TABLE " + h.conn->sqlId(databaseInfo.name + "__" + directorTable) +
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
    } catch (database::mysql::ER_DROP_PARTITION_NON_EXISTENT_ const&) {
        ;
    }
}


json HttpIngestTransModule::_getTransactionContributions(TransactionInfo const& transactionInfo,
                                                         bool longContribFormat) const {
    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    DatabaseInfo const databaseInfo = config->databaseInfo(transactionInfo.database);

    set<string> uniqueWorkers;
    unsigned int numRegularFiles = 0;
    unsigned int numChunkFiles = 0;
    unsigned int numChunkOverlapFiles = 0;
    float dataSizeGb = 0;
    uint64_t numRows = 0;
    uint64_t firstContribBeginTime = numeric_limits<uint64_t>::max();
    uint64_t lastContribEndTime = 0;

    json tableContribJson = json::object();
    json workerContribJson = json::object();
    json transContribFilesJson = json::array();

    json numFilesByStatusJson = json::object();
    for (auto status: TransactionContribInfo::statusCodes()) {
        numFilesByStatusJson[TransactionContribInfo::status2str(status)] = 0;
    }

    for (auto&& contrib: databaseServices->transactionContribs(transactionInfo.id)) {

        // Always include detailed into on the contributions
        if (longContribFormat) transContribFilesJson.push_back(contrib.toJson());

        // Count numbers of files in any state
        numFilesByStatusJson[TransactionContribInfo::status2str(contrib.status)] =
                numFilesByStatusJson[TransactionContribInfo::status2str(contrib.status)].get<int>() + 1;

        // Don't count incomplete or non-successful contributions for the summary statistics.
        if (contrib.status != TransactionContribInfo::Status::FINISHED) {
            continue;
        }
        uniqueWorkers.insert(contrib.worker);
        float const contribDataSizeGb = contrib.numBytes / GiB;

        // IMPLEMENTATION NOTE: Though the nlohhmann JSON API makes a good stride to look
        //   like STL the API implementation still leaves many holes resulting in traps when
        //   relying on the default allocations of the dictionary keys. Hence the code below
        //   addresses this issue by explicitly adding keys to the dictionary where it's needed.
        //   The second issue is related to operator '+=' that's implemented in the library
        //   as container's 'push_back'! The code below avoid using this operator.
        if (!tableContribJson.contains(contrib.table)) {
            tableContribJson[contrib.table] = json::object({
                {"data_size_gb", 0},
                {"num_rows", 0},
                {"num_files", 0}
            });
        }
        if (!workerContribJson.contains(contrib.worker)) {
            workerContribJson[contrib.worker] = json::object({
                {"data_size_gb", 0},
                {"num_rows", 0},
                {"num_chunk_overlap_files", 0},
                {"num_chunk_files", 0},
                {"num_regular_files", 0}
            });
        }
        json& objWorker = workerContribJson[contrib.worker];
        if (databaseInfo.isPartitioned(contrib.table)) {
            if (contrib.isOverlap) {
                if (!tableContribJson[contrib.table].contains("overlap")) {
                    tableContribJson[contrib.table]["overlap"] = json::object({
                        {"data_size_gb", 0},
                        {"num_rows", 0},
                        {"num_files", 0}
                    });
                }
                json& objTable = tableContribJson[contrib.table]["overlap"];
                incrementBy<float>(objTable, "data_size_gb", contribDataSizeGb);
                incrementBy<unsigned int>(objTable, "num_rows", contrib.numRows);
                incrementBy<unsigned int>(objTable, "num_files", 1);
                incrementBy<unsigned int>(objWorker, "num_chunk_overlap_files", 1);
                numChunkOverlapFiles++;
            } else {
                json& objTable = tableContribJson[contrib.table];
                incrementBy<float>(objTable, "data_size_gb", contribDataSizeGb);
                incrementBy<unsigned int>(objTable, "num_rows", contrib.numRows);
                incrementBy<unsigned int>(objTable, "num_files", 1);
                incrementBy<unsigned int>(objWorker, "num_chunk_files", 1);
                numChunkFiles++;
            }
        } else {
            json& objTable = tableContribJson[contrib.table];
            incrementBy<float>(objTable, "data_size_gb", contribDataSizeGb);
            incrementBy<unsigned int>(objTable, "num_rows", contrib.numRows);
            incrementBy<unsigned int>(objTable, "num_files", 1);
            incrementBy<unsigned int>(objWorker, "num_regular_files", 1);
            numRegularFiles++;
        }
        dataSizeGb += contribDataSizeGb;
        incrementBy<float>(objWorker, "data_size_gb", contribDataSizeGb);

        numRows += contrib.numRows;
        incrementBy<unsigned int>(objWorker, "num_rows", contrib.numRows);

        firstContribBeginTime = min(firstContribBeginTime, contrib.createTime);
        lastContribEndTime = max(lastContribEndTime, contrib.loadTime);
    }
    json resultJson;
    resultJson["summary"] = json::object({
        {"num_workers", uniqueWorkers.size()},
        {"num_files_by_status", numFilesByStatusJson},
        {"num_regular_files", numRegularFiles},
        {"num_chunk_files", numChunkFiles},
        {"num_chunk_overlap_files", numChunkOverlapFiles},
        {"data_size_gb", dataSizeGb},
        {"num_rows", numRows},
        // Force 0 if no contribution has been made
        {"first_contrib_begin", firstContribBeginTime == numeric_limits<uint64_t>::max() ?
                0 : firstContribBeginTime},
        // Will be 0 if none of the contributions has finished yet, or all have failed.
        {"last_contrib_end", lastContribEndTime},
        {"table", tableContribJson},
        {"worker", workerContribJson}
    });
    resultJson["files"] = transContribFilesJson;
    return resultJson;
}


}}}  // namespace lsst::qserv::replica
