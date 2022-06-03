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
#include "replica/HttpIngestModule.h"

// System headers
#include <algorithm>
#include <limits>
#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "css/CssAccess.h"
#include "css/CssError.h"
#include "css/DbInterfaceMySql.h"
#include "global/constants.h"
#include "replica/ChunkedTable.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/FindAllJob.h"
#include "replica/HttpExceptions.h"
#include "replica/QservSyncJob.h"
#include "replica/ReplicaInfo.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlCreateDbJob.h"
#include "replica/SqlCreateTableJob.h"
#include "replica/SqlCreateTablesJob.h"
#include "replica/SqlDeleteDbJob.h"
#include "replica/SqlDeleteTableJob.h"
#include "replica/SqlDisableDbJob.h"
#include "replica/SqlGrantAccessJob.h"
#include "replica/SqlEnableDbJob.h"
#include "replica/SqlRemoveTablePartitionsJob.h"
#include "replica/SqlResultSet.h"

// LSST headers
#include "lsst/sphgeom/Chunker.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace fs = boost::filesystem;

namespace {

string jobCompletionErrorIfAny(SqlJob::Ptr const& job, string const& prefix) {
    string error;
    switch (job->extendedState()) {
        case Job::ExtendedState::NONE:
        case Job::ExtendedState::SUCCESS:
            break;
        case Job::ExtendedState::FAILED: {
            auto const& resultData = job->getResultData();
            for (auto&& itr : resultData.resultSets) {
                auto&& worker = itr.first;
                for (auto&& result : itr.second) {
                    if (result.hasErrors()) {
                        error += prefix + ", worker: " + worker + ",  error: " + result.firstError() + " ";
                    }
                }
            }
            break;
        }
        default:
            // Job expiration, cancellation and other problems are reported here.
            error += prefix + "failed, job: " + job->id() + ", extended state: " + Job::state2string(job->extendedState());
            break;
    }

    return error;
}

/**
 * @brief Generate the name of a metadata table at czar for the specified data table.
 * @param database The name of a database where the data table is residing.
 * @param table The name of the data table.
 * @param suffix The optional suffix for the metadata table.
 * @return std::string The name of the metadata table at czar.
 * @throws invalid_argument If the length of the resulting name exceeds the MySQL limit.
 */
string tableNameBuilder(string const& database, string const& table, string const& suffix = string()) {
    size_t const tableNameLimit = 64;
    string const name = database + "__" + table + suffix;
    if (name.size() > tableNameLimit) {
        throw invalid_argument("HttpIngestModule::" + string(__func__) + " MySQL table name limit of " +
                               to_string(tableNameLimit) + " characters has been exceeded for table '" +
                               name + "'.");
    }
    return name;
}

/**
 * @return The name of a table at czar that stores indexes of the specified director table.
 * @see tableNameBuilder
 */
string directorIndexTable(string const& database, string const& table) {
    return tableNameBuilder(database, table);
}

/**
 * @return The name of a table at czar that stores table row counters of the specified data table.
 * @see tableNameBuilder
 */
string rowCountersTable(string const& database, string const& table) {
    return tableNameBuilder(database, table, "__rows");
}
}  // namespace

namespace lsst::qserv::replica {

string const HttpIngestModule::_partitionByColumn = "qserv_trans_id";
string const HttpIngestModule::_partitionByColumnType = "INT NOT NULL";

void HttpIngestModule::process(Controller::Ptr const& controller, string const& taskName,
                               HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp, string const& subModuleName,
                               HttpAuthType const authType) {
    HttpIngestModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpIngestModule::HttpIngestModule(Controller::Ptr const& controller, string const& taskName,
                                   HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpIngestModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "DATABASES")
        return _getDatabases();
    else if (subModuleName == "ADD-DATABASE")
        return _addDatabase();
    else if (subModuleName == "PUBLISH-DATABASE")
        return _publishDatabase();
    else if (subModuleName == "DELETE-DATABASE")
        return _deleteDatabase();
    else if (subModuleName == "TABLES")
        return _getTables();
    else if (subModuleName == "ADD-TABLE")
        return _addTable();
    else if (subModuleName == "DELETE-TABLE")
        return _deleteTable();
    else if (subModuleName == "SCAN-TABLE-STATS")
        return _scanTableStats();
    else if (subModuleName == "DELETE-TABLE-STATS")
        return _deleteTableStats();
    else if (subModuleName == "TABLE-STATS")
        return _tableStats();
    else if (subModuleName == "BUILD-CHUNK-LIST")
        return _buildEmptyChunksList();
    else if (subModuleName == "REGULAR")
        return _getRegular();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpIngestModule::_getDatabases() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    // Leaving this name empty would result in scanning databases across all known
    // families (instead of a single one) while applying the optional filter on
    // the publishing status of each candidate.
    //
    // Note that filters "family" and "publishing status" are orthogonal in
    // the current implementation of a method fetching the requested names of
    // databases from the system's configuration.
    string const family = body().optional<string>("family", string());

    bool const allDatabases = body().optional<unsigned int>("all", 1) != 0;

    // This parameter is used only if a subset of databases specified in the optional
    // flag "all" was requested. Should this be the case, a client will be required
    // to resolve the ambiguity.
    bool isPublished = false;
    if (!allDatabases) {
        isPublished = body().required<unsigned int>("published") != 0;
    }

    debug(__func__, "family=" + family);
    debug(__func__, "allDatabases=" + bool2str(allDatabases));
    debug(__func__, "isPublished=" + bool2str(isPublished));

    json databasesJson = json::array();
    for (auto const database : config->databases(family, allDatabases, isPublished)) {
        auto const databaseInfo = config->databaseInfo(database);
        databasesJson.push_back({{"name", databaseInfo.name},
                                 {"family", databaseInfo.family},
                                 {"is_published", databaseInfo.isPublished ? 1 : 0}});
    }
    return json::object({{"databases", databasesJson}});
}

json HttpIngestModule::_addDatabase() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    DatabaseInfo databaseInfo;
    string const database = body().required<string>("database");

    auto const numStripes = body().required<unsigned int>("num_stripes");
    auto const numSubStripes = body().required<unsigned int>("num_sub_stripes");
    auto const overlap = body().required<double>("overlap");
    auto const enableAutoBuildSecondaryIndex = body().optional<unsigned int>("auto_build_secondary_index", 1);
    auto const enableLocalLoadSecondaryIndex = body().optional<unsigned int>("local_load_secondary_index", 0);

    debug(__func__, "database=" + database);
    debug(__func__, "numStripes=" + to_string(numStripes));
    debug(__func__, "numSubStripes=" + to_string(numSubStripes));
    debug(__func__, "overlap=" + to_string(overlap));
    debug(__func__, "enableAutoBuildSecondaryIndex=" + to_string(enableAutoBuildSecondaryIndex ? 1 : 0));
    debug(__func__, "enableLocalLoadSecondaryIndex=" + to_string(enableLocalLoadSecondaryIndex ? 1 : 0));

    if (overlap < 0) throw HttpError(__func__, "overlap can't have a negative value");

    // Find an appropriate database family for the database. If none
    // found then create a new one named after the database.

    string family;
    for (auto&& candidateFamily : config->databaseFamilies()) {
        auto const familyInfo = config->databaseFamilyInfo(candidateFamily);
        if ((familyInfo.numStripes == numStripes) and (familyInfo.numSubStripes == numSubStripes) and
            (abs(familyInfo.overlap - overlap) <= numeric_limits<double>::epsilon())) {
            family = candidateFamily;
        }
    }
    if (family.empty()) {
        // When creating the family use partitioning attributes as the name of the family
        // as shown below:
        //
        //   layout_<numStripes>_<numSubStripes>

        family = "layout_" + to_string(numStripes) + "_" + to_string(numSubStripes);
        DatabaseFamilyInfo familyInfo;
        familyInfo.name = family;
        familyInfo.replicationLevel = 1;
        familyInfo.numStripes = numStripes;
        familyInfo.numSubStripes = numSubStripes;
        familyInfo.overlap = overlap;
        config->addDatabaseFamily(familyInfo);
    }

    // Create the database at all QServ workers

    bool const allWorkers = true;
    string const noParentJobId;
    auto const job = SqlCreateDbJob::create(database, allWorkers, controller(), noParentJobId, nullptr,
                                            config->get<int>("controller", "ingest-priority-level"));
    job->start();
    logJobStartedEvent(SqlCreateDbJob::typeName(), job, family);
    job->wait();
    logJobFinishedEvent(SqlCreateDbJob::typeName(), job, family);

    string error = ::jobCompletionErrorIfAny(job, "database creation failed");
    if (not error.empty()) throw HttpError(__func__, error);

    // Register the new database in the Configuration.
    // Note, this operation will fail if the database with the name
    // already exists. Also, the new database won't have any tables
    // until they will be added as a separate step.
    databaseInfo = config->addDatabase(database, family);

    // Register a requested mode for building the secondary index. If a value
    // of the parameter is set to 'true' (or '1' in the database) then contributions
    // into the index will be automatically made when committing transactions. Otherwise,
    // it's going to be up to a user's catalog ingest workflow to (re-)build
    // the index.
    databaseServices->saveIngestParam(databaseInfo.name, "secondary-index", "auto-build",
                                      to_string(enableAutoBuildSecondaryIndex ? 1 : 0));
    databaseServices->saveIngestParam(databaseInfo.name, "secondary-index", "local-load",
                                      to_string(enableLocalLoadSecondaryIndex ? 1 : 0));

    // Tell workers to reload their configurations
    error = reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);

    json result;
    result["database"] = databaseInfo.toJson();
    return result;
}

json HttpIngestModule::_publishDatabase() {
    debug(__func__);

    bool const allWorkers = true;
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const database = params().at("database");
    bool const consolidateSecondaryIndex = body().optional<int>("consolidate_secondary_index", 0) != 0;
    bool const rowCountersDeployAtQserv = body().optional<int>("row_counters_deploy_at_qserv", 0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "consolidate_secondary_index=" + bool2str(consolidateSecondaryIndex));
    debug(__func__, "row_counters_deploy_at_qserv=" + bool2str(rowCountersDeployAtQserv));

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) throw HttpError(__func__, "the database is already published");

    // Scan super-transactions to make sure none is still open
    for (auto&& t : databaseServices->transactions(databaseInfo.name)) {
        if (!(t.state == TransactionInfo::State::FINISHED || t.state == TransactionInfo::State::ABORTED)) {
            throw HttpError(__func__, "database has uncommitted transactions");
        }
    }

    // Refuse the operation if no chunks were registered
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, databaseInfo.name, allWorkers);
    if (chunks.empty()) throw HttpError(__func__, "the database doesn't have any chunks");

    // The operation can be vetoed by the corresponding workflow parameter requested
    // by a catalog ingest workflow at the database creation time.
    if (autoBuildSecondaryIndex(database) and consolidateSecondaryIndex) {
        for (auto&& table : databaseInfo.directorTables()) {
            // Skip tables that have been published.
            if (databaseInfo.tableIsPublished.at(table)) continue;
            // This operation may take a while if the table has a large number of entries.
            _consolidateSecondaryIndex(databaseInfo, table);
        }
    }

    // Note, this operation, depending on the amount of data ingested into
    // the database's tables, could be quite lengthy. Failures reported in
    // a course of this operation will not affect the "success" status of
    // the publishing request since.
    if (rowCountersDeployAtQserv) {
        bool const forceRescan = true;  // Since doing the scan for the first time.
        for (auto&& table : databaseInfo.tables()) {
            // Skip tables that have been published.
            if (databaseInfo.tableIsPublished.at(table)) continue;
            json const errorExt = _scanTableStatsImpl(
                    database, table, ChunkOverlapSelector::CHUNK_AND_OVERLAP,
                    SqlRowStatsJob::StateUpdatePolicy::ENABLED, rowCountersDeployAtQserv, forceRescan,
                    allWorkers, config->get<int>("controller", "ingest-priority-level"));
            if (!errorExt.empty()) {
                throw HttpError(__func__, "Table rows scanning/deployment failed.", errorExt);
            }
        }
    }
    _grantDatabaseAccess(databaseInfo, allWorkers);
    _enableDatabase(databaseInfo, allWorkers);
    _createMissingChunkTables(databaseInfo, allWorkers);
    _removeMySQLPartitions(databaseInfo, allWorkers);

    // Finalize setting the database in Qserv master to make the new catalog
    // visible to Qserv users.
    _publishDatabaseInMaster(databaseInfo);

    // Change database status so that it would be seen by the Qserv synchronization
    // algorithm (job) run on the next step. Otherwise users would have to wait
    // for the next synchronization cycle of the Master Replication Controller
    // which would synchronize chunks between the Replication System and Qserv
    // workers.
    json result;
    result["database"] = config->publishDatabase(database).toJson();

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    auto const error = reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);

    // Run the chunks scanner to ensure new chunks are registered in the persistent
    // store of the Replication system and synchronized with the Qserv workers.
    // The (fixing, re-balancing, replicating, etc.) will be taken care of by
    // the Replication system.
    _qservSync(databaseInfo, allWorkers);

    ControllerEvent event;
    event.status = "PUBLISH DATABASE";
    event.kvInfo.emplace_back("database", database);
    logEvent(event);

    return result;
}

json HttpIngestModule::_deleteDatabase() {
    debug(__func__);

    auto const cssAccess = qservCssAccess();
    auto const config = controller()->serviceProvider()->config();
    bool const allWorkers = true;
    auto const database = params().at("database");

    debug(__func__, "database=" + database);

    auto databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
        if (!isAdmin()) {
            throw HttpError(__func__, "deleting published databases requires administrator's privileges.");
        }
    }

    // Get the names of the 'director' tables either from the Replication/Ingest system's
    // configuration, or from CSS. It's okay not to have those tables if they weren't yet
    // created during the initial catalog ingest.
    // NOTE: Qserv allows more than one 'director' table.
    set<string> directorTables;
    for (auto&& table : databaseInfo.directorTables()) {
        directorTables.insert(table);
    }
    if (cssAccess->containsDb(databaseInfo.name)) {
        for (auto const table : cssAccess->getTableNames(databaseInfo.name)) {
            auto const partTableParams = cssAccess->getPartTableParams(databaseInfo.name, table);
            if (!partTableParams.dirTable.empty()) directorTables.insert(partTableParams.dirTable);
        }
    }

    // Remove related database entries from czar's MySQL if anything is still there
    if (cssAccess->containsDb(databaseInfo.name)) {
        cssAccess->dropDb(databaseInfo.name);
    }
    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
    h.conn->executeInOwnTransaction([&databaseInfo, &directorTables](decltype(h.conn) conn) {
        conn->execute("DROP DATABASE IF EXISTS " + conn->sqlId(databaseInfo.name));
        auto const emptyChunkListTable = css::DbInterfaceMySql::getEmptyChunksTableName(databaseInfo.name);
        conn->execute("DROP TABLE IF EXISTS " + conn->sqlId("qservCssData", emptyChunkListTable));
        for (auto const table : directorTables) {
            conn->execute("DROP TABLE IF EXISTS " +
                          conn->sqlId("qservMeta", ::directorIndexTable(databaseInfo.name, table)));
        }
        for (auto const table : databaseInfo.tables()) {
            try {
                conn->execute("DROP TABLE IF EXISTS " +
                              conn->sqlId("qservMeta", ::rowCountersTable(databaseInfo.name, table)));
            } catch (invalid_argument const&) {
                // This exception may be thrown by the table name generator if
                // it couldn't build a correct name due to MySQL limitations.
                ;
            }
        }
    });

    // Delete entries (if any still exist) for database and its chunks from worker
    // metadata tables. This will prevents Qserv workers from publishing the those
    // as XROOTD "resources".
    // NOTE: Ignore any errors should any be reported by the job.
    string const noParentJobId;
    auto const dasableDbJob =
            SqlDisableDbJob::create(databaseInfo.name, allWorkers, controller(), noParentJobId, nullptr,
                                    config->get<int>("controller", "catalog-management-priority-level"));
    dasableDbJob->start();
    logJobStartedEvent(SqlDisableDbJob::typeName(), dasableDbJob, databaseInfo.family);
    dasableDbJob->wait();
    logJobFinishedEvent(SqlDisableDbJob::typeName(), dasableDbJob, databaseInfo.family);

    // Delete database entries at workers
    auto const deleteDbJob =
            SqlDeleteDbJob::create(databaseInfo.name, allWorkers, controller(), noParentJobId, nullptr,
                                   config->get<int>("controller", "catalog-management-priority-level"));
    deleteDbJob->start();
    logJobStartedEvent(SqlDeleteDbJob::typeName(), deleteDbJob, databaseInfo.family);
    deleteDbJob->wait();
    logJobFinishedEvent(SqlDeleteDbJob::typeName(), deleteDbJob, databaseInfo.family);

    string error = ::jobCompletionErrorIfAny(deleteDbJob, "database deletion failed");
    if (not error.empty()) throw HttpError(__func__, error);

    // Remove database entry from the Configuration. This will also eliminate all
    // dependent metadata, such as replicas info
    config->deleteDatabase(databaseInfo.name);

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    error = reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);

    return json::object();
}

json HttpIngestModule::_getTables() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const database = params().at("database");

    debug(__func__, "database=" + database);

    auto const databaseInfo = config->databaseInfo(database);

    json tablesJson = json::array();
    for (auto&& table : databaseInfo.partitionedTables) {
        tablesJson.push_back({{"name", table}, {"is_partitioned", 1}});
    }
    for (auto&& table : databaseInfo.regularTables) {
        tablesJson.push_back({{"name", table}, {"is_partitioned", 0}});
    }
    return json::object({{"tables", tablesJson}});
}

json HttpIngestModule::_addTable() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    auto const database = body().required<string>("database");
    auto const table = body().required<string>("table");
    auto const isPartitioned = body().required<int>("is_partitioned") != 0;
    auto const directorTable = body().optional<string>("director_table", "");
    auto const isDirector = isPartitioned && directorTable.empty();
    auto const directorKey = isPartitioned ? body().required<string>("director_key") : "";
    auto const latitudeColName = isDirector ? body().required<string>("latitude_key")
                                            : body().optional<string>("latitude_key", "");
    auto const longitudeColName = isDirector ? body().required<string>("longitude_key")
                                             : body().optional<string>("longitude_key", "");
    auto const schema = body().required<json>("schema");

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);
    debug(__func__, "isPartitioned=" + bool2str(isPartitioned));
    debug(__func__, "isDirector=" + bool2str(isDirector));
    debug(__func__, "directorTable=" + directorTable);
    debug(__func__, "directorKey=" + directorKey);
    debug(__func__, "latitudeColName=" + latitudeColName);
    debug(__func__, "longitudeColName=" + longitudeColName);
    debug(__func__, "schema=" + schema.dump());

    auto databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) throw HttpError(__func__, "the database is already published");

    for (auto&& existingTable : databaseInfo.tables()) {
        if (table == existingTable) throw HttpError(__func__, "table already exists");
    }

    // Translate table schema
    if (schema.is_null()) throw HttpError(__func__, "table schema is empty");
    if (not schema.is_array()) throw HttpError(__func__, "table schema is not defined as an array");

    list<SqlColDef> columns;

    // The name of a special column for the super-transaction-based ingest.
    // Always insert this column as the very first one into the schema.
    columns.emplace_front(_partitionByColumn, _partitionByColumnType);

    for (auto&& coldef : schema) {
        if (not coldef.is_object()) {
            throw HttpError(__func__, "columns definitions in table schema are not JSON objects");
        }
        if (0 == coldef.count("name")) {
            throw HttpError(__func__,
                            "column attribute 'name' is missing in table schema for "
                            "column number: " +
                                    to_string(columns.size() + 1));
        }
        string colName = coldef["name"];
        if (0 == coldef.count("type")) {
            throw HttpError(__func__,
                            "column attribute 'type' is missing in table schema for "
                            "column number: " +
                                    to_string(columns.size() + 1));
        }
        string colType = coldef["type"];

        if (_partitionByColumn == colName) {
            throw HttpError(__func__, "reserved column '" + _partitionByColumn + "' is not allowed");
        }
        columns.emplace_back(colName, colType);
    }

    // Register table in the Configuration
    json result;
    result["database"] = config->addTable(databaseInfo.name, table, isPartitioned, columns, isDirector,
                                          directorTable, directorKey, latitudeColName, longitudeColName)
                                 .toJson();

    // Create template and special (if the partitioned table requested) tables on all
    // workers. These tables will be used to create chunk-specific tables before
    // loading data.
    //
    // The special tables to be created are for the "dummy" chunk which is required
    // to be present on each worker regardless if it (the worker) will have or not
    // any normal chunks upon completion of the ingest. Not having the special chunk
    // will confuse the ingest (and eventually - Qserv query processor).

    bool const allWorkers = true;
    string const engine = "MyISAM";
    string const noParentJobId;

    vector<string> tables;
    tables.push_back(table);
    if (isPartitioned) {
        unsigned int const chunk = lsst::qserv::DUMMY_CHUNK;
        bool const overlap = false;
        tables.push_back(ChunkedTable(table, chunk, overlap).name());
        tables.push_back(ChunkedTable(table, chunk, not overlap).name());
    }
    for (auto&& table : tables) {
        auto const job = SqlCreateTableJob::create(databaseInfo.name, table, engine, _partitionByColumn,
                                                   columns, allWorkers, controller(), noParentJobId, nullptr,
                                                   config->get<int>("controller", "ingest-priority-level"));
        job->start();
        logJobStartedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);

        string const error = ::jobCompletionErrorIfAny(job, "table creation failed for: '" + table + "'");
        if (not error.empty()) throw HttpError(__func__, error);
    }

    // Create the secondary index table using an updated version of
    // the database descriptor.
    //
    // This operation can be vetoed by a catalog ingest workflow at the database
    // registration time.
    if (autoBuildSecondaryIndex(databaseInfo.name)) {
        if (isPartitioned && isDirector) {
            _createSecondaryIndex(config->databaseInfo(databaseInfo.name), table);
        }
    }

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    string const error = reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);

    return result;
}

json HttpIngestModule::_deleteTable() {
    debug(__func__);

    auto const cssAccess = qservCssAccess();
    auto const config = controller()->serviceProvider()->config();
    bool const allWorkers = true;
    auto const database = params().at("database");
    auto const table = params().at("table");

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);

    auto databaseInfo = config->databaseInfo(database);
    auto const tables = databaseInfo.tables();
    if (tables.cend() == find(tables.cbegin(), tables.cend(), table)) {
        throw invalid_argument(context() + "::" + string(__func__) + " unknown table: '" + table + "'");
    }
    if (databaseInfo.isPublished) {
        if (!isAdmin()) {
            throw HttpError(__func__,
                            "deleting tables of published databases requires administrator's"
                            " privileges.");
        }
    }

    // Remove table entry from czar's databases if it's still there
    try {
        if (cssAccess->containsDb(databaseInfo.name)) {
            if (cssAccess->containsTable(databaseInfo.name, table)) {
                cssAccess->dropTable(databaseInfo.name, table);
            }
        }
    } catch (css::NoSuchTable const& ex) {
        // The table may have already been deleted by another request while this one
        // was checking for the table status in the CSS.
        ;
    }

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
    h.conn->executeInOwnTransaction([&databaseInfo, &table](decltype(h.conn) conn) {
        // Remove table entry from czar's MySQL
        conn->execute("DROP TABLE IF EXISTS " + conn->sqlId(databaseInfo.name, table));
        // Remove the director index (if any)
        if (databaseInfo.isDirector(table)) {
            conn->execute("DROP TABLE IF EXISTS " +
                          conn->sqlId("qservMeta", ::directorIndexTable(databaseInfo.name, table)));
        }
        // Remove the row counters table (if any)
        try {
            conn->execute("DROP TABLE IF EXISTS " +
                          conn->sqlId("qservMeta", ::rowCountersTable(databaseInfo.name, table)));
        } catch (invalid_argument const&) {
            // This exception may be thrown by the table name generator if
            // it couldn't build a correct name due to MySQL limitations.
            ;
        }
    });

    // Delete table entries at workers
    string const noParentJobId;
    auto const job = SqlDeleteTableJob::create(
            databaseInfo.name, table, allWorkers, controller(), noParentJobId, nullptr,
            config->get<int>("controller", "catalog-management-priority-level"));
    job->start();
    logJobStartedEvent(SqlDeleteTableJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlDeleteTableJob::typeName(), job, databaseInfo.family);

    string error = ::jobCompletionErrorIfAny(job, "table deletion failed");
    if (not error.empty()) throw HttpError(__func__, error);

    // Remove table entry from the Configuration. This will also eliminate all
    // dependent metadata, such as replicas info
    config->deleteTable(databaseInfo.name, table);

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    error = reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);

    return json::object();
}

json HttpIngestModule::_scanTableStats() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const database = body().required<string>("database");
    auto const table = body().required<string>("table");
    auto const overlapSelector =
            str2overlapSelector(body().optional<string>("overlap_selector", "CHUNK_AND_OVERLAP"));
    auto const rowCountersStateUpdatePolicy = SqlRowStatsJob::str2policy(
            body().optional<string>("row_counters_state_update_policy", "DISABLED"));
    bool const rowCountersDeployAtQserv = body().optional<int>("row_counters_deploy_at_qserv", 0) != 0;
    bool const forceRescan = body().optional<int>("force_rescan", 0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);
    debug(__func__, "overlap_selector=" + overlapSelector2str(overlapSelector));
    debug(__func__,
          "row_counters_state_update_policy=" + SqlRowStatsJob::policy2str(rowCountersStateUpdatePolicy));
    debug(__func__, "row_counters_deploy_at_qserv=" + bool2str(rowCountersDeployAtQserv));
    debug(__func__, "force_rescan=" + bool2str(forceRescan));

    if (rowCountersDeployAtQserv &&
        rowCountersStateUpdatePolicy != SqlRowStatsJob::StateUpdatePolicy::ENABLED) {
        throw invalid_argument(context() + "::" + string(__func__) +
                               "'row_counters_deploy_at_qserv'=1 requires"
                               " 'row_counters_state_update_policy'=ENABLED");
    }
    if (rowCountersDeployAtQserv && overlapSelector == ChunkOverlapSelector::OVERLAP) {
        throw invalid_argument(context() + "::" + string(__func__) +
                               "'row_counters_deploy_at_qserv'=1 requires"
                               " 'overlap_selector'=CHUNK or 'overlap_selector'=CHUNK_AND_OVERLAP");
    }
    bool const allWorkers = true;
    json const errorExt = _scanTableStatsImpl(
            database, table, overlapSelector, rowCountersStateUpdatePolicy, rowCountersDeployAtQserv,
            forceRescan, allWorkers, config->get<int>("controller", "catalog-management-priority-level"));
    if (!errorExt.empty()) {
        throw HttpError(__func__, "Table rows scanning/deployment failed.", errorExt);
    }
    return json::object();
}

json HttpIngestModule::_scanTableStatsImpl(string const& database, string const& table,
                                           ChunkOverlapSelector overlapSelector,
                                           SqlRowStatsJob::StateUpdatePolicy stateUpdatePolicy,
                                           bool deployAtQserv, bool forceRescan, bool allWorkers,
                                           int priority) {
    TransactionId const transactionId = 0;  // All transactions will be used.

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();
    auto const databaseInfo = config->databaseInfo(database);
    auto const tables = databaseInfo.tables();
    if (tables.cend() == find(tables.cbegin(), tables.cend(), table)) {
        throw invalid_argument(context() + "::" + string(__func__) + " unknown table: '" + table + "'");
    }
    bool const isPartitioned = databaseInfo.isPartitioned(table);

    // Check if an optional optimization of not re-scanning the counters would
    // be possible in the given context.
    bool scanRequired = forceRescan;
    if (!scanRequired) {
        auto const stats = databaseServices->tableRowStats(database, table, transactionId);
        if (stats.entries.size() == 0) {
            scanRequired = true;
            debug(__func__, "scan required since no entries exist for " + database + "." + table);
        } else if (isPartitioned) {
            // Get a collection of all (but th especial one) chunks that have been
            // registered for the database and turn it into a set.
            bool const enabledWorkersOnly = !allWorkers;
            vector<unsigned int> allChunks;
            databaseServices->findDatabaseChunks(allChunks, database, enabledWorkersOnly);
            set<unsigned int> chunks;
            for (unsigned int const chunk : allChunks) {
                if (chunk == lsst::qserv::DUMMY_CHUNK) continue;
                chunks.insert(chunk);
            }

            // Scan existing entries to find two sets of chunks corresponding
            // to chunk entries and chunk overlap entries.
            set<unsigned int> chunkOverlapsInEntries;
            set<unsigned int> chunksInEntries;
            for (auto&& e : stats.entries) {
                if (e.chunk == lsst::qserv::DUMMY_CHUNK) continue;
                if (e.isOverlap)
                    chunkOverlapsInEntries.insert(e.chunk);
                else
                    chunksInEntries.insert(e.chunk);
            }
            switch (overlapSelector) {
                case ChunkOverlapSelector::CHUNK:
                    scanRequired = chunksInEntries != chunks;
                    break;
                case ChunkOverlapSelector::OVERLAP:
                    scanRequired = chunkOverlapsInEntries != chunks;
                    break;
                case ChunkOverlapSelector::CHUNK_AND_OVERLAP:
                    scanRequired = chunksInEntries != chunks || chunkOverlapsInEntries != chunks;
                    break;
                default:
                    runtime_error("HttpIngestModule::" + string(__func__) +
                                  " unsupported overlap selector '" + overlapSelector2str(overlapSelector));
            }
            if (scanRequired) {
                debug(__func__, "scan required for " + database + "." + table +
                                        " since"
                                        " chunks.size(): " +
                                        to_string(chunks.size()) + " chunkOverlapsInEntries.size(): " +
                                        to_string(chunkOverlapsInEntries.size()) +
                                        " chunksInEntries.size(): " + to_string(chunksInEntries.size()) +
                                        " with overlapSelector: " + overlapSelector2str(overlapSelector));
            }
        } else {
            // The regular table won't require rescan since the collection of entries
            // for the table is not empty.
        }
    }
    if (scanRequired) {
        string const noParentJobId;
        auto const job = SqlRowStatsJob::create(databaseInfo.name, table, overlapSelector, stateUpdatePolicy,
                                                allWorkers, controller(), noParentJobId, nullptr, priority);
        job->start();
        logJobStartedEvent(SqlDisableDbJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlDisableDbJob::typeName(), job, databaseInfo.family);

        if (job->extendedState() != Job::ExtendedState::SUCCESS) {
            json errorExt = json::object({{"operation", "Scan table row counters."},
                                          {"job_id", job->id()},
                                          {"workers", json::object()}});
            job->getResultData().iterate([&](SqlJobResult::Worker const& worker,
                                             SqlJobResult::Scope const& internalTable,
                                             SqlResultSet::ResultSet const& resultSet) {
                if (resultSet.extendedStatus != ProtocolStatusExt::NONE) {
                    errorExt["workers"][worker][internalTable] =
                            json::object({{"status", ProtocolStatusExt_Name(resultSet.extendedStatus)},
                                          {"error", resultSet.error}});
                }
            });
            return errorExt;
        }
    }
    if (deployAtQserv) {
        auto const stats = databaseServices->tableRowStats(database, table, transactionId);

        // Entries for the partitioned and regular tables will be filtered and
        // processed differently. In case of the former counters from the chunk
        // overlaps will be ignored and row numbers will be aggregated by chunks.
        // For the latter a single number of rows for for the chunk number 0 will
        // be computed.
        map<unsigned int, size_t> chunk2rows;
        for (auto&& e : stats.entries) {
            if (isPartitioned) {
                if (!e.isOverlap) chunk2rows[e.chunk] += e.numRows;
            } else {
                chunk2rows[0] += e.numRows;
            }
        }

        // Load counters into Qserv after removing all previous entries
        // for the table to ensure the clean state.
        try {
            database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
            string const sqlCreateRowCountersTable =
                    "CREATE TABLE IF NOT EXISTS " + h.conn->sqlId(::rowCountersTable(database, table)) +
                    " (" + h.conn->sqlId("chunk") + " INT UNSIGNED NOT NULL , " + h.conn->sqlId("num_rows") +
                    " BIGINT UNSIGNED DEFAULT 0, "
                    " UNIQUE KEY (" +
                    h.conn->sqlId("chunk") +
                    ")) "
                    " ENGINE = InnoDB"
                    " COMMENT = 'Row counters for the internal tables. The table is supposed to be populated "
                    "by"
                    " the ingest system when publishing the catalog, or afterwards by the table scanner.'";
            string const sqlEmptyRowCountersTable =
                    "DELETE FROM " + h.conn->sqlId(::rowCountersTable(database, table));
            h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                conn->execute(sqlCreateRowCountersTable);
                conn->execute(sqlEmptyRowCountersTable);
                for (auto&& itr : chunk2rows) {
                    unsigned int const chunk = itr.first;
                    size_t const numRows = itr.second;
                    conn->execute(conn->sqlInsertQuery(::rowCountersTable(database, table), chunk, numRows));
                }
            });
        } catch (exception const& ex) {
            string const msg = "Failed to load/update row counters for table '" + table + "' of database '" +
                               database + "' into Qserv, ex: " + string(ex.what());
            error(__func__, msg);
            return json::object({{"operation", "Deploy table row counters in Qserv."}, {"error", msg}});
        }
    }
    return json::object();
}

json HttpIngestModule::_deleteTableStats() {
    debug(__func__);

    string database;
    string table;
    _getRequiredParameters(__func__, database, table);

    bool const qservOnly = body().optional<int>("qserv_only", 0) != 0;
    auto const overlapSelector =
            str2overlapSelector(body().optional<string>("overlap_selector", "CHUNK_AND_OVERLAP"));
    debug(__func__, "qserv_only=" + bool2str(qservOnly));
    debug(__func__, "overlap_selector=" + overlapSelector2str(overlapSelector));

    try {
        database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
        h.conn->executeInOwnTransaction([&database, &table](decltype(h.conn) conn) {
            conn->execute("DROP TABLE IF EXISTS " + conn->sqlId(::rowCountersTable(database, table)));
        });
    } catch (exception const& ex) {
        string const msg = "Failed to delete metadata table with counters for table '" + table +
                           "' of database '" + database + "' from Qserv, ex: " + string(ex.what());
        error(__func__, msg);
        throw HttpError(__func__, msg,
                        json::object({{"operation", "Deploy table row counters in Qserv."}, {"error", msg}}));
    }

    // Delete stats from the Replication system's persistent state if requested.
    if (!qservOnly) {
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        databaseServices->deleteTableRowStats(database, table, overlapSelector);
    }
    return json::object();
}

json HttpIngestModule::_tableStats() {
    debug(__func__);

    string database;
    string table;
    _getRequiredParameters(__func__, database, table);

    // Counters ingested in all transactions are pulled.
    TransactionId const transactionId = 0;
    return controller()
            ->serviceProvider()
            ->databaseServices()
            ->tableRowStats(database, table, transactionId)
            .toJson();
}

json HttpIngestModule::_buildEmptyChunksList() {
    debug(__func__);

    string const database = body().required<string>("database");
    bool const force = body().optional<int>("force", 0) != 0;
    bool const tableImpl = body().optional<int>("table_impl", 0) != 0;

    debug(__func__, "database=" + database);
    debug(__func__, "force=" + bool2str(force));
    debug(__func__, "table_impl=" + bool2str(tableImpl));

    return _buildEmptyChunksListImpl(database, force, tableImpl);
}

json HttpIngestModule::_getRegular() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    DatabaseInfo const databaseInfo = getDatabaseInfo(__func__);

    json resultLocations = json::array();
    for (auto&& worker : config->workers()) {
        auto&& workerInfo = config->workerInfo(worker);
        json resultLocation;
        resultLocation["worker"] = workerInfo.name;
        resultLocation["host"] = workerInfo.loaderHost;
        resultLocation["port"] = workerInfo.loaderPort;
        resultLocation["http_host"] = workerInfo.httpLoaderHost;
        resultLocation["http_port"] = workerInfo.httpLoaderPort;
        resultLocations.push_back(resultLocation);
    }
    json result;
    result["locations"] = resultLocations;
    return result;
}

void HttpIngestModule::_getRequiredParameters(string const& func, string& database, string& table) {
    database = params().at("database");
    table = params().at("table");
    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);
    auto const databaseInfo = controller()->serviceProvider()->config()->databaseInfo(database);
    auto const tables = databaseInfo.tables();
    if (tables.cend() == find(tables.cbegin(), tables.cend(), table)) {
        throw invalid_argument(context() + "::" + func + " unknown table: '" + table + "'.");
    }
}

void HttpIngestModule::_grantDatabaseAccess(DatabaseInfo const& databaseInfo, bool allWorkers) const {
    debug(__func__);

    string const noParentJobId;
    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlGrantAccessJob::create(
            databaseInfo.name, config->get<string>("database", "qserv-master-user"), allWorkers, controller(),
            noParentJobId, nullptr, config->get<int>("controller", "ingest-priority-level"));
    job->start();
    logJobStartedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);

    string const error = ::jobCompletionErrorIfAny(job, "grant access to a database failed");
    if (not error.empty()) throw HttpError(__func__, error);
}

void HttpIngestModule::_enableDatabase(DatabaseInfo const& databaseInfo, bool allWorkers) const {
    debug(__func__);

    string const noParentJobId;
    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlEnableDbJob::create(databaseInfo.name, allWorkers, controller(), noParentJobId,
                                            nullptr, config->get<int>("controller", "ingest-priority-level"));
    job->start();
    logJobStartedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);

    string const error = ::jobCompletionErrorIfAny(job, "enabling database failed");
    if (not error.empty()) throw HttpError(__func__, error);
}

void HttpIngestModule::_createMissingChunkTables(DatabaseInfo const& databaseInfo, bool allWorkers) const {
    debug(__func__);

    string const engine = "MyISAM";
    string const noParentJobId;

    for (auto&& table : databaseInfo.partitionedTables) {
        // Skip tables that have been published.
        if (databaseInfo.tableIsPublished.at(table)) continue;
        auto const columnsItr = databaseInfo.columns.find(table);
        if (columnsItr == databaseInfo.columns.cend()) {
            throw HttpError(__func__, "schema is empty for table: '" + table + "'");
        }
        auto const job = SqlCreateTablesJob::create(
                databaseInfo.name, table, engine, _partitionByColumn, columnsItr->second, allWorkers,
                controller(), noParentJobId, nullptr,
                controller()->serviceProvider()->config()->get<int>("controller", "ingest-priority-level"));
        job->start();
        logJobStartedEvent(SqlCreateTablesJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlCreateTablesJob::typeName(), job, databaseInfo.family);

        string const error = ::jobCompletionErrorIfAny(job, "table creation failed for: '" + table + "'");
        if (not error.empty()) throw HttpError(__func__, error);
    }
}

void HttpIngestModule::_removeMySQLPartitions(DatabaseInfo const& databaseInfo, bool allWorkers) const {
    debug(__func__);

    // Ignore tables which may have already been processed at a previous attempt
    // of running this algorithm.
    bool const ignoreNonPartitioned = true;
    string const noParentJobId;
    string error;
    for (auto const table : databaseInfo.tables()) {
        // Skip tables that have been published.
        if (databaseInfo.tableIsPublished.at(table)) continue;
        auto const job = SqlRemoveTablePartitionsJob::create(
                databaseInfo.name, table, allWorkers, ignoreNonPartitioned, controller(), noParentJobId,
                nullptr,
                controller()->serviceProvider()->config()->get<int>("controller", "ingest-priority-level"));
        job->start();
        logJobStartedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);

        error += ::jobCompletionErrorIfAny(job, "MySQL partitions removal failed for database: " +
                                                        databaseInfo.name + ", table: " + table);
    }
    if (not error.empty()) throw HttpError(__func__, error);
}

void HttpIngestModule::_publishDatabaseInMaster(DatabaseInfo const& databaseInfo) const {
    auto const config = controller()->serviceProvider()->config();
    auto const databaseFamilyInfo = config->databaseFamilyInfo(databaseInfo.family);

    // Connect to the master database as user "root".
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.
    {
        database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));

        // SQL statements to be executed
        vector<string> statements;

        // Statements for creating the database & table entries
        statements.push_back("CREATE DATABASE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name));
        for (auto const& table : databaseInfo.tables()) {
            // Skip tables that have been published.
            if (databaseInfo.tableIsPublished.at(table)) continue;
            string sql = "CREATE TABLE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name) + "." +
                         h.conn->sqlId(table) + " (";
            bool first = true;
            for (auto const& coldef : databaseInfo.columns.at(table)) {
                if (first) {
                    first = false;
                } else {
                    sql += ",";
                }
                sql += h.conn->sqlId(coldef.name) + " " + coldef.type;
            }
            sql += ") ENGINE=InnoDB";
            statements.push_back(sql);
        }

        // Statements for granting SELECT authorizations for the new database
        // to the Qserv account.
        statements.push_back("GRANT ALL ON " + h.conn->sqlId(databaseInfo.name) + ".* TO " +
                             h.conn->sqlValue(config->get<string>("database", "qserv-master-user")) + "@" +
                             h.conn->sqlValue("localhost"));

        h.conn->executeInOwnTransaction([&statements](decltype(h.conn) conn) {
            for (auto const& query : statements) {
                conn->execute(query);
            }
        });
    }

    // Register the database, tables and the partitioning scheme at CSS
    auto const cssAccess = qservCssAccess();
    if (not cssAccess->containsDb(databaseInfo.name)) {
        // First, try to find another database within the same family which
        // has already been published, and the one is found then use it
        // as a template when registering the database in CSS.
        //
        // Otherwise, create a new database using an extended CSS API which
        // will allocate a new partitioning identifier.
        bool const allDatabases = false;
        bool const isPublished = true;
        auto const databases = config->databases(databaseFamilyInfo.name, allDatabases, isPublished);
        if (not databases.empty()) {
            auto const templateDatabase = databases.front();
            cssAccess->createDbLike(databaseInfo.name, templateDatabase);
        } else {
            // This parameter is not used by the current implementation of the CSS API.
            // However, we should give it some meaningless value in case of the implementation
            // will change. (Hopefully) this would trigger an exception.
            int const unusedPartitioningId = -1;
            css::StripingParams const stripingParams(databaseFamilyInfo.numStripes,
                                                     databaseFamilyInfo.numSubStripes, unusedPartitioningId,
                                                     databaseFamilyInfo.overlap);
            string const storageClass = "L2";
            string const releaseStatus = "RELEASED";
            cssAccess->createDb(databaseInfo.name, stripingParams, storageClass, releaseStatus);
        }
    }

    // Register tables which still hasn't been registered in CSS
    for (auto const& table : databaseInfo.regularTables) {
        // Skip tables that have been published.
        if (databaseInfo.tableIsPublished.at(table)) continue;
        if (not cssAccess->containsTable(databaseInfo.name, table)) {
            // Neither of those groups of parameters apply to the regular tables,
            // so we're leaving them default constructed.
            css::PartTableParams const partParams;
            css::ScanTableParams const scanParams;
            cssAccess->createTable(databaseInfo.name, table, databaseInfo.schema4css(table), partParams,
                                   scanParams);
        }
    }
    for (auto const& table : databaseInfo.partitionedTables) {
        // Skip tables that have been published.
        if (databaseInfo.tableIsPublished.at(table)) continue;
        if (not cssAccess->containsTable(databaseInfo.name, table)) {
            bool const isPartitioned = true;

            // These parameters need to be set correctly for the 'director' and dependent
            // tables to avoid confusing Qserv query analyzer. Also note, that the 'overlap'
            // is set to be the same for all 'director' tables of the database family.
            bool const isDirector = databaseInfo.isDirector(table);
            double const overlap = isDirector ? databaseFamilyInfo.overlap : 0;
            bool const hasSubChunks = isDirector;

            css::PartTableParams const partParams(
                    databaseInfo.name, databaseInfo.directorTable.at(table),
                    databaseInfo.directorTableKey.at(table), databaseInfo.latitudeColName.at(table),
                    databaseInfo.longitudeColName.at(table), overlap, isPartitioned, hasSubChunks);

            bool const lockInMem = true;
            int const scanRating = 1;
            css::ScanTableParams const scanParams(lockInMem, scanRating);

            cssAccess->createTable(databaseInfo.name, table, databaseInfo.schema4css(table), partParams,
                                   scanParams);
        }
    }

    bool const forceRebuild = true;
    bool const tableImpl = true;
    _buildEmptyChunksListImpl(databaseInfo.name, forceRebuild, tableImpl);
}

json HttpIngestModule::_buildEmptyChunksListImpl(string const& database, bool force, bool tableImpl) const {
    debug(__func__);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) {
        throw invalid_argument("database is already published");
    }

    // Get a collection of all possible chunks which are allowed to be present
    // in the database given its partitioning scheme.
    auto const familyInfo = config->databaseFamilyInfo(databaseInfo.family);
    lsst::sphgeom::Chunker const chunker(familyInfo.numStripes, familyInfo.numSubStripes);
    auto const allChunks = chunker.getAllChunks();

    // Get the numbers of chunks ingested into the database. They will be excluded
    // from the "empty chunk list".
    set<unsigned int> ingestedChunks;
    {
        bool const enabledWorkersOnly = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database, enabledWorkersOnly);

        for (auto chunk : chunks) ingestedChunks.insert(chunk);
    }

    if (tableImpl) {
        database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
        string const table = css::DbInterfaceMySql::getEmptyChunksTableName(database);
        vector<string> statements;
        if (force) statements.push_back("DROP TABLE IF EXISTS " + h.conn->sqlId(table));
        statements.push_back(css::DbInterfaceMySql::getEmptyChunksSchema(database));
        for (auto&& chunk : allChunks) {
            if (not ingestedChunks.count(chunk)) {
                statements.push_back(h.conn->sqlInsertQuery(table, chunk));
            }
        }
        h.conn->executeInOwnTransaction([&statements](decltype(h.conn) conn) {
            for (auto const& query : statements) {
                conn->execute(query);
            }
        });
    } else {
        auto const file = "empty_" + database + ".txt";
        auto const filePath = fs::path(config->get<string>("controller", "empty-chunks-dir")) / file;

        if (not force) {
            boost::system::error_code ec;
            fs::file_status const stat = fs::status(filePath, ec);
            if (stat.type() == fs::status_error) {
                throw runtime_error("failed to check the status of file: " + filePath.string());
            }
            if (fs::exists(stat)) {
                throw runtime_error("'force' is required to overwrite existing file: " + filePath.string());
            }
        }

        debug(__func__, "creating/opening file: " + filePath.string());
        ofstream ofs(filePath.string());
        if (not ofs.good()) {
            throw runtime_error("failed to create/open file: " + filePath.string());
        }
        for (auto&& chunk : allChunks) {
            if (not ingestedChunks.count(chunk)) {
                ofs << chunk << "\n";
            }
        }
        ofs.flush();
        ofs.close();
    }
    json result;
    result["num_chunks_ingested"] = ingestedChunks.size();
    result["num_chunks_all"] = allChunks.size();
    return result;
}

void HttpIngestModule::_createSecondaryIndex(DatabaseInfo const& databaseInfo,
                                             string const& directorTable) const {
    if (!databaseInfo.isDirector(directorTable)) {
        throw logic_error("table '" + directorTable + "' is not configured in database '" +
                          databaseInfo.name + "' as the director table");
    }
    if ((databaseInfo.directorTableKey.count(directorTable) == 0) or
        databaseInfo.directorTableKey.at(directorTable).empty()) {
        throw logic_error("director key of table '" + directorTable + "' is not configured in database '" +
                          databaseInfo.name + "'");
    }
    string const& directorTableKey = databaseInfo.directorTableKey.at(directorTable);
    if (0 == databaseInfo.columns.count(directorTable)) {
        throw logic_error("no schema found for director table '" + directorTable + "' of database '" +
                          databaseInfo.name + "'");
    }

    // Find types of the secondary index table's columns
    string directorTableKeyType;
    string const chunkIdColNameType = "INT";
    string const subChunkIdColNameType = "INT";

    for (auto&& coldef : databaseInfo.columns.at(directorTable)) {
        if (coldef.name == directorTableKey) directorTableKeyType = coldef.type;
    }
    if (directorTableKeyType.empty()) {
        throw logic_error("column definition for the director key column '" + directorTableKey +
                          "' is missing in the director table schema for table '" + directorTable +
                          "' of database '" + databaseInfo.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.
    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    vector<string> queries;
    queries.push_back("DROP TABLE IF EXISTS " +
                      h.conn->sqlId(::directorIndexTable(databaseInfo.name, directorTable)));
    queries.push_back("CREATE TABLE IF NOT EXISTS " +
                      h.conn->sqlId(::directorIndexTable(databaseInfo.name, directorTable)) + " (" +
                      h.conn->sqlId(_partitionByColumn) + " " + _partitionByColumnType + "," +
                      h.conn->sqlId(directorTableKey) + " " + directorTableKeyType + "," +
                      h.conn->sqlId(lsst::qserv::CHUNK_COLUMN) + " " + chunkIdColNameType + "," +
                      h.conn->sqlId(lsst::qserv::SUB_CHUNK_COLUMN) + " " + subChunkIdColNameType +
                      ","
                      " UNIQUE KEY (" +
                      h.conn->sqlId(_partitionByColumn) + "," + h.conn->sqlId(directorTableKey) +
                      "),"
                      " KEY (" +
                      h.conn->sqlId(directorTableKey) +
                      ")"
                      ") ENGINE=InnoDB PARTITION BY LIST (" +
                      h.conn->sqlId(_partitionByColumn) + ") (PARTITION `p0` VALUES IN (0) ENGINE=InnoDB)");
    h.conn->executeInOwnTransaction([&queries](decltype(h.conn) conn) {
        for (auto&& query : queries) {
            conn->execute(query);
        }
    });
}

void HttpIngestModule::_consolidateSecondaryIndex(DatabaseInfo const& databaseInfo,
                                                  string const& directorTable) const {
    if (!databaseInfo.isDirector(directorTable)) {
        throw logic_error("table '" + directorTable + "' is not configured in database '" +
                          databaseInfo.name + "' as the director table");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.
    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    string const query = "ALTER TABLE " +
                         h.conn->sqlId(::directorIndexTable(databaseInfo.name, directorTable)) +
                         " REMOVE PARTITIONING";
    debug(__func__, query);
    h.conn->executeInOwnTransaction([&query](decltype(h.conn) conn) { conn->execute(query); });
}

void HttpIngestModule::_qservSync(DatabaseInfo const& databaseInfo, bool allWorkers) const {
    debug(__func__);

    bool const saveReplicaInfo = true;
    string const noParentJobId;
    auto const findAlljob = FindAllJob::create(
            databaseInfo.family, saveReplicaInfo, allWorkers, controller(), noParentJobId, nullptr,
            controller()->serviceProvider()->config()->get<int>("controller", "ingest-priority-level"));
    findAlljob->start();
    logJobStartedEvent(FindAllJob::typeName(), findAlljob, databaseInfo.family);
    findAlljob->wait();
    logJobFinishedEvent(FindAllJob::typeName(), findAlljob, databaseInfo.family);

    if (findAlljob->extendedState() != Job::SUCCESS) {
        throw HttpError(__func__, "replica lookup stage failed");
    }

    bool const force = false;
    auto const qservSyncJob =
            QservSyncJob::create(databaseInfo.family, force, qservSyncTimeoutSec(), controller());
    qservSyncJob->start();
    logJobStartedEvent(QservSyncJob::typeName(), qservSyncJob, databaseInfo.family);
    qservSyncJob->wait();
    logJobFinishedEvent(QservSyncJob::typeName(), qservSyncJob, databaseInfo.family);

    if (qservSyncJob->extendedState() != Job::SUCCESS) {
        throw HttpError(__func__, "Qserv synchronization failed");
    }
}

}  // namespace lsst::qserv::replica
