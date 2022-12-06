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
            error += prefix + "failed, job: " + job->id() +
                     ", extended state: " + Job::state2string(job->extendedState());
            break;
    }

    return error;
}

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

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
    checkApiVersion(__func__, 12);

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
    for (auto const databaseName : config->databases(family, allDatabases, isPublished)) {
        auto const database = config->databaseInfo(databaseName);
        databasesJson.push_back({{"name", database.name},
                                 {"family", database.family},
                                 {"is_published", database.isPublished ? 1 : 0}});
    }
    return json::object({{"databases", databasesJson}});
}

json HttpIngestModule::_addDatabase() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    string const databaseName = body().required<string>("database");

    auto const numStripes = body().required<unsigned int>("num_stripes");
    auto const numSubStripes = body().required<unsigned int>("num_sub_stripes");
    auto const overlap = body().required<double>("overlap");
    auto const enableAutoBuildDirectorIndex = body().optional<unsigned int>("auto_build_secondary_index", 1);
    auto const enableLocalLoadDirectorIndex = body().optional<unsigned int>("local_load_secondary_index", 0);

    debug(__func__, "database=" + databaseName);
    debug(__func__, "num_stripes=" + to_string(numStripes));
    debug(__func__, "num_sub_stripes=" + to_string(numSubStripes));
    debug(__func__, "overlap=" + to_string(overlap));
    debug(__func__, "auto_build_secondary_index=" + to_string(enableAutoBuildDirectorIndex ? 1 : 0));
    debug(__func__, "local_load_secondary_index=" + to_string(enableLocalLoadDirectorIndex ? 1 : 0));

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
    auto const job = SqlCreateDbJob::create(databaseName, allWorkers, controller(), noParentJobId, nullptr,
                                            config->get<int>("controller", "ingest-priority-level"));
    job->start();
    logJobStartedEvent(SqlCreateDbJob::typeName(), job, family);
    job->wait();
    logJobFinishedEvent(SqlCreateDbJob::typeName(), job, family);

    string error = ::jobCompletionErrorIfAny(job, "database creation failed");
    if (!error.empty()) throw HttpError(__func__, error);

    // Register the new database in the Configuration.
    // Note, this operation will fail if the database with the name
    // already exists. Also, the new database won't have any tables
    // until they will be added as a separate step.
    auto database = config->addDatabase(databaseName, family);

    // Register a requested mode for building the "director" index. If a value
    // of the parameter is set to 'true' (or '1' in the database) then contributions
    // into the index will be automatically made when committing transactions. Otherwise,
    // it's going to be up to a user's catalog ingest workflow to (re-)build
    // the index.
    databaseServices->saveIngestParam(database.name, "secondary-index", "auto-build",
                                      to_string(enableAutoBuildDirectorIndex ? 1 : 0));
    databaseServices->saveIngestParam(database.name, "secondary-index", "local-load",
                                      to_string(enableLocalLoadDirectorIndex ? 1 : 0));

    // Tell workers to reload their configurations
    error = reconfigureWorkers(database, allWorkers, workerReconfigTimeoutSec());
    if (!error.empty()) throw HttpError(__func__, error);

    json result;
    result["database"] = database.toJson();
    return result;
}

json HttpIngestModule::_publishDatabase() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    bool const allWorkers = true;
    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const databaseName = params().at("database");
    bool const consolidateDirectorIndex = body().optional<int>("consolidate_secondary_index", 0) != 0;
    bool const rowCountersDeployAtQserv = body().optional<int>("row_counters_deploy_at_qserv", 0) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "consolidate_secondary_index=" + bool2str(consolidateDirectorIndex));
    debug(__func__, "row_counters_deploy_at_qserv=" + bool2str(rowCountersDeployAtQserv));

    auto const database = config->databaseInfo(databaseName);
    if (database.isPublished) throw HttpError(__func__, "the database is already published");

    // Scan super-transactions to make sure none is still open
    for (auto&& t : databaseServices->transactions(database.name)) {
        if (!(t.state == TransactionInfo::State::FINISHED || t.state == TransactionInfo::State::ABORTED)) {
            throw HttpError(__func__, "database has uncommitted transactions");
        }
    }

    // Refuse the operation if no chunks were registered
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, database.name, allWorkers);
    if (chunks.empty()) throw HttpError(__func__, "the database doesn't have any chunks");

    // The operation can be vetoed by the corresponding workflow parameter requested
    // by a catalog ingest workflow at the database creation time.
    if (autoBuildDirectorIndex(database.name) and consolidateDirectorIndex) {
        for (auto&& tableName : database.directorTables()) {
            auto const table = database.findTable(tableName);
            if (table.isPublished) continue;
            // This operation may take a while if the table has a large number of entries.
            _consolidateDirectorIndex(database, table.name);
        }
    }

    // Note, this operation, depending on the amount of data ingested into
    // the database's tables, could be quite lengthy. Failures reported in
    // a course of this operation will not affect the "success" status of
    // the publishing request since.
    if (rowCountersDeployAtQserv) {
        bool const forceRescan = true;  // Since doing the scan for the first time.
        for (auto&& tableName : database.tables()) {
            auto const table = database.findTable(tableName);
            if (table.isPublished) continue;
            json const errorExt = _scanTableStatsImpl(
                    database.name, table.name, ChunkOverlapSelector::CHUNK_AND_OVERLAP,
                    SqlRowStatsJob::StateUpdatePolicy::ENABLED, rowCountersDeployAtQserv, forceRescan,
                    allWorkers, config->get<int>("controller", "ingest-priority-level"));
            if (!errorExt.empty()) {
                throw HttpError(__func__, "Table rows scanning/deployment failed.", errorExt);
            }
        }
    }
    _grantDatabaseAccess(database, allWorkers);
    _enableDatabase(database, allWorkers);
    _createMissingChunkTables(database, allWorkers);
    _removeMySQLPartitions(database, allWorkers);

    // Finalize setting the database in Qserv master to make the new catalog
    // visible to Qserv users.
    _publishDatabaseInMaster(database);

    // Change database status so that it would be seen by the Qserv synchronization
    // algorithm (job) run on the next step. Otherwise users would have to wait
    // for the next synchronization cycle of the Master Replication Controller
    // which would synchronize chunks between the Replication System and Qserv
    // workers.
    json result;
    result["database"] = config->publishDatabase(database.name).toJson();

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    auto const error = reconfigureWorkers(database, allWorkers, workerReconfigTimeoutSec());
    if (!error.empty()) throw HttpError(__func__, error);

    // Run the chunks scanner to ensure new chunks are registered in the persistent
    // store of the Replication system and synchronized with the Qserv workers.
    // The (fixing, re-balancing, replicating, etc.) will be taken care of by
    // the Replication system.
    _qservSync(database, allWorkers);

    ControllerEvent event;
    event.status = "PUBLISH DATABASE";
    event.kvInfo.emplace_back("database", database.name);
    logEvent(event);

    return result;
}

json HttpIngestModule::_deleteDatabase() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const cssAccess = qservCssAccess();
    auto const config = controller()->serviceProvider()->config();
    bool const allWorkers = true;
    auto const databaseName = params().at("database");

    debug(__func__, "database=" + databaseName);

    auto database = config->databaseInfo(databaseName);
    if (database.isPublished) {
        if (!isAdmin()) {
            throw HttpError(__func__, "deleting published databases requires administrator's privileges.");
        }
    }

    // Get the names of the 'director' tables either from the Replication/Ingest system's
    // configuration, or from CSS. It's okay not to have those tables if they weren't yet
    // created during the initial catalog ingest.
    // NOTE: Qserv allows more than one 'director' table.
    set<string> directorTables;
    for (auto&& tableName : database.directorTables()) {
        directorTables.insert(tableName);
    }
    if (cssAccess->containsDb(database.name)) {
        for (auto const tableName : cssAccess->getTableNames(database.name)) {
            auto const partTableParams = cssAccess->getPartTableParams(database.name, tableName);
            if (!partTableParams.dirTable.empty()) directorTables.insert(partTableParams.dirTable);
        }
    }

    // Remove related database entries from czar's MySQL if anything is still there
    if (cssAccess->containsDb(database.name)) {
        cssAccess->dropDb(database.name);
    }
    ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
    QueryGenerator const g(h.conn);
    h.conn->executeInOwnTransaction([&, func = __func__](decltype(h.conn) conn) {
        bool const ifExists = true;
        conn->execute(g.dropDb(database.name, ifExists));
        auto const emptyChunkListTable = css::DbInterfaceMySql::getEmptyChunksTableName(database.name);
        conn->execute(g.dropTable(g.id("qservCssData", emptyChunkListTable), ifExists));
        for (auto const tableName : directorTables) {
            string const query = g.dropTable(
                    g.id("qservMeta", directorIndexTableName(database.name, tableName)), ifExists);
            conn->execute(query);
        }
        for (auto const tableName : database.tables()) {
            try {
                string const query =
                        g.dropTable(g.id("qservMeta", rowCountersTable(database.name, tableName)), ifExists);
                conn->execute(query);
            } catch (invalid_argument const& ex) {
                // This exception may be thrown by the table name generator if
                // it couldn't build a correct name due to MySQL limitations.
                error(func, ex.what());
            }
        }
    });

    // Delete entries (if any still exist) for database and its chunks from worker
    // metadata tables. This will prevents Qserv workers from publishing the those
    // as XROOTD "resources".
    // NOTE: Ignore any errors should any be reported by the job.
    string const noParentJobId;
    auto const disableDbJob =
            SqlDisableDbJob::create(database.name, allWorkers, controller(), noParentJobId, nullptr,
                                    config->get<int>("controller", "catalog-management-priority-level"));
    disableDbJob->start();
    logJobStartedEvent(SqlDisableDbJob::typeName(), disableDbJob, database.family);
    disableDbJob->wait();
    logJobFinishedEvent(SqlDisableDbJob::typeName(), disableDbJob, database.family);

    // Delete database entries at workers
    auto const deleteDbJob =
            SqlDeleteDbJob::create(database.name, allWorkers, controller(), noParentJobId, nullptr,
                                   config->get<int>("controller", "catalog-management-priority-level"));
    deleteDbJob->start();
    logJobStartedEvent(SqlDeleteDbJob::typeName(), deleteDbJob, database.family);
    deleteDbJob->wait();
    logJobFinishedEvent(SqlDeleteDbJob::typeName(), deleteDbJob, database.family);

    string error = ::jobCompletionErrorIfAny(deleteDbJob, "database deletion failed");
    if (!error.empty()) throw HttpError(__func__, error);

    // Remove database entry from the Configuration. This will also eliminate all
    // dependent metadata, such as replicas info
    config->deleteDatabase(database.name);

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    error = reconfigureWorkers(database, allWorkers, workerReconfigTimeoutSec());
    if (!error.empty()) throw HttpError(__func__, error);

    return json::object();
}

json HttpIngestModule::_getTables() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseName = params().at("database");

    debug(__func__, "database=" + databaseName);

    auto const database = config->databaseInfo(databaseName);

    json tablesJson = json::array();
    for (auto&& tableName : database.partitionedTables()) {
        tablesJson.push_back({{"name", tableName}, {"is_partitioned", 1}});
    }
    for (auto&& tableName : database.regularTables()) {
        tablesJson.push_back({{"name", tableName}, {"is_partitioned", 0}});
    }
    return json::object({{"tables", tablesJson}});
}

json HttpIngestModule::_addTable() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    TableInfo table;
    table.database = body().required<string>("database");
    table.name = body().required<string>("table");
    table.isPartitioned = body().required<int>("is_partitioned") != 0;
    table.directorTable = DirectorTableRef(body().optional<string>("director_table", ""),
                                           body().optional<string>("director_key", ""));
    table.directorTable2 = DirectorTableRef(body().optional<string>("director_table2", ""),
                                            body().optional<string>("director_key2", ""));
    table.latitudeColName = body().optional<string>("latitude_key", "");
    table.longitudeColName = body().optional<string>("longitude_key", "");
    table.flagColName = body().optional<string>("flag", "");
    table.angSep = body().optional<double>("ang_sep", 0);

    auto const schema = body().required<json>("schema");

    debug(__func__, "database=" + table.database);
    debug(__func__, "table=" + table.name);
    debug(__func__, "is_partitioned=" + bool2str(table.isPartitioned));
    debug(__func__, "director=" + table.directorTable.databaseTableName());
    debug(__func__, "director_key=" + table.directorTable.primaryKeyColumn());
    debug(__func__, "director2=" + table.directorTable2.databaseTableName());
    debug(__func__, "director_key2=" + table.directorTable2.primaryKeyColumn());
    debug(__func__, "latitude_key=" + table.latitudeColName);
    debug(__func__, "longitude_key=" + table.longitudeColName);
    debug(__func__, "flag=" + table.flagColName);
    debug(__func__, "ang_sep=" + to_string(table.angSep));
    debug(__func__, "schema=" + schema.dump());

    auto const config = controller()->serviceProvider()->config();
    auto database = config->databaseInfo(table.database);
    if (database.isPublished) throw HttpError(__func__, "the database is already published");
    if (database.tableExists(table.name)) throw HttpError(__func__, "table already exists");

    // Translate table schema
    if (schema.is_null()) throw HttpError(__func__, "table schema is empty");
    if (!schema.is_array()) throw HttpError(__func__, "table schema is not defined as an array");

    // The name of a special column for the super-transaction-based ingest.
    // Always insert this column as the very first one into the schema.
    table.columns.emplace_front(_partitionByColumn, _partitionByColumnType);

    for (auto&& column : schema) {
        if (!column.is_object()) {
            throw HttpError(__func__, "columns definitions in table schema are not JSON objects");
        }
        if (0 == column.count("name")) {
            string const msg =
                    "column attribute 'name' is missing in table schema for "
                    "column number: " +
                    to_string(table.columns.size() + 1);
            throw HttpError(__func__, msg);
        }
        string colName = column["name"];
        if (0 == column.count("type")) {
            string const msg =
                    "column attribute 'type' is missing in table schema for "
                    "column number: " +
                    to_string(table.columns.size() + 1);
            throw HttpError(__func__, msg);
        }
        string colType = column["type"];

        if (_partitionByColumn == colName) {
            string const msg = "reserved column '" + _partitionByColumn + "' is not allowed";
            throw HttpError(__func__, msg);
        }
        table.columns.emplace_back(colName, colType);
    }

    // Register table in the Configuration
    json result;
    database = config->addTable(table);
    result["database"] = database.toJson();
    table = database.findTable(table.name);

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
    tables.push_back(table.name);
    if (table.isPartitioned) {
        unsigned int const chunk = lsst::qserv::DUMMY_CHUNK;
        bool const overlap = true;
        tables.push_back(ChunkedTable(table.name, chunk, !overlap).name());
        tables.push_back(ChunkedTable(table.name, chunk, overlap).name());
    }
    for (auto&& thisTableName : tables) {
        auto const job =
                SqlCreateTableJob::create(database.name, thisTableName, engine, _partitionByColumn,
                                          table.columns, allWorkers, controller(), noParentJobId, nullptr,
                                          config->get<int>("controller", "ingest-priority-level"));
        job->start();
        logJobStartedEvent(SqlCreateTableJob::typeName(), job, database.family);
        job->wait();
        logJobFinishedEvent(SqlCreateTableJob::typeName(), job, database.family);

        string const error =
                ::jobCompletionErrorIfAny(job, "table creation failed for: '" + thisTableName + "'");
        if (!error.empty()) throw HttpError(__func__, error);
    }

    // Create the "director" index table using an updated version of
    // the database descriptor.
    //
    // This operation can be vetoed by a catalog ingest workflow at the database
    // registration time.
    if (autoBuildDirectorIndex(database.name)) {
        if (table.isDirector) {
            _createDirectorIndex(config->databaseInfo(database.name), table.name);
        }
    }

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    string const error = reconfigureWorkers(database, allWorkers, workerReconfigTimeoutSec());
    if (!error.empty()) throw HttpError(__func__, error);

    return result;
}

json HttpIngestModule::_deleteTable() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const cssAccess = qservCssAccess();
    auto const config = controller()->serviceProvider()->config();
    bool const allWorkers = true;
    auto const databaseName = params().at("database");
    auto const tableName = params().at("table");

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);

    auto database = config->databaseInfo(databaseName);
    auto table = database.findTable(tableName);

    if (database.isPublished) {
        if (!isAdmin()) {
            throw HttpError(__func__,
                            "deleting tables of published databases requires administrator's"
                            " privileges.");
        }
    }

    // Remove table entry from czar's databases if it's still there
    try {
        if (cssAccess->containsDb(database.name)) {
            if (cssAccess->containsTable(database.name, table.name)) {
                cssAccess->dropTable(database.name, table.name);
            }
        }
    } catch (css::NoSuchTable const& ex) {
        // The table may have already been deleted by another request while this one
        // was checking for the table status in the CSS.
        ;
    }

    ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
    QueryGenerator const g(h.conn);
    h.conn->executeInOwnTransaction([&, func = __func__](decltype(h.conn) conn) {
        // Remove table entry from czar's MySQL
        bool const ifExists = true;
        conn->execute(g.dropTable(g.id(database.name, table.name), ifExists));
        // Remove the director index (if any)
        if (table.isDirector) {
            string const query = g.dropTable(
                    g.id("qservMeta", directorIndexTableName(database.name, table.name)), ifExists);
            conn->execute(query);
        }
        // Remove the row counters table (if any)
        try {
            string const query =
                    g.dropTable(g.id("qservMeta", rowCountersTable(database.name, table.name)), ifExists);
            conn->execute(query);
        } catch (invalid_argument const& ex) {
            // This exception may be thrown by the table name generator if
            // it couldn't build a correct name due to MySQL limitations.
            error(func, ex.what());
        }
    });

    // Delete table entries at workers
    string const noParentJobId;
    auto const job = SqlDeleteTableJob::create(
            database.name, table.name, allWorkers, controller(), noParentJobId, nullptr,
            config->get<int>("controller", "catalog-management-priority-level"));
    job->start();
    logJobStartedEvent(SqlDeleteTableJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(SqlDeleteTableJob::typeName(), job, database.family);

    string error = ::jobCompletionErrorIfAny(job, "table deletion failed");
    if (!error.empty()) throw HttpError(__func__, error);

    // Remove table entry from the Configuration. This will also eliminate all
    // dependent metadata, such as replicas info
    config->deleteTable(database.name, table.name);

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    error = reconfigureWorkers(database, allWorkers, workerReconfigTimeoutSec());
    if (!error.empty()) throw HttpError(__func__, error);

    return json::object();
}

json HttpIngestModule::_scanTableStats() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseName = body().required<string>("database");
    auto const tableName = body().required<string>("table");
    auto const overlapSelector =
            str2overlapSelector(body().optional<string>("overlap_selector", "CHUNK_AND_OVERLAP"));
    auto const rowCountersStateUpdatePolicy = SqlRowStatsJob::str2policy(
            body().optional<string>("row_counters_state_update_policy", "DISABLED"));
    bool const rowCountersDeployAtQserv = body().optional<int>("row_counters_deploy_at_qserv", 0) != 0;
    bool const forceRescan = body().optional<int>("force_rescan", 0) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
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
            databaseName, tableName, overlapSelector, rowCountersStateUpdatePolicy, rowCountersDeployAtQserv,
            forceRescan, allWorkers, config->get<int>("controller", "catalog-management-priority-level"));
    if (!errorExt.empty()) {
        throw HttpError(__func__, "Table rows scanning/deployment failed.", errorExt);
    }
    return json::object();
}

json HttpIngestModule::_scanTableStatsImpl(string const& databaseName, string const& tableName,
                                           ChunkOverlapSelector overlapSelector,
                                           SqlRowStatsJob::StateUpdatePolicy stateUpdatePolicy,
                                           bool deployAtQserv, bool forceRescan, bool allWorkers,
                                           int priority) {
    TransactionId const transactionId = 0;  // All transactions will be used.

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(tableName);

    // Check if an optional optimization of not re-scanning the counters would
    // be possible in the given context.
    bool scanRequired = forceRescan;
    if (!scanRequired) {
        auto const stats = databaseServices->tableRowStats(database.name, table.name, transactionId);
        if (stats.entries.size() == 0) {
            scanRequired = true;
            debug(__func__, "scan required since no entries exist for " + database.name + "." + table.name);
        } else if (table.isPartitioned) {
            // Get a collection of all (but th especial one) chunks that have been
            // registered for the database and turn it into a set.
            bool const enabledWorkersOnly = !allWorkers;
            vector<unsigned int> allChunks;
            databaseServices->findDatabaseChunks(allChunks, database.name, enabledWorkersOnly);
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
                debug(__func__, "scan required for " + database.name + "." + table.name +
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
        auto const job = SqlRowStatsJob::create(database.name, table.name, overlapSelector, stateUpdatePolicy,
                                                allWorkers, controller(), noParentJobId, nullptr, priority);
        job->start();
        logJobStartedEvent(SqlRowStatsJob::typeName(), job, database.family);
        job->wait();
        logJobFinishedEvent(SqlRowStatsJob::typeName(), job, database.family);

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
        auto const stats = databaseServices->tableRowStats(database.name, table.name, transactionId);

        // Entries for the partitioned and regular tables will be filtered and
        // processed differently. In case of the former counters from the chunk
        // overlaps will be ignored and row numbers will be aggregated by chunks.
        // For the latter a single number of rows for for the chunk number 0 will
        // be computed.
        map<unsigned int, size_t> chunk2rows;
        for (auto&& e : stats.entries) {
            if (table.isPartitioned) {
                if (!e.isOverlap) chunk2rows[e.chunk] += e.numRows;
            } else {
                chunk2rows[0] += e.numRows;
            }
        }

        // Load counters into Qserv after removing all previous entries
        // for the table to ensure the clean state.
        try {
            ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
            QueryGenerator const g(h.conn);
            string const countersTable = rowCountersTable(database.name, table.name);
            bool ifNotExists = true;
            list<SqlColDef> const columns = {SqlColDef{"chunk", "INT UNSIGNED NOT NULL"},
                                             SqlColDef{"num_rows", "BIGINT UNSIGNED DEFAULT 0"}};
            list<string> const keys = {g.packTableKey("UNIQUE KEY", "", "chunk")};
            string const engine = "InnoDB";
            string const comment =
                    "Row counters for the internal tables."
                    " The table is supposed to be populated by the ingest system when"
                    " publishing the catalog, or afterwards by the table scanner.";
            vector<string> queries;
            queries.emplace_back(g.createTable(countersTable, ifNotExists, columns, keys, engine, comment));
            queries.emplace_back(g.delete_(countersTable));
            for (auto&& itr : chunk2rows) {
                unsigned int const chunk = itr.first;
                size_t const numRows = itr.second;
                queries.emplace_back(g.insert(countersTable, chunk, numRows));
            }
            h.conn->executeInOwnTransaction([&queries](decltype(h.conn) conn) {
                for (auto&& query : queries) {
                    conn->execute(query);
                }
            });
        } catch (exception const& ex) {
            string const msg = "Failed to load/update row counters for table '" + table.name +
                               "' of database '" + database.name + "' into Qserv, ex: " + string(ex.what());
            error(__func__, msg);
            return json::object({{"operation", "Deploy table row counters in Qserv."}, {"error", msg}});
        }
    }
    return json::object();
}

json HttpIngestModule::_deleteTableStats() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const table = _getTableInfo(__func__);
    bool const qservOnly = body().optional<int>("qserv_only", 0) != 0;
    auto const overlapSelector =
            str2overlapSelector(body().optional<string>("overlap_selector", "CHUNK_AND_OVERLAP"));
    debug(__func__, "qserv_only=" + bool2str(qservOnly));
    debug(__func__, "overlap_selector=" + overlapSelector2str(overlapSelector));

    try {
        ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
        QueryGenerator const g(h.conn);
        bool const ifExists = true;
        string const query = g.dropTable(rowCountersTable(table.database, table.name), ifExists);
        h.conn->executeInOwnTransaction([&query](decltype(h.conn) conn) { conn->execute(query); });
    } catch (exception const& ex) {
        string const msg = "Failed to delete metadata table with counters for table '" + table.name +
                           "' of database '" + table.database + "' from Qserv, ex: " + string(ex.what());
        error(__func__, msg);
        throw HttpError(__func__, msg,
                        json::object({{"operation", "Deploy table row counters in Qserv."}, {"error", msg}}));
    }

    // Delete stats from the Replication system's persistent state if requested.
    if (!qservOnly) {
        auto const databaseServices = controller()->serviceProvider()->databaseServices();
        databaseServices->deleteTableRowStats(table.database, table.name, overlapSelector);
    }
    return json::object();
}

json HttpIngestModule::_tableStats() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const table = _getTableInfo(__func__);
    TransactionId const transactionId = 0;  // Aggregate counters ingested across all transactions.
    return controller()
            ->serviceProvider()
            ->databaseServices()
            ->tableRowStats(table.database, table.name, transactionId)
            .toJson();
}

json HttpIngestModule::_buildEmptyChunksList() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const databaseName = body().required<string>("database");
    bool const force = body().optional<int>("force", 0) != 0;
    bool const tableImpl = body().optional<int>("table_impl", 0) != 0;

    debug(__func__, "database=" + databaseName);
    debug(__func__, "force=" + bool2str(force));
    debug(__func__, "table_impl=" + bool2str(tableImpl));

    return _buildEmptyChunksListImpl(databaseName, force, tableImpl);
}

json HttpIngestModule::_getRegular() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    auto const config = controller()->serviceProvider()->config();
    auto const database = getDatabaseInfo(__func__);

    json resultLocations = json::array();
    for (auto&& workerName : config->workers()) {
        auto const worker = config->workerInfo(workerName);
        json resultLocation;
        resultLocation["worker"] = worker.name;
        resultLocation["host"] = worker.loaderHost.addr;
        resultLocation["host_name"] = worker.loaderHost.name;
        resultLocation["port"] = worker.loaderPort;
        resultLocation["http_host"] = worker.httpLoaderHost.addr;
        resultLocation["http_host_name"] = worker.httpLoaderHost.name;
        resultLocation["http_port"] = worker.httpLoaderPort;
        resultLocations.push_back(resultLocation);
    }
    json result;
    result["locations"] = resultLocations;
    return result;
}

TableInfo HttpIngestModule::_getTableInfo(string const& func) {
    auto const databaseName = params().at("database");
    auto const tableName = params().at("table");
    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    return controller()->serviceProvider()->config()->databaseInfo(databaseName).findTable(tableName);
}

void HttpIngestModule::_grantDatabaseAccess(DatabaseInfo const& database, bool allWorkers) const {
    debug(__func__);

    string const noParentJobId;
    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlGrantAccessJob::create(
            database.name, config->get<string>("database", "qserv-master-user"), allWorkers, controller(),
            noParentJobId, nullptr, config->get<int>("controller", "ingest-priority-level"));
    job->start();
    logJobStartedEvent(SqlGrantAccessJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(SqlGrantAccessJob::typeName(), job, database.family);

    string const error = ::jobCompletionErrorIfAny(job, "grant access to a database failed");
    if (!error.empty()) throw HttpError(__func__, error);
}

void HttpIngestModule::_enableDatabase(DatabaseInfo const& database, bool allWorkers) const {
    debug(__func__);

    string const noParentJobId;
    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlEnableDbJob::create(database.name, allWorkers, controller(), noParentJobId, nullptr,
                                            config->get<int>("controller", "ingest-priority-level"));
    job->start();
    logJobStartedEvent(SqlEnableDbJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(SqlEnableDbJob::typeName(), job, database.family);

    string const error = ::jobCompletionErrorIfAny(job, "enabling database failed");
    if (!error.empty()) throw HttpError(__func__, error);
}

void HttpIngestModule::_createMissingChunkTables(DatabaseInfo const& database, bool allWorkers) const {
    debug(__func__);

    string const engine = "MyISAM";
    string const noParentJobId;

    for (auto&& tableName : database.partitionedTables()) {
        auto const& table = database.findTable(tableName);
        // Skip tables that have been published.
        if (table.isPublished) continue;
        if (table.columns.empty()) {
            throw HttpError(__func__, "schema is empty for table: '" + table.name + "'");
        }
        auto const job = SqlCreateTablesJob::create(
                database.name, table.name, engine, _partitionByColumn, table.columns, allWorkers,
                controller(), noParentJobId, nullptr,
                controller()->serviceProvider()->config()->get<int>("controller", "ingest-priority-level"));
        job->start();
        logJobStartedEvent(SqlCreateTablesJob::typeName(), job, database.family);
        job->wait();
        logJobFinishedEvent(SqlCreateTablesJob::typeName(), job, database.family);

        string const error =
                ::jobCompletionErrorIfAny(job, "table creation failed for: '" + table.name + "'");
        if (!error.empty()) throw HttpError(__func__, error);
    }
}

void HttpIngestModule::_removeMySQLPartitions(DatabaseInfo const& database, bool allWorkers) const {
    debug(__func__);

    // Ignore tables which may have already been processed at a previous attempt
    // of running this algorithm.
    bool const ignoreNonPartitioned = true;
    string const noParentJobId;
    string error;
    for (auto const tableName : database.tables()) {
        auto const& table = database.findTable(tableName);
        // Skip tables that have been published.
        if (table.isPublished) continue;
        auto const job = SqlRemoveTablePartitionsJob::create(
                database.name, table.name, allWorkers, ignoreNonPartitioned, controller(), noParentJobId,
                nullptr,
                controller()->serviceProvider()->config()->get<int>("controller", "ingest-priority-level"));
        job->start();
        logJobStartedEvent(SqlRemoveTablePartitionsJob::typeName(), job, database.family);
        job->wait();
        logJobFinishedEvent(SqlRemoveTablePartitionsJob::typeName(), job, database.family);

        error += ::jobCompletionErrorIfAny(job, "MySQL partitions removal failed for database: " +
                                                        database.name + ", table: " + table.name);
    }
    if (!error.empty()) throw HttpError(__func__, error);
}

void HttpIngestModule::_publishDatabaseInMaster(DatabaseInfo const& database) const {
    auto const config = controller()->serviceProvider()->config();
    auto const databaseFamilyInfo = config->databaseFamilyInfo(database.family);

    // Connect to the master database as user "root".
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.
    {
        ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
        QueryGenerator const g(h.conn);

        // SQL statements to be executed
        vector<string> statements;

        // Statements for creating the database & table entries
        bool const ifNotExists = true;
        statements.push_back(g.createDb(database.name, ifNotExists));
        for (auto const& tableName : database.tables()) {
            auto const& table = database.findTable(tableName);
            // Skip tables that have been published.
            if (table.isPublished) continue;
            string const query = g.createTable(table.database, table.name, ifNotExists, table.columns);
            statements.push_back(query);
        }

        // Statements for granting SELECT authorizations on all tables of
        // the new database to the configured Qserv account.
        string const query = g.grant("ALL", database.name,
                                     config->get<string>("database", "qserv-master-user"), "localhost");
        statements.push_back(query);
        h.conn->executeInOwnTransaction([&statements](decltype(h.conn) conn) {
            for (auto const& query : statements) {
                conn->execute(query);
            }
        });
    }

    // Register the database, tables and the partitioning scheme at CSS
    auto const cssAccess = qservCssAccess();
    if (!cssAccess->containsDb(database.name)) {
        // First, try to find another database within the same family which
        // has already been published, and the one is found then use it
        // as a template when registering the database in CSS.
        //
        // Otherwise, create a new database using an extended CSS API which
        // will allocate a new partitioning identifier.
        bool const allDatabases = false;
        bool const isPublished = true;
        auto const databases = config->databases(databaseFamilyInfo.name, allDatabases, isPublished);
        if (!databases.empty()) {
            auto const templateDatabase = databases.front();
            cssAccess->createDbLike(database.name, templateDatabase);
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
            cssAccess->createDb(database.name, stripingParams, storageClass, releaseStatus);
        }
    }

    // Register tables which still hasn't been registered in CSS
    for (auto const& tableName : database.regularTables()) {
        auto const& table = database.findTable(tableName);
        // Skip tables that have been published.
        if (table.isPublished) continue;
        if (!cssAccess->containsTable(database.name, table.name)) {
            // Neither of those groups of parameters apply to the regular tables,
            // so we're leaving them default constructed.
            css::PartTableParams const partParams;
            css::ScanTableParams const scanParams;
            cssAccess->createTable(database.name, table.name, table.schema4css(), partParams, scanParams);
        }
    }
    for (auto const& tableName : database.partitionedTables()) {
        auto const& table = database.findTable(tableName);
        // Skip tables that have been published.
        if (table.isPublished) continue;
        if (!cssAccess->containsTable(database.name, table.name)) {
            if (table.isRefMatch) {
                css::MatchTableParams const matchParams(
                        table.directorTable.databaseTableName(), table.directorTable.primaryKeyColumn(),
                        table.directorTable2.databaseTableName(), table.directorTable2.primaryKeyColumn(),
                        table.flagColName, table.angSep);
                cssAccess->createMatchTable(database.name, table.name, table.schema4css(), matchParams);
            } else {
                // These parameters need to be set correctly for the 'director' and dependent
                // tables to avoid confusing Qserv query analyzer. Also note, that the 'overlap'
                // is set to be the same for all 'director' tables of the database family.
                double const overlap = table.isDirector ? databaseFamilyInfo.overlap : 0;
                bool const isPartitioned = true;
                bool const hasSubChunks = table.isDirector;
                css::PartTableParams const partParams(database.name, table.directorTable.tableName(),
                                                      table.directorTable.primaryKeyColumn(),
                                                      table.latitudeColName, table.longitudeColName, overlap,
                                                      isPartitioned, hasSubChunks);
                bool const lockInMem = true;
                int const scanRating = 1;
                css::ScanTableParams const scanParams(lockInMem, scanRating);

                cssAccess->createTable(database.name, table.name, table.schema4css(), partParams, scanParams);
            }
        }
    }

    bool const forceRebuild = true;
    bool const tableImpl = true;
    _buildEmptyChunksListImpl(database.name, forceRebuild, tableImpl);
}

json HttpIngestModule::_buildEmptyChunksListImpl(string const& databaseName, bool force,
                                                 bool tableImpl) const {
    debug(__func__);

    auto const databaseServices = controller()->serviceProvider()->databaseServices();
    auto const config = controller()->serviceProvider()->config();

    auto const database = config->databaseInfo(databaseName);
    if (database.isPublished) {
        throw invalid_argument("database is already published");
    }

    // Get a collection of all possible chunks which are allowed to be present
    // in the database given its partitioning scheme.
    auto const family = config->databaseFamilyInfo(database.family);
    lsst::sphgeom::Chunker const chunker(family.numStripes, family.numSubStripes);
    auto const allChunks = chunker.getAllChunks();

    // Get the numbers of chunks ingested into the database. They will be excluded
    // from the "empty chunk list".
    set<unsigned int> ingestedChunks;
    {
        bool const enabledWorkersOnly = true;
        vector<unsigned int> chunks;
        databaseServices->findDatabaseChunks(chunks, database.name, enabledWorkersOnly);

        for (auto chunk : chunks) ingestedChunks.insert(chunk);
    }

    if (tableImpl) {
        ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
        QueryGenerator const g(h.conn);
        string const tableName = css::DbInterfaceMySql::getEmptyChunksTableName(database.name);
        vector<string> statements;
        if (force) {
            bool const ifExists = true;
            statements.push_back(g.dropTable(tableName, ifExists));
        }
        statements.push_back(css::DbInterfaceMySql::getEmptyChunksSchema(database.name));
        for (auto&& chunk : allChunks) {
            if (!ingestedChunks.count(chunk)) {
                statements.push_back(g.insert(tableName, chunk));
            }
        }
        h.conn->executeInOwnTransaction([&statements](decltype(h.conn) conn) {
            for (auto const& query : statements) {
                conn->execute(query);
            }
        });
    } else {
        auto const file = "empty_" + database.name + ".txt";
        auto const filePath = fs::path(config->get<string>("controller", "empty-chunks-dir")) / file;

        if (!force) {
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
        if (!ofs.good()) {
            throw runtime_error("failed to create/open file: " + filePath.string());
        }
        for (auto&& chunk : allChunks) {
            if (!ingestedChunks.count(chunk)) {
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

void HttpIngestModule::_createDirectorIndex(DatabaseInfo const& database,
                                            string const& directorTableName) const {
    auto const& table = database.findTable(directorTableName);
    if (!table.isDirector) {
        throw logic_error("table '" + table.name + "' is not configured in database '" + database.name +
                          "' as the director table");
    }
    if (table.directorTable.primaryKeyColumn().empty()) {
        throw logic_error("director key of table '" + table.name + "' is not configured in database '" +
                          database.name + "'");
    }
    if (table.columns.empty()) {
        throw logic_error("no schema found for director table '" + table.name + "' of database '" +
                          database.name + "'");
    }

    // Find types of the "director" index table's columns
    string primaryKeyColumnType;
    string const chunkIdColNameType = "INT";
    string const subChunkIdColNameType = "INT";

    for (auto&& column : table.columns) {
        if (column.name == table.directorTable.primaryKeyColumn()) primaryKeyColumnType = column.type;
    }
    if (primaryKeyColumnType.empty()) {
        throw logic_error("column definition for the director key column '" +
                          table.directorTable.primaryKeyColumn() +
                          "' is missing in the director table schema for table '" + table.name +
                          "' of database '" + database.name + "'");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.
    ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    QueryGenerator const g(h.conn);
    bool const ifExists = true;
    string const dropTableQuery = g.dropTable(directorIndexTableName(database.name, table.name), ifExists);
    bool const ifNotExists = true;
    list<SqlColDef> const columns = {SqlColDef{_partitionByColumn, _partitionByColumnType},
                                     SqlColDef{table.directorTable.primaryKeyColumn(), primaryKeyColumnType},
                                     SqlColDef{lsst::qserv::CHUNK_COLUMN, chunkIdColNameType},
                                     SqlColDef{lsst::qserv::SUB_CHUNK_COLUMN, subChunkIdColNameType}};
    list<string> const keys = {
            g.packTableKey("UNIQUE KEY", "", _partitionByColumn, table.directorTable.primaryKeyColumn()),
            g.packTableKey("KEY", "", table.directorTable.primaryKeyColumn())};
    auto const config = controller()->serviceProvider()->config();
    TransactionId const transactionId = 0;
    string const createTableQuery =
            g.createTable(directorIndexTableName(database.name, table.name), ifNotExists, columns, keys,
                          config->get<string>("controller", "director-index-engine")) +
            g.partitionByList(_partitionByColumn) + g.partition(transactionId);
    h.conn->executeInOwnTransaction([&dropTableQuery, &createTableQuery](decltype(h.conn) conn) {
        conn->execute(dropTableQuery);
        conn->execute(createTableQuery);
    });
}

void HttpIngestModule::_consolidateDirectorIndex(DatabaseInfo const& database,
                                                 string const& directorTableName) const {
    auto const table = database.findTable(directorTableName);
    if (!table.isDirector) {
        throw logic_error("table '" + table.name + "' is not configured in database '" + database.name +
                          "' as the director table");
    }

    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.
    ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    QueryGenerator const g(h.conn);
    string const query =
            g.alterTable(directorIndexTableName(database.name, table.name)) + g.removePartitioning();
    h.conn->executeInOwnTransaction([&query](decltype(h.conn) conn) { conn->execute(query); });
}

void HttpIngestModule::_qservSync(DatabaseInfo const& database, bool allWorkers) const {
    debug(__func__);

    bool const saveReplicaInfo = true;
    string const noParentJobId;
    auto const findAlljob = FindAllJob::create(
            database.family, saveReplicaInfo, allWorkers, controller(), noParentJobId, nullptr,
            controller()->serviceProvider()->config()->get<int>("controller", "ingest-priority-level"));
    findAlljob->start();
    logJobStartedEvent(FindAllJob::typeName(), findAlljob, database.family);
    findAlljob->wait();
    logJobFinishedEvent(FindAllJob::typeName(), findAlljob, database.family);

    if (findAlljob->extendedState() != Job::SUCCESS) {
        throw HttpError(__func__, "replica lookup stage failed");
    }

    bool const force = false;
    auto const qservSyncJob =
            QservSyncJob::create(database.family, force, qservSyncTimeoutSec(), controller());
    qservSyncJob->start();
    logJobStartedEvent(QservSyncJob::typeName(), qservSyncJob, database.family);
    qservSyncJob->wait();
    logJobFinishedEvent(QservSyncJob::typeName(), qservSyncJob, database.family);

    if (qservSyncJob->extendedState() != Job::SUCCESS) {
        throw HttpError(__func__, "Qserv synchronization failed");
    }
}

}  // namespace lsst::qserv::replica
