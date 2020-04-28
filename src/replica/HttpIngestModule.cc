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
#include "replica/ServiceManagementJob.h"
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

// LSST headers
#include "lsst/sphgeom/Chunker.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace fs = boost::filesystem;

namespace {

string jobCompletionErrorIfAny(SqlJob::Ptr const& job,
                               string const& prefix) {
    string error;
    if (job->extendedState() != Job::ExtendedState::SUCCESS) {
        auto const& resultData = job->getResultData();
        for (auto&& itr: resultData.resultSets) {
            auto&& worker = itr.first;
            for (auto&& result: itr.second) {
                if (result.hasErrors()) {
                    error += prefix + ", worker: " + worker + ",  error: " +
                             result.firstError() + " ";
                }
            }
        }
    }
    return error;
}
}

namespace lsst {
namespace qserv {
namespace replica {

string const HttpIngestModule::_partitionByColumn = "qserv_trans_id";
string const HttpIngestModule::_partitionByColumnType = "INT NOT NULL";


void HttpIngestModule::process(Controller::Ptr const& controller,
                               string const& taskName,
                               HttpProcessorConfig const& processorConfig,
                               qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp,
                               string const& subModuleName,
                               HttpModule::AuthType const authType) {
    HttpIngestModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpIngestModule::HttpIngestModule(Controller::Ptr const& controller,
                                   string const& taskName,
                                   HttpProcessorConfig const& processorConfig,
                                   qhttp::Request::Ptr const& req,
                                   qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpIngestModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "DATABASES") return _getDatabases();
    else if (subModuleName == "ADD-DATABASE") return _addDatabase();
    else if (subModuleName == "PUBLISH-DATABASE") return _publishDatabase();
    else if (subModuleName == "DELETE-DATABASE") return _deleteDatabase();
    else if (subModuleName == "TABLES") return _getTables();
    else if (subModuleName == "ADD-TABLE") return _addTable();
    else if (subModuleName == "DELETE-TABLE") return _deleteTable();
    else if (subModuleName == "BUILD-CHUNK-LIST") return _buildEmptyChunksList();
    else if (subModuleName == "REGULAR") return _getRegular();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpIngestModule::_getDatabases() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    // Leaving this name empty would result in scaning databases across all known
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
    for (auto const database: config->databases(family, allDatabases, isPublished )) {
        auto const databaseInfo = config->databaseInfo(database);
        databasesJson.push_back({
            {"name", databaseInfo.name},
            {"family", databaseInfo.family},
            {"is_published", databaseInfo.isPublished ? 1: 0}
        });
    }
    return json::object({{"databases", databasesJson}});
}


json HttpIngestModule::_addDatabase() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseServices = controller()->serviceProvider()->databaseServices();

    DatabaseInfo databaseInfo;
    string const database = body().required<string>("database");

    auto const numStripes    = body().required<unsigned int>("num_stripes");
    auto const numSubStripes = body().required<unsigned int>("num_sub_stripes");
    auto const overlap       = body().required<double>("overlap");
    auto const enableAutoBuildSecondaryIndex = body().optional<unsigned int>("auto_build_secondary_index", 1);
    auto const enableLocalLoadSecondaryIndex = body().optional<unsigned int>("local_load_secondary_index", 0);

    debug(__func__, "database="      + database);
    debug(__func__, "numStripes="    + to_string(numStripes));
    debug(__func__, "numSubStripes=" + to_string(numSubStripes));
    debug(__func__, "overlap="       + to_string(overlap));
    debug(__func__, "enableAutoBuildSecondaryIndex=" + to_string(enableAutoBuildSecondaryIndex ? 1 : 0));
    debug(__func__, "enableLocalLoadSecondaryIndex=" + to_string(enableLocalLoadSecondaryIndex ? 1 : 0));

    if (overlap < 0) throw HttpError(__func__, "overlap can't have a negative value");

    // Find an appropriate database family for the database. If none
    // found then create a new one named after the database.

    string family;
    for (auto&& candidateFamily: config->databaseFamilies()) {
        auto const familyInfo = config->databaseFamilyInfo(candidateFamily);
        if ((familyInfo.numStripes == numStripes) and (familyInfo.numSubStripes == numSubStripes)
            and (abs(familyInfo.overlap - overlap) <= numeric_limits<double>::epsilon())) {
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
    auto const job = SqlCreateDbJob::create(
        database,
        allWorkers,
        controller()
    );
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
    databaseServices->saveIngestParam(
            databaseInfo.name, "secondary-index", "auto-build",
            to_string(enableAutoBuildSecondaryIndex ? 1 : 0));
    databaseServices->saveIngestParam(
            databaseInfo.name, "secondary-index", "local-load",
            to_string(enableLocalLoadSecondaryIndex ? 1 : 0));

    // Tell workers to reload their configurations
    error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
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

    bool const consolidateSecondayIndex = query().optionalBool("consolidate_secondary_index", false);

    debug(__func__, "database=" + database);
    debug(__func__, "consolidate_secondary_index=" + to_string(consolidateSecondayIndex ? 1 : 0));

    auto const databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) throw HttpError(__func__, "the database is already published");

    // Scan super-transactions to make sure none is still open
    for (auto&& t: databaseServices->transactions(databaseInfo.name)) {
        if (t.state == TransactionInfo::STARTED) {
            throw HttpError(__func__, "database has uncommitted transactions");
        }
    }

    // Refuse the operation if no chunks were registered
    vector<unsigned int> chunks;
    databaseServices->findDatabaseChunks(chunks, databaseInfo.name, allWorkers);
    if (chunks.empty()) throw HttpError(__func__, "the database doesn't have any chunks");

    // The operation can be vetoed by the corresponding workflow parameter requested
    // by a catalog ingest workflow at the database creation time.
    if (autoBuildSecondaryIndex(database) and consolidateSecondayIndex) {
        // This operation may take a while if the table has a large number of entries.
        _consolidateSecondaryIndex(databaseInfo);
    }
    _grantDatabaseAccess(databaseInfo, allWorkers);
    _enableDatabase(databaseInfo, allWorkers);
    _createMissingChunkTables(databaseInfo, allWorkers);
    _removeMySQLPartitions(databaseInfo, allWorkers);

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.
    auto const error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);

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
            throw HttpError(
                    __func__, "deleting published databases requires administrator's privileges.");
        }
    }

    // Get the names of the 'director' tables either from the Replication/Ingest system's
    // configuration, or from CSS. It's okay not to have those tables if they weren't yet
    // created during the initial catalog ingest.
    // NOTE: Qserv allows more than one 'director' table. 
    set<string> directorTables; 
    directorTables.insert(databaseInfo.directorTable);
    if (cssAccess->containsDb(databaseInfo.name)) {
        for (auto const table: cssAccess->getTableNames(databaseInfo.name)) {
            auto const partTableParams = cssAccess->getPartTableParams(databaseInfo.name, table);
            if (!partTableParams.dirTable.empty()) directorTables.insert(partTableParams.dirTable);
        }
    }

    // Remove related database etries from czar's MySQL if anyting is still there

    if (cssAccess->containsDb(databaseInfo.name)) {
        cssAccess->dropDb(databaseInfo.name);
    }
    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
    h.conn->executeInOwnTransaction([&databaseInfo, &directorTables](decltype(h.conn) conn) {
        conn->execute("DROP DATABASE IF EXISTS " + conn->sqlId(databaseInfo.name));
        auto const emptyChunkListTable = css::DbInterfaceMySql::getEmptyChunksTableName(databaseInfo.name); 
        conn->execute("DROP TABLE IF EXISTS " + conn->sqlId("qservCssData", emptyChunkListTable));
        for (auto const table: directorTables) {
            auto const secondaryIndexTable = databaseInfo.name + "__" + table;
            conn->execute("DROP TABLE IF EXISTS " + conn->sqlId("qservMeta", secondaryIndexTable));
        }
    });

    // Delete entries (if any still exist) for database and its chunks from worker
    // metadata tables. This will prevents Qserv workers from publishing the those
    // as XROOTD "resources".
    // NOTE: Ignore any errors should any be reported by the job.
    auto const dasableDbJob = SqlDisableDbJob::create(databaseInfo.name, allWorkers, controller());
    dasableDbJob->start();
    logJobStartedEvent(SqlDisableDbJob::typeName(), dasableDbJob, databaseInfo.family);
    dasableDbJob->wait();
    logJobFinishedEvent(SqlDisableDbJob::typeName(), dasableDbJob, databaseInfo.family);

    // Delete database entries at workers
    auto const deleteDbJob = SqlDeleteDbJob::create(databaseInfo.name, allWorkers, controller());
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
    error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
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
    for (auto&& table: databaseInfo.partitionedTables) {
        tablesJson.push_back({
            {"name", table},
            {"is_partitioned", 1}
        });
    }
    for (auto&& table: databaseInfo.regularTables) {
        tablesJson.push_back({
            {"name", table},
            {"is_partitioned", 0}
        });
    }
    return json::object({{"tables", tablesJson}});
}


json HttpIngestModule::_addTable() {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();

    auto const database      = body().required<string>("database");
    auto const table         = body().required<string>("table");
    auto const isPartitioned = body().required<int>("is_partitioned") != 0;
    auto const schema        = body().required<json>("schema");
    auto const isDirector    = body().required<int>("is_director") != 0;
    auto const directorKey   = body().optional<string>("director_key", "");
    auto const chunkIdColName    = body().optional<string>("chunk_id_key", "");
    auto const subChunkIdColName = body().optional<string>("sub_chunk_id_key", "");
    auto const latitudeColName  = body().optional<string>("latitude_key",  "");
    auto const longitudeColName = body().optional<string>("longitude_key", "");

    debug(__func__, "database="      + database);
    debug(__func__, "table="         + table);
    debug(__func__, "isPartitioned=" + bool2str(isPartitioned));
    debug(__func__, "schema="        + schema.dump());
    debug(__func__, "isDirector="    + bool2str(isDirector));
    debug(__func__, "directorKey="   + directorKey);
    debug(__func__, "chunkIdColName="    + chunkIdColName);
    debug(__func__, "subChunkIdColName=" + subChunkIdColName);
    debug(__func__, "latitudeColName="  + latitudeColName);
    debug(__func__, "longitudeColName=" + longitudeColName);

    // Make sure the database is known and it's not PUBLISHED yet

    auto databaseInfo = config->databaseInfo(database);
    if (databaseInfo.isPublished) throw HttpError(__func__, "the database is already published");

    // Make sure the table doesn't exist in the Configuration

    for (auto&& existingTable: databaseInfo.tables()) {
        if (table == existingTable) throw HttpError(__func__, "table already exists");
    }

    // Translate table schema

    if (schema.is_null()) throw HttpError( __func__, "table schema is empty");
    if (not schema.is_array()) throw HttpError(__func__, "table schema is not defined as an array");

    list<SqlColDef> columns;

    // The name of a special column for the super-transaction-based ingest.
    // Always insert this column as the very first one into the schema.
    columns.emplace_front(_partitionByColumn, _partitionByColumnType);

    for (auto&& coldef: schema) {
        if (not coldef.is_object()) {
            throw HttpError(__func__,
                    "columns definitions in table schema are not JSON objects");
        }
        if (0 == coldef.count("name")) {
            throw HttpError(__func__,
                    "column attribute 'name' is missing in table schema for "
                    "column number: " + to_string(columns.size() + 1));
        }
        string colName = coldef["name"];
        if (0 == coldef.count("type")) {
            throw HttpError(__func__,
                    "column attribute 'type' is missing in table schema for "
                    "column number: " + to_string(columns.size() + 1));
        }
        string colType = coldef["type"];

        if (_partitionByColumn == colName) {
            throw HttpError(__func__,
                    "reserved column '" + _partitionByColumn + "' is not allowed");
        }
        columns.emplace_back(colName, colType);
    }

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

    vector<string> tables;
    tables.push_back(table);
    if (isPartitioned) {
        unsigned int const chunk = lsst::qserv::DUMMY_CHUNK;
        bool const overlap = false;
        tables.push_back(ChunkedTable(table, chunk, overlap).name());
        tables.push_back(ChunkedTable(table, chunk, not overlap).name());
    }
    for (auto&& table: tables) {
        auto const job = SqlCreateTableJob::create(
            databaseInfo.name,
            table,
            engine,
            _partitionByColumn,
            columns,
            allWorkers,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlCreateTableJob::typeName(), job, databaseInfo.family);

        string const error = ::jobCompletionErrorIfAny(job, "table creation failed for: '" + table + "'");
        if (not error.empty()) throw HttpError(__func__, error);
    }

    // Register table in the Configuration

    json result;
    result["database"] = config->addTable(
        databaseInfo.name, table, isPartitioned, columns, isDirector,
        directorKey, chunkIdColName, subChunkIdColName,
        latitudeColName, longitudeColName
    ).toJson();

    // Create the secondary index table using an updated version of
    // the database descriptor.
    //
    // This operation can be vetoed by a catalog ingest workflow at the database
    // registration time.
    if (autoBuildSecondaryIndex(databaseInfo.name)) {
        if (isPartitioned and isDirector) _createSecondaryIndex(config->databaseInfo(databaseInfo.name));
    }

    // This step is needed to get workers' Configuration in-sync with its
    // persistent state.

    string const error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
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
            throw HttpError(
                    __func__, "deleting tables of published databases requires administrator's"
                    " privileges.");
        }
        auto const tableParams = cssAccess->getTableParams(databaseInfo.name, table);
        if (tableParams.partitioning.dirTable == table) {
            throw HttpError(
                    __func__, "the director table can't be deleted from the published catalog"
                    " w/o deleting the whole database.");
        }
    } else {
        // This check is done agaist the internal data structure of the Replication/Ingest System
        // since CSS is not populated before a database gets published.
        if (databaseInfo.isDirector(table)) {
            throw HttpError(
                    __func__, "the director table can't be deleted from the un-published catalog"
                    " w/o deleting the whole database.");
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
    });

    // Delete table entries at workers
    auto const job = SqlDeleteTableJob::create(databaseInfo.name, table, allWorkers, controller());
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
    error = _reconfigureWorkers(databaseInfo, allWorkers, workerReconfigTimeoutSec());
    if (not error.empty()) throw HttpError(__func__, error);

    return json::object();
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
    for (auto&& worker: config->workers()) {
        auto&& workerInfo = config->workerInfo(worker);
        json resultLocation;
        resultLocation["worker"] = workerInfo.name;
        resultLocation["host"]   = workerInfo.loaderHost;
        resultLocation["port"]   = workerInfo.loaderPort;
        resultLocation["http_host"] = workerInfo.httpLoaderHost;
        resultLocation["http_port"] = workerInfo.httpLoaderPort;
        resultLocations.push_back(resultLocation);
    }
    json result;
    result["locations"] = resultLocations;
    return result;
}


void HttpIngestModule::_grantDatabaseAccess(DatabaseInfo const& databaseInfo,
                                            bool allWorkers) const {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlGrantAccessJob::create(
        databaseInfo.name,
        config->get<string>("database", "qserv_master_user"),
        allWorkers,
        controller()
    );
    job->start();
    logJobStartedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlGrantAccessJob::typeName(), job, databaseInfo.family);

    string const error = ::jobCompletionErrorIfAny(job, "grant access to a database failed");
    if (not error.empty()) throw HttpError(__func__, error);
}


void HttpIngestModule::_enableDatabase(DatabaseInfo const& databaseInfo,
                                       bool allWorkers) const {
    debug(__func__);

    auto const config = controller()->serviceProvider()->config();
    auto const job = SqlEnableDbJob::create(
        databaseInfo.name,
        allWorkers,
        controller()
    );
    job->start();
    logJobStartedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlEnableDbJob::typeName(), job, databaseInfo.family);

    string const error = ::jobCompletionErrorIfAny(job, "enabling database failed");
    if (not error.empty()) throw HttpError(__func__, error);

}


void HttpIngestModule::_createMissingChunkTables(DatabaseInfo const& databaseInfo,
                                                 bool allWorkers) const {
    debug(__func__);

    string const engine = "MyISAM";

    for (auto&& table: databaseInfo.partitionedTables) {

        auto const columnsItr = databaseInfo.columns.find(table);
        if (columnsItr == databaseInfo.columns.cend()) {
            throw HttpError( __func__, "schema is empty for table: '" + table + "'");
        }
        auto const job = SqlCreateTablesJob::create(
            databaseInfo.name,
            table,
            engine,
            _partitionByColumn,
            columnsItr->second,
            allWorkers,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlCreateTablesJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlCreateTablesJob::typeName(), job, databaseInfo.family);

        string const error = ::jobCompletionErrorIfAny(job, "table creation failed for: '" + table + "'");
        if (not error.empty()) throw HttpError(__func__, error);
    }
}


void HttpIngestModule::_removeMySQLPartitions(DatabaseInfo const& databaseInfo,
                                              bool allWorkers) const {
    debug(__func__);

    // Ignore tables which may have already been processed at a previous attempt
    // of running this algorithm.
    bool const ignoreNonPartitioned = true;

    string error;
    for (auto const table: databaseInfo.tables()) {
        auto const job = SqlRemoveTablePartitionsJob::create(
            databaseInfo.name,
            table,
            allWorkers,
            ignoreNonPartitioned,
            controller()
        );
        job->start();
        logJobStartedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);
        job->wait();
        logJobFinishedEvent(SqlRemoveTablePartitionsJob::typeName(), job, databaseInfo.family);

        error += ::jobCompletionErrorIfAny(
                        job,
                        "MySQL partitions removal failed for database: " +
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

        statements.push_back(
            "CREATE DATABASE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name)
        );
        for (auto const& table: databaseInfo.tables()) {
            string sql = "CREATE TABLE IF NOT EXISTS " + h.conn->sqlId(databaseInfo.name) +
                    "." + h.conn->sqlId(table) + " (";
            bool first = true;
            for (auto const& coldef: databaseInfo.columns.at(table)) {
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

        statements.push_back(
            "GRANT ALL ON " + h.conn->sqlId(databaseInfo.name) + ".* TO " +
            h.conn->sqlValue(config->get<string>("database", "qserv_master_user")) + "@" +
            h.conn->sqlValue("localhost"));

        h.conn->execute([&statements](decltype(h.conn) conn) {
            conn->begin();
            for (auto const& query: statements) {
                conn->execute(query);
            }
            conn->commit();
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

            css::StripingParams const stripingParams(
                    databaseFamilyInfo.numStripes,
                    databaseFamilyInfo.numSubStripes,
                    unusedPartitioningId,
                    databaseFamilyInfo.overlap
            );
            string const storageClass = "L2";
            string const releaseStatus = "RELEASED";
            cssAccess->createDb(databaseInfo.name, stripingParams, storageClass, releaseStatus);
        }
    }

    // Register tables which still hasn't been registered in CSS
    
    for (auto const& table: databaseInfo.regularTables) {
        if (not cssAccess->containsTable(databaseInfo.name, table)) {

            // Neither of those groups of parameters apply to the regular tables,
            // so we're leaving them default constructed. 
            css::PartTableParams const partParams;
            css::ScanTableParams const scanParams;

            cssAccess->createTable(
                databaseInfo.name,
                table,
                databaseInfo.schema4css(table),
                partParams,
                scanParams
            );
        }
    }
    for (auto const& table: databaseInfo.partitionedTables) {
        if (not cssAccess->containsTable(databaseInfo.name, table)) {

            bool const isPartitioned = true;

            // These parameters need to be set correctly for the 'director' and dependent
            // tables to avoid confusing Qserv query analyzer. Also note, that the 'overlap'
            // is set to be the same for all 'director' tables of the database family.
            bool const isDirector = databaseInfo.isDirector(table);
            double const overlap = isDirector ? databaseFamilyInfo.overlap : 0;
            bool const hasSubChunks = isDirector;

            css::PartTableParams const partParams(
                databaseInfo.name,
                databaseInfo.directorTable,
                databaseInfo.directorTableKey.at(table),
                databaseInfo.latitudeColName.at(table),
                databaseInfo.longitudeColName.at(table),
                overlap,
                isPartitioned,
                hasSubChunks
            );

            bool const lockInMem = true;
            int const scanRating = 1;
            css::ScanTableParams const scanParams(lockInMem, scanRating);

            cssAccess->createTable(
                databaseInfo.name,
                table,
                databaseInfo.schema4css(table),
                partParams,
                scanParams
            );
        }
    }
    
    bool const forceRebuild = true;
    bool const tableImpl = true;
    _buildEmptyChunksListImpl(databaseInfo.name, forceRebuild, tableImpl);
}


json HttpIngestModule::_buildEmptyChunksListImpl(string const& database,
                                                 bool force,
                                                 bool tableImpl) const {
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

        for (auto chunk: chunks) ingestedChunks.insert(chunk);
    }

    if (tableImpl) {
        database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservCssData"));
        string const table = css::DbInterfaceMySql::getEmptyChunksTableName(database);
        vector<string> statements;
        if (force) statements.push_back("DROP TABLE IF EXISTS " + h.conn->sqlId(table));
        statements.push_back(css::DbInterfaceMySql::getEmptyChunksSchema(database));
        for (auto&& chunk: allChunks) {
            if (not ingestedChunks.count(chunk)) {
                statements.push_back(h.conn->sqlInsertQuery(table, chunk));
            }
        }
        h.conn->execute([&statements](decltype(h.conn) conn) {
            conn->begin();
            for (auto const& query: statements) {
                conn->execute(query);
            }
            conn->commit();
        });
    } else {
        auto const file = "empty_" + database + ".txt";
        auto const filePath = fs::path(config->get<string>("controller", "empty_chunks_dir")) / file;

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
        for (auto&& chunk: allChunks) {
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


string HttpIngestModule::_reconfigureWorkers(DatabaseInfo const& databaseInfo,
                                             bool allWorkers,
                                             unsigned int workerResponseTimeoutSec) const {

    auto const job = ServiceReconfigJob::create(
        allWorkers,
        workerResponseTimeoutSec,
        controller()
    );
    job->start();
    logJobStartedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(ServiceReconfigJob::typeName(), job, databaseInfo.family);

    string error;
    auto const& resultData = job->getResultData();
    for (auto&& itr: resultData.workers) {
        auto&& worker = itr.first;
        auto&& success = itr.second;
        if (not success) {
            error += "reconfiguration failed on worker: " + worker + " ";
        }
    }
    return error;
}


void HttpIngestModule::_createSecondaryIndex(DatabaseInfo const& databaseInfo) const {

    string const& directorTable = databaseInfo.directorTable;
    if (directorTable.empty() or
        (databaseInfo.directorTableKey.count(directorTable) == 0) or
        databaseInfo.directorTableKey.at(directorTable).empty() or
        databaseInfo.chunkIdColName.empty() or databaseInfo.subChunkIdColName.empty()) {
        throw logic_error(
                "director table has not been properly configured in database '" +
                databaseInfo.name + "'");
    }
    string const& directorTableKey = databaseInfo.directorTableKey.at(directorTable);
    if (0 == databaseInfo.columns.count(directorTable)) {
        throw logic_error(
                "no schema found for director table '" + directorTable +
                "' of database '" + databaseInfo.name + "'");
    }

    // Find types of the secondary index table's columns

    string directorTableKeyType;
    string chunkIdColNameType;
    string subChunkIdColNameType;

    for (auto&& coldef: databaseInfo.columns.at(directorTable)) {
        if (coldef.name == directorTableKey) directorTableKeyType = coldef.type;
        else if (coldef.name == databaseInfo.chunkIdColName) chunkIdColNameType = coldef.type;
        else if (coldef.name == databaseInfo.subChunkIdColName) subChunkIdColNameType = coldef.type;
    }
    if (directorTableKeyType.empty() or chunkIdColNameType.empty() or subChunkIdColNameType.empty()) {
        throw logic_error(
                "column definitions for the Object identifier or chunk/sub-chunk identifier"
                " columns are missing in the director table schema for table '" +
                directorTable + "' of database '" + databaseInfo.name + "'");
    }
    
    // Manage the new connection via the RAII-style handler to ensure the transaction
    // is automatically rolled-back in case of exceptions.

    database::mysql::ConnectionHandler const h(qservMasterDbConnection("qservMeta"));
    auto const escapedTableName = h.conn->sqlId(databaseInfo.name + "__" + directorTable);

    vector<string> queries;
    queries.push_back(
        "DROP TABLE IF EXISTS " + escapedTableName
    );
    queries.push_back(
        "CREATE TABLE IF NOT EXISTS " + escapedTableName +
        " (" + h.conn->sqlId(_partitionByColumn) + " " + _partitionByColumnType + "," +
               h.conn->sqlId(directorTableKey) + " " + directorTableKeyType   + "," +
               h.conn->sqlId(databaseInfo.chunkIdColName) + " " + chunkIdColNameType     + "," +
               h.conn->sqlId(databaseInfo.subChunkIdColName) + " " + subChunkIdColNameType  + ","
               " UNIQUE KEY (" + h.conn->sqlId(_partitionByColumn) + "," + h.conn->sqlId(directorTableKey) + "),"
               " KEY (" + h.conn->sqlId(directorTableKey) + ")"
        ") ENGINE=InnoDB PARTITION BY LIST (" + h.conn->sqlId(_partitionByColumn) +
        ") (PARTITION `p0` VALUES IN (0) ENGINE=InnoDB)"
    );

    h.conn->execute([&queries](decltype(h.conn) conn) {
        conn->begin();
        for (auto&& query: queries) {
            conn->execute(query);
        }
        conn->commit();
    });
}


void HttpIngestModule::_consolidateSecondaryIndex(DatabaseInfo const& databaseInfo) const {

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
        " REMOVE PARTITIONING";

    debug(__func__, query);

    h.conn->execute([&query](decltype(h.conn) conn) {
        conn->begin();
        conn->execute(query);
        conn->commit();
    });
}


void HttpIngestModule::_qservSync(DatabaseInfo const& databaseInfo,
                                  bool allWorkers) const {
    debug(__func__);

    bool const saveReplicaInfo = true;
    auto const findAlljob = FindAllJob::create(
        databaseInfo.family,
        saveReplicaInfo,
        allWorkers,
        controller()
    );
    findAlljob->start();
    logJobStartedEvent(FindAllJob::typeName(), findAlljob, databaseInfo.family);
    findAlljob->wait();
    logJobFinishedEvent(FindAllJob::typeName(), findAlljob, databaseInfo.family);

    if (findAlljob->extendedState() != Job::SUCCESS) {
        throw HttpError(__func__, "replica lookup stage failed");
    }

    bool const force = false;
    auto const qservSyncJob = QservSyncJob::create(
        databaseInfo.family,
        force,
        qservSyncTimeoutSec(),
        controller()
    );
    qservSyncJob->start();
    logJobStartedEvent(QservSyncJob::typeName(), qservSyncJob, databaseInfo.family);
    qservSyncJob->wait();
    logJobFinishedEvent(QservSyncJob::typeName(), qservSyncJob, databaseInfo.family);

    if (qservSyncJob->extendedState() != Job::SUCCESS) {
        throw HttpError( __func__, "Qserv synchronization failed");
    }
}

}}}  // namespace lsst::qserv::replica

