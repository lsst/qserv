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
#ifndef LSST_QSERV_HTTPINGESTMODULE_H
#define LSST_QSERV_HTTPINGESTMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/HttpModule.h"
#include "replica/SqlRowStatsJob.h"

// Forward declarations
namespace lsst::qserv::replica {
class DatabaseInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpIngestModule provides a support ingesting catalogs
 * into Qserv.
 */
class HttpIngestModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpIngestModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   DATABASES                 for retrieving info on databases for specified criteria
     *   ADD-DATABASE              for adding a new database for the data ingest
     *   PUBLISH-DATABASE          for publishing a database when data ingest is over
     *   DELETE-DATABASE           for deleting a database
     *   TABLES                    for retrieving the names of tables in a scope of a database
     *   ADD-TABLE                 for adding a new table for the data ingest
     *   DELETE-TABLE              for deleting a table from a database'
     *   SCAN-TABLE-STATS          for scanning worker tables and obtaining row counters
     *   DELETE-TABLE-STATS        for deleting existing stats on row counters
     *   TABLE-STATS               for retrieving existing stats on the row counters
     *   BUILD-CHUNK-LIST          for building (or rebuilding) an "empty chunk list"
     *   REGULAR                   for reporting connection parameters of the ingest servers
     *                             required to load the regular tables
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, std::string const& taskName,
                        HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp, std::string const& subModuleName = std::string(),
                        HttpAuthType const authType = HttpAuthType::NONE);

    HttpIngestModule() = delete;
    HttpIngestModule(HttpIngestModule const&) = delete;
    HttpIngestModule& operator=(HttpIngestModule const&) = delete;

    ~HttpIngestModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpIngestModule(Controller::Ptr const& controller, std::string const& taskName,
                     HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp);

    /// Get info on select databases
    nlohmann::json _getDatabases();

    /// Register a database for an ingest
    nlohmann::json _addDatabase();

    /// Publish a database whose data were ingested earlier
    nlohmann::json _publishDatabase();

    /**
     * Delete a database. All relevant data, including databases and tables at workers,
     * the secondary index (if any), the Replication System's Configuration, database entries
     * at Qserv czar  will get deleted.
     * @note This operation requires administrator-level privileges for deleting
     *   published databases.
     */
    nlohmann::json _deleteDatabase();

    /// Get info on select tables
    nlohmann::json _getTables();

    /// Register a database table for an ingest
    nlohmann::json _addTable();

    /**
     * Delete a table. All relevant data, including the tables at workers,
     * the Replication System's Configuration, table entries at Qserv czar will get deleted.
     * @note This operation requires administrator-level privileges for deleting
     *   tables of published databases.
     * @note The "director" tables can't be deleted with this method.
     */
    nlohmann::json _deleteTable();

    /**
     * @brief Scan internal tables of a given table to collect row counters
     *   and return this information to a caller.
     *
     * Depending on parameters of the method, the service may also update
     * the persistent state of the counters in the Replication systems database
     * and deploy the counters at Qserv's czar database.
     */
    nlohmann::json _scanTableStats();

    /**
     * @brief Implement scanning internal tables of a given table to collect row counters
     *   and return this information to a caller.
     * @note The operaton can be optimized in some curcumstances. In particular,
     *   if the \param forceRescan is set to 'false' and the persistent state
     *   of the counters within the system is not empty then no scan will be made.
     * @param databaseName The name of a database.
     * @param tableName The name of an existing table.
     * @param overlapSelector The selector (applies to the partitioned tables only)
     *   indicating which kind of the partitioned tables to be affected by
     *   the operation.
     * @param stateUpdatePolicy The policy for updating the persistent state of the row
     *   counters in the Replication system database.
     * @param deployAtQserv The flag that triggers deploying the counters at czar's metadata.
     * @param forceRescan The flag that forced the rescan at Qserv workers.
     * @param allWorkers The flag should be set to 'true' if no worker filtering is required.
     *   Otherwise only the "enabled" workers will be involved into the operation.
     * @param priority The priority level of the Replication Framework's jobs and requests
     *   submitted by the method.
     * @return nlohmann::json The extended error object that will be empty of no errors ocurred.
     */
    nlohmann::json _scanTableStatsImpl(std::string const& databaseName, std::string const& tableName,
                                       ChunkOverlapSelector overlapSelector,
                                       SqlRowStatsJob::StateUpdatePolicy stateUpdatePolicy,
                                       bool deployAtQserv, bool forceRescan, bool allWorkers, int priority);

    /// Delete existing stats on the row counters
    nlohmann::json _deleteTableStats();

    /// Get existing stats on the row counters
    nlohmann::json _tableStats();

    /// (Re-)build the "empty chunks list" for a database.
    nlohmann::json _buildEmptyChunksList();

    /**
     * Return connection parameters of the ingest servers of all workers
     * where the regular tables would have to be loaded.
     */
    nlohmann::json _getRegular();

    /**
     * @brief Retrieve and validate database and table names from the service's URL.
     * @param func A scope from which the method is called (for logging and error reporting).
     * @return The table descriptor.
     * @throws std::invalid_argument For unknown databases or tables.
     */
    TableInfo _getTableInfo(std::string const& func);

    /**
     * Grant SELECT authorizations for the new database to Qserv
     * MySQL account(s) at workers.
     *
     * @param database database descriptor
     * @param allWorkers  'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _grantDatabaseAccess(DatabaseInfo const& database, bool allWorkers) const;

    /**
     * Enable this database in Qserv workers by adding an entry
     * to table 'qservw_worker.Dbs' at workers.
     *
     * @param database database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _enableDatabase(DatabaseInfo const& database, bool allWorkers) const;

    /**
     * The optional fix-up for missing chunked tables applied by the database publishing
     * service. Once successfully complete the operation ensures that all partitioned
     * tables have chunks representations for all registered chunks, even though some
     * of the chunks may be empty. This stage enforces structural consistency across
     * partitioned tables.
     *
     * @param database database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _createMissingChunkTables(DatabaseInfo const& database, bool allWorkers) const;

    /**
     * Consolidate MySQL partitioned tables at workers by removing partitions.
     *
     * @param database database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if operation failed
     */
    void _removeMySQLPartitions(DatabaseInfo const& database, bool allWorkers) const;

    /**
     * Publish database in the Qserv master database. This involves the following
     * steps:
     * - creating database entry
     * - creating empty tables (with the proper) schema at the database
     * - registering database, tables and their partitioning parameters in CSS
     * - granting MySQL privileges for the Qserv account to access the database and tables
     * @param databaseName the name of a database to be published
     */
    void _publishDatabaseInMaster(DatabaseInfo const& database) const;

    /**
     * (Re-)build the empty chunks list (table) for the specified database.
     * The methods throws exceptions in case of any errors.
     *
     * @param databaseName The name of a database.
     * @param force Rebuild the list if 'true'.
     * @param tableImpl Create/update the table-based list implementation if 'true'.
     * @return An object representing a result of the operation (empty chunk list
     *   file/table name, number of chunks) in case of successful completion.
     */
    nlohmann::json _buildEmptyChunksListImpl(std::string const& databaseName, bool force,
                                             bool tableImpl) const;

    /**
     * Create an empty "secondary index" table partitioned using MySQL partitions.
     * The table will be configured with a single initial partition. More partitions
     * corresponding to super-transactions open during catalog ingest sessions will
     * be added later.
     * @param database defines a scope of the operation
     * @param directorTableName the name of the director table
     */
    void _createSecondaryIndex(DatabaseInfo const& database, std::string const& directorTableName) const;

    /**
     * Remove MySQL partitions from the "secondary index" table by turning it
     * into a regular monolithic table.
     * @param database  defines a scope of the operation
     * @param directorTableName the name of the director table
     */
    void _consolidateSecondaryIndex(DatabaseInfo const& database, std::string const& directorTableName) const;

    /**
     * This operation is called in a context of publishing new databases.
     * It runs the Replication system's chunks scanner to register chunk info
     * in the persistent state of the system. It also registers (synchronizes)
     * new chunks at Qserv workers.
     *
     * @param database database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _qservSync(DatabaseInfo const& database, bool allWorkers) const;

    // The name and a type of a special column for the super-transaction-based ingest

    static std::string const _partitionByColumn;
    static std::string const _partitionByColumnType;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPINGESTMODULE_H
