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

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class DatabaseInfo;
}}} // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpIngestModule provides a support ingesting catalogs
 * into Qserv.
 */
class HttpIngestModule: public HttpModule {
public:
    typedef std::shared_ptr<HttpIngestModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   TRANSACTIONS              for many transactions (possible selected by various criteria)
     *   SELECT-TRANSACTION-BY-ID  for a single transaction
     *   BEGIN-TRANSACTION         for starting a new transaction
     *   END-TRANSACTION           for finishing/aborting a transaction
     *   ADD-DATABASE              for adding a new database for the data ingest
     *   PUBLISH-DATABASE          for publishing a database when data ingest is over
     *   DELETE-DATABASE           for deleting an unpublished (partially ingest) database
     *   ADD-TABLE                 for adding a new table for the data ingest
     *   BUILD-CHUNK-LIST          for building (or rebuilding) an "empty chunk list"
     *   REGULAR                   for reporting connection parameters of the ingest servers
     *                             required to load the regular tables
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller,
                        std::string const& taskName,
                        HttpProcessorConfig const& processorConfig,
                        qhttp::Request::Ptr const& req,
                        qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName=std::string(),
                        HttpModule::AuthType const authType=HttpModule::AUTH_NONE);

    HttpIngestModule() = delete;
    HttpIngestModule(HttpIngestModule const&) = delete;
    HttpIngestModule& operator=(HttpIngestModule const&) = delete;

    ~HttpIngestModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpIngestModule(Controller::Ptr const& controller,
                     std::string const& taskName,
                     HttpProcessorConfig const& processorConfig,
                     qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp);

    /// Get info on super-transactions
    nlohmann::json _getTransactions();

    /// Get info on the current/latest super-transaction
    nlohmann::json _getTransaction();

    /// Crate and start a super-transaction
    nlohmann::json _beginTransaction();

    /// Commit or rollback a super-transaction
    nlohmann::json _endTransaction();

    /// Register a database for an ingest
    nlohmann::json _addDatabase();

    /// Publish a database whose data were ingested earlier
    nlohmann::json _publishDatabase();

    /**
     * Delete a database which is not yet published. All relevant data,
     * including databases and tables at workers, the secondary index (if any)
     * and Replication System's Configuration will be deleted s well.
     */
    nlohmann::json _deleteDatabase();

    /// Register a database table for an ingest
    nlohmann::json _addTable();

    /// (Re-)build the "empty chunks list" for a database.
    nlohmann::json _buildEmptyChunksList();

    /**
     * Return connection parameters of the ingest servers of all workers
     * where the regular tables would have to be loaded.
     */
    nlohmann::json _getRegular();

    /**
     * Grant SELECT authorizations for the new database to Qserv
     * MySQL account(s) at workers.
     * 
     * @param databaseInfo database descriptor
     * @param allWorkers  'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _grantDatabaseAccess(DatabaseInfo const& databaseInfo,
                              bool allWorkers) const;

    /**
     * Enable this database in Qserv workers by adding an entry
     * to table 'qservw_worker.Dbs' at workers.
     * 
     * @param databaseInfo database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _enableDatabase(DatabaseInfo const& databaseInfo,
                         bool allWorkers) const;

    /**
     * The optional fix-up for missing chunked tables applied by the database publishing
     * service. Once successfully complete the operation ensures that all partitioned
     * tables have chunks representations for all registered chunks, even though some
     * of the chunks may be empty. This stage enforces structural consistency across
     * partitioned tables.
     * 
     * @param databaseInfo database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _createMissingChunkTables(DatabaseInfo const& databaseInfo,
                                   bool allWorkers) const;

    /**
     * Consolidate MySQL partitioned tables at workers by removing partitions.
     * 
     * @param databaseInfo database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if operation failed
     */
    void _removeMySQLPartitions(DatabaseInfo const& databaseInfo,
                                bool allWorkers) const;

    /**
     * Publish database in the Qserv master database. This involves the following
     * steps:
     * - creating database entry
     * - creating empty tables (with the proper) schema at the database
     * - registering database, tables and their partitioning parameters in CSS
     * - granting MySQL privileges for the Qserv account to access the database and tables
     * @param databaseName the name of a database to be published
     */
    void _publishDatabaseInMaster(DatabaseInfo const& databaseInfo) const;

    /**
     * (Re-)build the empty chunks list (table) for the specified database.
     * The methods throws exceptions in case of any errors.
     *
     * @param database The name of a database.
     * @param force Rebuild the list if 'true'.
     * @param tableImpl Create/update the table-based list implementation if 'true'.
     * @return An object representing a result of the operation (empty chunk list
     *   file/table name, number of chunks) in case of successful completion.
     */
    nlohmann::json _buildEmptyChunksListImpl(std::string const& database,
                                             bool force,
                                             bool tableImpl) const;

    /**
     * This method will tell all (or a subset of) workers to reload cache Configuration
     * parameters. The operation is needed after significant changes in the Replication
     * system's configuration occur, such as creating new databases or tables.
     * This is to implement an explicit model of making workers aware about changes
     * in the mostly static state of the system.
     * @param databaseInfo  defines a scope of the operation (used for status and error reporting)
     * @param allWorkers  'true' if all workers are involved into the operation
     * @param workerResponseTimeoutSec  do not wait longer than the specified number of seconds
     * @return non-empty string to indicate a error
     */
    std::string _reconfigureWorkers(DatabaseInfo const& databaseInfo,
                                    bool allWorkers,
                                    unsigned int workerResponseTimeoutSec) const;

    /**
     * Fetch a mode of building the "secondary index" as requested by a catalog
     * ingest workflow and recorded at the database creation time. A value of
     * the parameter is recorded in a database.
     * 
     * @param database The name of a database for which a value of the parameter
     *   is requested.
     * @return 'true' if the index was requested to be built automatically w/o any
     *   explicit requests from a catalog ingest workflow.
     */
    bool _autoBuildSecondaryIndex(std::string const& database) const;

    /**
     * Create an empty "secondary index" table partitioned using MySQL partitions.
     * The table will be configured with a single initial partition. More partitions
     * corresponding to super-transactions open during catalog ingest sessions will
     * be added later.
     * @param databaseInfo defines a scope of the operation
     */
    void _createSecondaryIndex(DatabaseInfo const& databaseInfo) const;

    /**
     * Extend an existing "secondary index" table by adding a MySQL partition
     * corresponding to the specified transaction identifier.
     * @param databaseInfo defines a scope of the operation
     * @param transactionId unique identifier of a super-transaction
     */
    void _addPartitionToSecondaryIndex(DatabaseInfo const& databaseInfo,
                                       TransactionId transactionId) const;

   /**
     * Shrink an existing "secondary index" table by removing a MySQL partition
     * corresponding to the specified transaction identifier from the table.
     * @param databaseInfo defines a scope of the operation
     * @param transactionId unique identifier of a super-transaction
     */
    void _removePartitionFromSecondaryIndex(DatabaseInfo const& databaseInfo,
                                            TransactionId transactionId) const;

    /**
     * Remove MySQL partitions from the "secondary index" table by turning it
     * into a regular monolithic table.
     * @param databaseInfo  defines a scope of the operation
     */
    void _consolidateSecondaryIndex(DatabaseInfo const& databaseInfo) const;

    /**
     * Delete the "secondary index" table.
     * @param databaseInfo defines a scope of the operation
     */
    void _deleteSecondaryIndex(DatabaseInfo const& databaseInfo) const;

    /**
     * This operation is called in a context of publishing new databases.
     * It runs the Replication system's chunks scanner to register chunk info
     * in the persistent state of the system. It also registers (synchronizes)
     * new chunks at Qserv workers.
     *
     * @param databaseInfo database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @throws HttpError if the operation failed
     */
    void _qservSync(DatabaseInfo const& databaseInfo,
                    bool allWorkers) const;

    // The name and a type of a special column for the super-transaction-based ingest

    static std::string const _partitionByColumn;
    static std::string const _partitionByColumnType;
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPINGESTMODULE_H
