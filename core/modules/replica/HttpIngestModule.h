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
#include "util/Mutex.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class DatabaseInfo;
namespace database {
namespace mysql {
    class Connection;
}}}}} // Forward declarations

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

    static Ptr create(Controller::Ptr const& controller,
                      std::string const& taskName,
                      HttpProcessorConfig const& processorConfig);

    HttpIngestModule() = delete;
    HttpIngestModule(HttpIngestModule const&) = delete;
    HttpIngestModule& operator=(HttpIngestModule const&) = delete;

    ~HttpIngestModule() final = default;

protected:
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
     *   ADD-CHUNK                 for registering (or requesting a status of) of a new chunk
     *   BUILD-CHUNK-LIST          for building (or rebuilding) an "empty chunk list"
     *   REGULAR                   for reporting connection parameters of the ingest servers
     *                             required to load the regular tables
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    void executeImpl(qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp,
                     std::string const& subModuleName) final;

private:
    HttpIngestModule(Controller::Ptr const& controller,
                     std::string const& taskName,
                     HttpProcessorConfig const& processorConfig);

    /**
     * Get info on super-transactions
     */
    void _getTransactions(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Get info on the current/latest super-transaction
     */
    void _getTransaction(qhttp::Request::Ptr const& req,
                         qhttp::Response::Ptr const& resp);

    /**
     * Crate and start a super-transaction
     */
    void _beginTransaction(qhttp::Request::Ptr const& req,
                           qhttp::Response::Ptr const& resp);

    /**
     * Commit or rollback a super-transaction
     */
    void _endTransaction(qhttp::Request::Ptr const& req,
                         qhttp::Response::Ptr const& resp);

    /**
     * Register a database for an ingest
     */
    void _addDatabase(qhttp::Request::Ptr const& req,
                      qhttp::Response::Ptr const& resp);

    /**
     * Publish a database whose data were ingested earlier
     */
    void _publishDatabase(qhttp::Request::Ptr const& req,
                          qhttp::Response::Ptr const& resp);

    /**
     * Delete a database which is not yet published. All relevant data,
     * including databases and tables at workers, the secondary index (if any)
     * and Replication System's Configuration will be deleted s well.
     */
    void _deleteDatabase(qhttp::Request::Ptr const& req,
                         qhttp::Response::Ptr const& resp);

    /**
     * Register a database table for an ingest
     */
    void _addTable(qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    /**
     * Register (if it's not register yet) a chunk for ingest.
     * Return connection parameters to an end-point service where chunk
     * data will need to be ingested.
     */
    void _addChunk(qhttp::Request::Ptr const& req,
                   qhttp::Response::Ptr const& resp);

    /**
     * (Re-)build the "empty chunks list" for a database.
     */
    void _buildEmptyChunksList(qhttp::Request::Ptr const& req,
                               qhttp::Response::Ptr const& resp);

    /**
     * Return connection parameters of the ingest servers of all workers
     * where the regular tbles would have to be loaded.
     */
    void _getRegular(qhttp::Request::Ptr const& req,
                     qhttp::Response::Ptr const& resp);

    /**
     * Grant SELECT authorizations for the new database to Qserv
     * MySQL account(s) at workers
     * @param resp the HTTP response channel for reporting errors
     * @param databaseInfo database descriptor
     * @param allWorkers  'true' if all workers should be involved into the operation
     * @return 'false' if operation failed
     */
    bool _grantDatabaseAccess(qhttp::Response::Ptr const& resp,
                              DatabaseInfo const& databaseInfo,
                              bool allWorkers) const;

    /**
     * Enable this database in Qserv workers by adding an entry
     * to table 'qservw_worker.Dbs' at workers.
     * @param resp the HTTP response channel for reporting errors
     * @param databaseInfo database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @return 'false' if operation failed
     */
    bool _enableDatabase(qhttp::Response::Ptr const& resp,
                         DatabaseInfo const& databaseInfo,
                         bool allWorkers) const;

    /**
     * Consolidate MySQL partitioned tables at workers by removing partitions.
     * @param resp the HTTP response channel for reporting errors
     * @param databaseInfo database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @return 'false' if operation failed
     */
    bool _removeMySQLPartitions(qhttp::Response::Ptr const& resp,
                                DatabaseInfo const& databaseInfo,
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
     * @param database the name of a database
     * @param force rebuild the file if 'true'
     * @return pair of (empty chunk lists file name, number of chunks)
     */
    std::pair<std::string,size_t> _buildEmptyChunksListImpl(std::string const& database,
                                                            bool force) const;

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
     * @param resp the HTTP response channel for reporting errors
     * @param databaseInfo database descriptor
     * @param allWorkers 'true' if all workers should be involved into the operation
     * @return 'false' if operation failed
     */
    bool _qservSync(qhttp::Response::Ptr const& resp,
                    DatabaseInfo const& databaseInfo,
                    bool allWorkers) const;

    /// @return connection object for the Qserv Master Database server
    std::shared_ptr<database::mysql::Connection> _qservMasterDbConnection() const;

    // The name and a type of a special column for the super-transaction-based ingest

    static std::string const _partitionByColumn;
    static std::string const _partitionByColumnType;

    util::Mutex _ingestManagementMtx;   /// Synchronized access to the Ingest management operations
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPINGESTMODULE_H
