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
#ifndef LSST_QSERV_HTTPINGESTTRANSMODULE_H
#define LSST_QSERV_HTTPINGESTTRANSMODULE_H

// System headers
#include <memory>
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/contr/HttpModule.h"
#include "replica/ingest/TransactionContrib.h"
#include "replica/services/DatabaseServices.h"
#include "replica/util/Common.h"

// Forward declarations
namespace lsst::qserv::replica {
class TransactionInfo;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpIngestTransModule provides a support for managing "super-transactions"
 * in the Replication system as needed during catalog ingest.
 */
class HttpIngestTransModule : public HttpModule {
public:
    typedef std::shared_ptr<HttpIngestTransModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   TRANSACTIONS              for many transactions (possible selected by various criteria)
     *   SELECT-TRANSACTION-BY-ID  for a single transaction
     *   BEGIN-TRANSACTION         for starting a new transaction
     *   END-TRANSACTION           for finishing/aborting a transaction
     *   GET-CONTRIBUTION-BY-ID    for pulling info on the transaction contributions
     *
     * @throws std::invalid_argument for unknown values of parameter 'subModuleName'
     */
    static void process(Controller::Ptr const& controller, NamedMutexRegistry& transactionMutexRegistry,
                        std::string const& taskName, HttpProcessorConfig const& processorConfig,
                        qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp,
                        std::string const& subModuleName = std::string(),
                        http::AuthType const authType = http::AuthType::NONE);

    HttpIngestTransModule() = delete;
    HttpIngestTransModule(HttpIngestTransModule const&) = delete;
    HttpIngestTransModule& operator=(HttpIngestTransModule const&) = delete;

    ~HttpIngestTransModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpIngestTransModule(Controller::Ptr const& controller, NamedMutexRegistry& transactionMutexRegistry,
                          std::string const& taskName, HttpProcessorConfig const& processorConfig,
                          qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp);

    /// Get info on super-transactions
    nlohmann::json _getTransactions();

    /// Get info on the current/latest super-transaction
    nlohmann::json _getTransaction();

    /// Crate and start a super-transaction
    nlohmann::json _beginTransaction();

    /// Commit or rollback a super-transaction
    nlohmann::json _endTransaction();

    /// Get info on the the transaction contributions
    nlohmann::json _getContribution();

    /**
     * @brief Log controller events for the transaction management operations.
     *
     * @param operation The name of the operation.
     * @param status The completion status of the operation.
     * @param transactionId The unique identifier of the transaction affected by the operation.
     * @param databaseName The name of a database associated with the transaction.
     * @param msg The optional error message in case if the operation failed.
     */
    void _logTransactionMgtEvent(std::string const& operation, std::string const& status,
                                 TransactionId transactionId, std::string const& databaseName,
                                 std::string const& msg = std::string()) const;

    /**
     * Extend an existing "director" index table by adding a MySQL partition
     * corresponding to the specified transaction identifier.
     * @param database the database descriptor defines a scope of the operation
     * @param transactionId unique identifier of a super-transaction
     * @param directorTableName the name of the director table to build the index for
     */
    void _addPartitionToDirectorIndex(DatabaseInfo const& database, TransactionId transactionId,
                                      std::string const& directorTableName) const;

    /**
     * Shrink an existing "directtor" index table by removing a MySQL partition
     * corresponding to the specified transaction identifier from the table.
     * @param database the database descriptor defines a scope of the operation
     * @param transactionId unique identifier of a super-transaction
     * @param directorTableName the name of the director table to remove the index from
     */
    void _removePartitionFromDirectorIndex(DatabaseInfo const& database, TransactionId transactionId,
                                           std::string const& directorTableName) const;

    /**
     * Extract contributions into a transaction.
     * @param transaction A transaction defining a scope of the request.
     * @param tableName The name of a table to pull contributions from (if empty then all tables will be
     * assumed).
     * @param workerName The name of a worker to pull contributions from (if empty then all workers will be
     * assumed).
     * @param contribStatus A set of the contribution statuses to filter the contributions by (all
     * contributions if empty).
     * @param chunkSelector The chunk selector to filter the contributions by (all contributions if -1).
     * @param longContribFormat If 'true' then the method will also return info on
     *   the individual file contributions rather than just the summary info.
     * @param includeExtensions If 'true' then include info on the contributions extensions. Note that
     *  this option is ignored if longContribFormat == false.
     * @param includeWarnings If 'true' then include info on the MySQL warnings
     *   if any were captured after LOAD DATA INFILE. Note that this option is
     *   ignored if longContribFormat == false.
     * @param includeRetries If 'true' then include info on the failed retries
     *   if any were made when reading the input data of the contributions. Note that
     *   this option is ignored if longContribFormat == false.
     * @param minRetries The minimum number of retries for a contribution to be included in
     *   the response (0 for all).
     * @param minWarnings The minimum number of warnings for a contribution to include in
     *   the response (0 for all).
     * @param maxEntries The maximum number of contributions to return (0 for all).
     * @return A JSON object.
     */
    nlohmann::json _getTransactionContributions(TransactionInfo const& transaction,
                                                std::string const& tableName, std::string const& workerName,
                                                std::set<TransactionContribInfo::Status> const& contribStatus,
                                                int chunkSelector, bool longContribFormat,
                                                bool includeExtensions, bool includeWarnings,
                                                bool includeRetries, size_t minRetries = 0,
                                                size_t minWarnings = 0, size_t maxEntries = 0) const;

    /**
     * Parse a string representation of the transaction state.
     * @param param The name of the query parameter to parse.
     * @return A set of the transaction states (empty if the parameter is not set).
     * @throws std::invalid_argument If the string didn't match any known code.
     */
    std::set<TransactionInfo::State> _parseTransStateSelector(std::string const& param) const;

    /**
     * Parse a string representation of the contribution status.
     * @param param The name of the query parameter to parse.
     * @return A set of the contribution statuses (empty if the parameter is not set).
     * @throws std::invalid_argument If the string didn't match any known code.
     */
    std::set<TransactionContribInfo::Status> _parseContribStatusSelector(std::string const& param) const;

    /// Named mutexes are used for acquiring exclusive transient locks on the transaction
    /// management operations performed by the module.
    NamedMutexRegistry& _transactionMutexRegistry;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPINGESTTRANSMODULE_H
