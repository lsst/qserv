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
#include "replica/Common.h"
#include "replica/HttpModule.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class HttpIngestTransModule provides a support for managing "super-transactions"
 * in the Replication system as needed during catalog ingest.
 */
class HttpIngestTransModule: public HttpModule {
public:
    typedef std::shared_ptr<HttpIngestTransModule> Ptr;

    /**
     * Supported values for parameter 'subModuleName':
     *
     *   TRANSACTIONS              for many transactions (possible selected by various criteria)
     *   SELECT-TRANSACTION-BY-ID  for a single transaction
     *   BEGIN-TRANSACTION         for starting a new transaction
     *   END-TRANSACTION           for finishing/aborting a transaction
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

    HttpIngestTransModule() = delete;
    HttpIngestTransModule(HttpIngestTransModule const&) = delete;
    HttpIngestTransModule& operator=(HttpIngestTransModule const&) = delete;

    ~HttpIngestTransModule() final = default;

protected:
    nlohmann::json executeImpl(std::string const& subModuleName) final;

private:
    HttpIngestTransModule(Controller::Ptr const& controller,
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
};
    
}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_HTTPINGESTTRANSMODULE_H
