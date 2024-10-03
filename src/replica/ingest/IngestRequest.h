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
#ifndef LSST_QSERV_REPLICA_INGESTREQUEST_H
#define LSST_QSERV_REPLICA_INGESTREQUEST_H

// System headers
#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

// Qserv headers
#include "http/Method.h"
#include "http/Url.h"
#include "replica/ingest/IngestFileSvc.h"
#include "replica/services/DatabaseServices.h"
#include "replica/util/Csv.h"
#include "replica/util/Mutex.h"

// Forward declarations

namespace lsst::qserv::http {
class ClientConfig;
}  // namespace lsst::qserv::http

namespace lsst::qserv::replica {
class ServiceProvider;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestRequestInterrupted represents exceptions thrown by the request
 * processing method IngestRequest::process() after terminating the request
 * either due to an explicit request cancellation or expiration.
 */
class IngestRequestInterrupted : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

/**
 * Class IngestRequest encapsulates a state and algorithms needed for processing
 * ingest contributions.
 *
 * @note All public methods of the class are thread-safe (synchronized).
 * @note The class can be also used for unit testing w/o making any side effects
 *  (like attempting to connect to the Replication system's database or other
 *  remote services). In order to instaniate instances of the class for unit
 *  testing one has to call the special factory method IngestRequest::test().
 *  Methods process() and cancel() called on the test objects will do nothing.
 */
class IngestRequest : public std::enable_shared_from_this<IngestRequest>, public IngestFileSvc {
public:
    typedef std::shared_ptr<IngestRequest> Ptr;

    /// The default record size when reading from an input file.
    constexpr static size_t defaultRecordSizeBytes = 1048576;

    /**
     * The factory method for instantiating the request.
     *
     * - Parameters of the requests will be validated for validity and consistency.
     * - Upon successful completion of the parameters checking stage the request
     *   will be registered in the Replication/Ingest system's database and be ready for
     *   processing.
     * - Exceptions may be thrown by the method if any problems will be found
     *   while validating the parameters or registering the request in the database.
     *
     * @param serviceProvider The provider is needed to access various services of
     *   the Replication system's framework, such as the Configuration service,
     *   the Database service, etc.
     * @param workerName The name of a worker this service is acting upon.
     * @return A newly created instance of the request object.
     */
    static std::shared_ptr<IngestRequest> create(
            std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& workerName,
            TransactionId transactionId, std::string const& table, unsigned int chunk, bool isOverlap,
            std::string const& url, std::string const& charsetName, bool async,
            csv::DialectInput const& dialectInput, http::Method httpMethod = http::Method::GET,
            std::string const& httpData = std::string(),
            std::vector<std::string> const& httpHeaders = std::vector<std::string>(),
            unsigned int maxNumWarnings = 0, unsigned int maxRetries = 0);

    /**
     * The factory method for instantiating the request from an existing contribution.
     *
     * - Parameters of the request will be still validated to ensure the request is in
     *   the clean state. Though, unlike method create() the request won't be re-created
     *   in the database.
     *
     * @param serviceProvider The provider is needed to access various services of
     *   the Replication system's framework, such as the Configuration service,
     *   the Database service, etc.
     * @param workerName The name of a worker this service is acting upon.
     * @param contribId A unique identifier of an existing contribution request.
     * @return A newly created instance of the request object.
     */
    static std::shared_ptr<IngestRequest> resume(std::shared_ptr<ServiceProvider> const& serviceProvider,
                                                 std::string const& workerName, unsigned int contribId);

    /**
     * Special factory method for creating dummy requests for unit testing.
     * @note Attempts to process or cancel requests created by this method will result
     *   in throwing exceptions as described in the documentations of the corresponding
     *   methods.
     * @param contrib The description of the contribution.
     * @return A newly created instance of the request object.
     */
    static std::shared_ptr<IngestRequest> test(TransactionContribInfo const& contrib);

    /**
     * The factory method for instantiating the request from an existing contribution.
     *
     * Parameters of the request will be still validated to ensure the request was
     * failed while attempting to read or preprocess the input data. The method will
     * also ensure the original request was processed at the same worker as the one
     * specified in the parameter \param workerName. The processing mode (SYNC or ASYNC)
     * of the request will be updated to a value specified in the input parameter
     * \param async.
     *
     * @note Unlike the method create(), the request won't be re-created in the database.
     *   And it will retain the original identifier.
     *
     * @param serviceProvider The provider is needed to access various services of
     *   the Replication system's framework, such as the Configuration service,
     *   the Database service, etc.
     * @param workerName The name of a worker this service is acting upon.
     * @param contribId A unique identifier of an existing contribution request.
     * @param async The processing mode to be set at the request for bookkeeping purposes.
     * @throw std::invalid_argument For non-existing request, or incorrect values of
     *   the input parameters.
     * @return A newly created instance of the request object.
     */
    static std::shared_ptr<IngestRequest> createRetry(std::shared_ptr<ServiceProvider> const& serviceProvider,
                                                      std::string const& workerName, unsigned int contribId,
                                                      bool async);

    /// @return The descriptor of the request.
    TransactionContribInfo transactionContribInfo() const;

    /**
     * Process the request.
     *
     * - This operation will block a calling thread for a duration of the request
     *   processing before it succeeds, fails or gets interrupted due to the cancellation
     *   or expiration events.
     * - The method may also throw exception should any problem happened while
     *   the method using other services (the Replication system's database, etc.).
     *
     * @param contrib The input parameters of the request.
     * @throw std::logic_error If attempting to call the method while the processing is
     *   already in progress, or after the processing has finished.
     * @throw IngestRequestInterrupted if the request processing got cancelled
     */
    void process();

    /**
     * Cancel the request.
     *
     * A result of the operation depends on the current state of the request.
     * - No actions will be taken if the request has already been finished.
     * - If the request is being processed then the advisory cancellation flag
     *   be set to notify the processor
     */
    void cancel();

private:
    /**
     * @brief Validate a state the contribution's context.
     * @param trans The transaction to be evaluated.
     * @param database The database to be evaluated.
     * @param contrib The contribution.
     * @throw std::logic_error If the state is not suitable for ingesting
     *   the contribution.
     */
    static void _validateState(TransactionInfo const& trans, DatabaseInfo const& database,
                               TransactionContribInfo const& contrib);

    /// @see method IngestRequest::create()
    IngestRequest(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& workerName,
                  TransactionId transactionId, std::string const& table, unsigned int chunk, bool isOverlap,
                  std::string const& url, std::string const& charsetName, bool async,
                  csv::DialectInput const& dialectInput, http::Method httpMethod, std::string const& httpData,
                  std::vector<std::string> const& httpHeaders, unsigned int maxNumWarnings,
                  unsigned int maxRetries);

    /// @see method IngestRequest::resume()
    IngestRequest(std::shared_ptr<ServiceProvider> const& serviceProvider, std::string const& workerName,
                  TransactionContribInfo const& contrib);

    /// @see method IngestRequest::test()
    IngestRequest(TransactionContribInfo const& contrib);

    /// Update the contribution object. The method is thread-safe. It's required
    /// to be called from any method but the constructors when the contribution
    /// state needs to be updated.
    void _updateTransactionContribInfo(TransactionContribInfo const& contrib);

    // Three processing stages of the request

    void _processStart();
    void _processReadData();
    void _processLoadData();

    /// Open the temporary file and mark the contribution as started.
    void _openTmpFileAndStart(replica::Lock const& lock);

    /// @return 'true' if retry is possible.
    bool _closeTmpFileAndRetry(replica::Lock const& lock);

    /// Read a local file and preprocess it.
    void _readLocalFile(replica::Lock const& lock);

    /// Pull an input file from a remote HTTP service and preprocess it.
    void _readRemoteFile(replica::Lock const& lock);

    /**
     * Pull file reader's configuration from the config store.
     * @return The configuration object.
     */
    http::ClientConfig _clientConfig(replica::Lock const& lock) const;

    /// Mutex guarding internal state.
    mutable replica::Mutex _mtx;

    // These variables are set by the constructors after completing parameter validation.
    std::unique_ptr<http::Url> _resource;
    csv::Dialect _dialect;

    /// The flag is set by method process(), and once it's set it's never
    /// reset. The flag is used for coordinating state change with other methods
    /// of the class. In particular, setting this flag would prevent executing
    /// the request more than one time.
    std::atomic<bool> _processing{false};

    /// Set by calling the public method cancel(). Setting the flag will interrupt
    /// request processing (if the one is still going on).
    std::atomic<bool> _cancelled{false};

    /// Mutex guarding transitions of the transaction contribution object _contrib.
    mutable replica::Mutex _contribMtx;

    /**
     * The descriptor is initialized either from a value passed into the corresponding
     * constructor or it's built from scratch by another constructor after validating
     * input parameters of a request.
     *
     * In order to understand how the descriptor gets evolved during the lifecycle of
     * a request object one has to keep in mind that the descriptor is used in 5 different
     * contexts:
     * - It represents the current state of an ingest request (current class), and
     *   it changes during subsequent request processing after it starts.
     * - It's used in communications with the Replication system's database API
     *   when the persistent state of the ingest request needs to be updated.
     * - It's used for coordinating and managing the request processing by the ingest
     *   requests manager (class IngestRequestMgr).
     * - It's used for resuming the request processing after a restart of
     *   the Replication worker.
     * - It's used by the worker's REST API for providing the status of the request
     *   to the clients.
     *
     * Altogether these requirements led to the following "copy-on-write" state management
     * strategy for the descriptor in the implementation of the current class:
     * - The descriptor is guarded by the mutex _contribMtx.
     * - Values of non-changing attributes of the descriptor initialized by the c-tor can
     *   be read by any method of the current class w/o any synchronization.
     * - Any changes to other attributes of the descriptior made by methods of the current
     *   class should be done in the transactional mode by the following sequence
     *   of operations:
     *   1. Obtaining a copy of the descriptor by calling method transactionContribInfo()
     *      (that is protected by the mutex).
     *   2. Modifying the copy of the descriptor.
     *   3. Updating the descriptor by calling method _updateTransactionContribInfo()
     *      (that is protected by the mutex).
     *
     * This sequence ensures that clients of the request object will always get the consistent
     * state of the transaction contribution descriptor, and the descriptor retrieval won't
     * be blocked by any stage of the request processing.
     */
    TransactionContribInfo _contrib;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTREQUEST_H
