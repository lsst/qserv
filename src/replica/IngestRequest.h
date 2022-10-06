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
#include "replica/Csv.h"
#include "replica/DatabaseServices.h"
#include "replica/IngestFileSvc.h"
#include "replica/ServiceProvider.h"
#include "replica/Url.h"
#include "util/Mutex.h"

// Forward declarations
namespace lsst::qserv::replica {
class HttpClientConfig;
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
    static IngestRequest::Ptr create(ServiceProvider::Ptr const& serviceProvider,
                                     std::string const& workerName, TransactionId transactionId,
                                     std::string const& table, unsigned int chunk, bool isOverlap,
                                     std::string const& url, bool async,
                                     csv::DialectInput const& dialectInput,
                                     std::string const& httpMethod = "GET",
                                     std::string const& httpData = std::string(),
                                     std::vector<std::string> const& httpHeaders = std::vector<std::string>(),
                                     unsigned int maxNumWarnings = 0);

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
    static IngestRequest::Ptr resume(ServiceProvider::Ptr const& serviceProvider,
                                     std::string const& workerName, unsigned int contribId);

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
    IngestRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                  TransactionId transactionId, std::string const& table, unsigned int chunk, bool isOverlap,
                  std::string const& url, bool async, csv::DialectInput const& dialectInput,
                  std::string const& httpMethod, std::string const& httpData,
                  std::vector<std::string> const& httpHeaders, unsigned int maxNumWarnings);

    /// @see method IngestRequest::resume()
    IngestRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                  TransactionContribInfo const& contrib);

    // Three processing stages of the request

    void _processStart();
    void _processReadData();
    void _processLoadData();

    /// Read a local file and preprocess it.
    void _readLocalFile();

    /// Pull an input file from a remote HTTP service and preprocess it.
    void _readRemoteFile();

    /**
     * Pull file reader's configuration from the config store.
     * @return The configuration object.
     */
    HttpClientConfig _clientConfig() const;

    /// Mutex guarding internal state.
    mutable util::Mutex _mtx;

    /// The descriptor is build by the c-tor after validating the input
    /// parameters of the request.
    TransactionContribInfo _contrib;

    // These variables are set after completing parameter validation
    std::unique_ptr<Url> _resource;
    csv::Dialect _dialect;

    /// The flag is set by method process(), and once it's set it's never
    /// reset. The flag is used for coordinating state change with other methods
    /// of the class. In particular, setting this flag would prevent executing
    /// the request more than one time. The flag is also used by the request
    /// cancellation method cancel().
    std::atomic<bool> _processing{false};

    // Setting the flag will interrupt request processing (if the one is
    // still going on).
    std::atomic<bool> _cancelled{false};  ///< Set by calling the public method cancel()
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTREQUEST_H
