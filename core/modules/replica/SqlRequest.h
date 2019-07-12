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
#ifndef LSST_QSERV_REPLICA_SQLREQUEST_H
#define LSST_QSERV_REPLICA_SQLREQUEST_H

// System headers
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

// Qserv headers
#include "replica/Common.h"
#include "replica/RequestMessenger.h"
#include "replica/SqlResultSet.h"
#include "replica/protocol.pb.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class Messenger;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Abstract class SqlBaseRequest is a base class for a family of the Controller-side
 * requests launching queries against a MySQL/MariaDB database of Qserv workers
 * via the corresponding Replication workers. The requests are sent over
 * the controller-worker protocol and they are executed by the the worker-side
 * framework. 
 *
 * In case of a successful completion of a request an object of this request class
 * will receive a result set (if any) of the query.
 */
class SqlBaseRequest : public RequestMessenger  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlBaseRequest> Ptr;

    // Default construction and copy semantics are prohibited

    SqlBaseRequest() = delete;
    SqlBaseRequest(SqlBaseRequest const&) = delete;
    SqlBaseRequest& operator=(SqlBaseRequest const&) = delete;

    ~SqlBaseRequest() override = default;

    // Trivial get methods

    uint64_t maxRows() const { return requestBody.max_rows(); }

    /// @return target request specific parameters
    SqlRequestParams const& targetRequestParams() const { return _targetRequestParams; }

    /**
     * @return a reference to a result obtained from a remote service.
     *
     * @note
     *   This operation will return a sensible result only if the operation
     *   finishes with status FINISHED::SUCCESS
     */
    SqlResultSet const& responseData() const;

    /// @see Request::extendedPersistentState()
    std::list<std::pair<std::string,std::string>> extendedPersistentState() const final;

protected:

    /**
     * Create a new request with specified parameters.
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node

     * @param maxRows
     *   (optional) limit for the maximum number of rows to be returned with the request.
     *   Laving the default value of the parameter to 0 will result in not imposing any
     *   explicit restrictions on a size of the result set. NOte that other, resource-defined
     *   restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *   objects, the amount of available memory, etc.

     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     */
    SqlBaseRequest(ServiceProvider::Ptr const& serviceProvider,
                   boost::asio::io_service& io_service,
                   std::string const& worker,
                   uint64_t maxRows,
                   int priority,
                   bool keepTracking,
                   std::shared_ptr<Messenger> const& messenger);

    /// @see Request::startImpl()
    void startImpl(util::Lock const& lock) final;

    /// @see Request::savePersistentState()
    void savePersistentState(util::Lock const& lock) final;

    /**
     * Request body to be sent to the worker. The content of the request is partially
     * set by this class's constructor, and it's fully initialized by the constructor
     * of a subclass depending on a type of request.
     */
    ProtocolRequestSql requestBody;

private:

    /**
     * Start the timer before attempting the previously failed
     * or successful (if a status check is needed) step.
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _wait(util::Lock const& lock);

    /**
     * Callback handler for the asynchronous operation
     *
     * @param ec
     *   error code to be checked
     */
    void _awaken(boost::system::error_code const& ec);

    /**
     * Send the serialized content of the buffer to a worker
     *
     * @param lock
     *   a lock on Request::_mtx must be acquired before calling this method
     */
    void _send(util::Lock const& lock);

    /**
     * Process the completion of the requested operation
     *
     * @param success
     *   'true' indicates a successful response from a worker
     *
     * @param response
     *   response from a worker (if success)
     */
    void _analyze(bool success,
                  ProtocolResponseSql const& response);

    /// Request-specific parameters of the target request
    SqlRequestParams _targetRequestParams;

    /// The results reported by a worker service
    SqlResultSet _responseData;
};


/**
 * Class SqlQueryRequest represents Controller-side requests for initiating
 * arbitrary database queries at a remote worker nodes.
 */
class SqlQueryRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlQueryRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlQueryRequest() = delete;
    SqlQueryRequest(SqlQueryRequest const&) = delete;
    SqlQueryRequest& operator=(SqlQueryRequest const&) = delete;

    ~SqlQueryRequest() final = default;

    std::string const& query()    const { return requestBody.query(); }
    std::string const& user()     const { return requestBody.user(); }
    std::string const& password() const { return requestBody.password(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param query
     *   the query to be executed
     *
     * @param user
     *   the name of a database account for connecting to the database service
     *
     * @param password
     *   a database for connecting to the database service
     *
     * @param maxRows
     *   (optional) limit for the maximum number of rows to be returned with the request.
     *   Laving the default value of the parameter to 0 will result in not imposing any
     *   explicit restrictions on a size of the result set. NOte that other, resource-defined
     *   restrictions will still apply. The later includes the maximum size of the Google Protobuf
     *   objects, the amount of available memory, etc.
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& query,
                      std::string const& user,
                      std::string const& password,
                      uint64_t maxRows,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlQueryRequest::create()
    SqlQueryRequest(ServiceProvider::Ptr const& serviceProvider,
                    boost::asio::io_service& io_service,
                    std::string const& worker,
                    std::string const& query,
                    std::string const& user,
                    std::string const& password,
                    uint64_t maxRows,
                    CallbackType const& onFinish,
                    int priority,
                    bool keepTracking,
                    std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlCreateDbRequest represents Controller-side requests for initiating
 * queries for creating databases at a remote worker nodes.
 */
class SqlCreateDbRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlCreateDbRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlCreateDbRequest() = delete;
    SqlCreateDbRequest(SqlCreateDbRequest const&) = delete;
    SqlCreateDbRequest& operator=(SqlCreateDbRequest const&) = delete;

    ~SqlCreateDbRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of a database to be created
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlCreateDbRequest::create()
    SqlCreateDbRequest(ServiceProvider::Ptr const& serviceProvider,
                       boost::asio::io_service& io_service,
                       std::string const& worker,
                       std::string const& database,
                       CallbackType const& onFinish,
                       int priority,
                       bool keepTracking,
                       std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlDeleteDbRequest represents Controller-side requests for initiating
 * queries for deleting databases at a remote worker nodes.
 */
class SqlDeleteDbRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDeleteDbRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlDeleteDbRequest() = delete;
    SqlDeleteDbRequest(SqlDeleteDbRequest const&) = delete;
    SqlDeleteDbRequest& operator=(SqlDeleteDbRequest const&) = delete;

    ~SqlDeleteDbRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of a database to be deleted
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlDeleteDbRequest::create()
    SqlDeleteDbRequest(ServiceProvider::Ptr const& serviceProvider,
                       boost::asio::io_service& io_service,
                       std::string const& worker,
                       std::string const& database,
                       CallbackType const& onFinish,
                       int priority,
                       bool keepTracking,
                       std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlEnableDbRequest represents Controller-side requests for initiating
 * queries for enabling databases in Qserv at a remote worker nodes.
 */
class SqlEnableDbRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlEnableDbRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlEnableDbRequest() = delete;
    SqlEnableDbRequest(SqlEnableDbRequest const&) = delete;
    SqlEnableDbRequest& operator=(SqlEnableDbRequest const&) = delete;

    ~SqlEnableDbRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of a database to be enabled
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlEnableDbRequest::create()
    SqlEnableDbRequest(ServiceProvider::Ptr const& serviceProvider,
                       boost::asio::io_service& io_service,
                       std::string const& worker,
                       std::string const& database,
                       CallbackType const& onFinish,
                       int priority,
                       bool keepTracking,
                       std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlDisableDbRequest represents Controller-side requests for initiating
 * queries for disabling databases in Qserv at a remote worker nodes.
 */
class SqlDisableDbRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDisableDbRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlDisableDbRequest() = delete;
    SqlDisableDbRequest(SqlDisableDbRequest const&) = delete;
    SqlDisableDbRequest& operator=(SqlDisableDbRequest const&) = delete;

    ~SqlDisableDbRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of a database to be disabled
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlDisableDbRequest::create()
    SqlDisableDbRequest(ServiceProvider::Ptr const& serviceProvider,
                        boost::asio::io_service& io_service,
                        std::string const& worker,
                        std::string const& database,
                        CallbackType const& onFinish,
                        int priority,
                        bool keepTracking,
                        std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlGrantAccessRequest represents Controller-side requests for initiating
 * queries for granting access to a database by a specified MySQL user at remote
 * worker nodes.
 */
class SqlGrantAccessRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlGrantAccessRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlGrantAccessRequest() = delete;
    SqlGrantAccessRequest(SqlGrantAccessRequest const&) = delete;
    SqlGrantAccessRequest& operator=(SqlGrantAccessRequest const&) = delete;

    ~SqlGrantAccessRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of a database to be disabled
     *
     * @param user
     *   the name of an existing database account to be affected by the operation
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      std::string const& user,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlGrantAccessRequest::create()
    SqlGrantAccessRequest(ServiceProvider::Ptr const& serviceProvider,
                          boost::asio::io_service& io_service,
                          std::string const& worker,
                          std::string const& database,
                          std::string const& user,
                          CallbackType const& onFinish,
                          int priority,
                          bool keepTracking,
                          std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlCreateTableRequest represents Controller-side requests for initiating
 * queries for creating tables at a remote worker nodes.
 */
class SqlCreateTableRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlCreateTableRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlCreateTableRequest() = delete;
    SqlCreateTableRequest(SqlCreateTableRequest const&) = delete;
    SqlCreateTableRequest& operator=(SqlCreateTableRequest const&) = delete;

    ~SqlCreateTableRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of an existing database where the new table will be created
     *
     * @param table
     *   the name of a table to be created
     *
     * @param engine
     *   the name of the MySQL engine for the new table
     *
     * @pram partitionByColumn
     *   (optional, if not empty) the name of a column which will be used
     *   as a key to configure MySQL partitions for the new table.
     *   This variation of table schema will be used for the super-transaction-based
     *   ingest into the table.
     *
     * @param columns
     *   column definitions as pairs of (name,type) of the table
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      std::string const& table,
                      std::string const& engine,
                      std::string const& partitionByColumn,
                      std::list<std::pair<std::string, std::string>> const& columns,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlCreateTableRequest::create()
    SqlCreateTableRequest(ServiceProvider::Ptr const& serviceProvider,
                          boost::asio::io_service& io_service,
                          std::string const& worker,
                          std::string const& database,
                          std::string const& table,
                          std::string const& engine,
                          std::string const& partitionByColumn,
                          std::list<std::pair<std::string, std::string>> const& columns,
                          CallbackType const& onFinish,
                          int priority,
                          bool keepTracking,
                          std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlDeleteTableRequest represents Controller-side requests for initiating
 * queries for deleting tables at a remote worker nodes.
 */
class SqlDeleteTableRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDeleteTableRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlDeleteTableRequest() = delete;
    SqlDeleteTableRequest(SqlDeleteTableRequest const&) = delete;
    SqlDeleteTableRequest& operator=(SqlDeleteTableRequest const&) = delete;

    ~SqlDeleteTableRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of an existing database where the table is residing
     *
     * @param table
     *   the name of a table to be deleted
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      std::string const& table,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlDeleteTableRequest::create()
    SqlDeleteTableRequest(ServiceProvider::Ptr const& serviceProvider,
                          boost::asio::io_service& io_service,
                          std::string const& worker,
                          std::string const& database,
                          std::string const& table,
                          CallbackType const& onFinish,
                          int priority,
                          bool keepTracking,
                          std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlRemoveTablePartitionsRequest represents Controller-side requests for initiating
 * queries for removing MySQL partitions from tables at a remote worker nodes.
 */
class SqlRemoveTablePartitionsRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlRemoveTablePartitionsRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlRemoveTablePartitionsRequest() = delete;
    SqlRemoveTablePartitionsRequest(SqlRemoveTablePartitionsRequest const&) = delete;
    SqlRemoveTablePartitionsRequest& operator=(SqlRemoveTablePartitionsRequest const&) = delete;

    ~SqlRemoveTablePartitionsRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of an existing database where the table is residing
     *
     * @param table
     *   the name of a table affected by the operation
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      std::string const& table,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlRemoveTablePartitionsRequest::create()
    SqlRemoveTablePartitionsRequest(ServiceProvider::Ptr const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& database,
                                    std::string const& table,
                                    CallbackType const& onFinish,
                                    int priority,
                                    bool keepTracking,
                                    std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};


/**
 * Class SqlDeleteTablePartitionRequest represents Controller-side requests for initiating
 * queries for removing one MySQL partition corresponding to a given "super-transaction"
 * identifier from tables at a remote worker nodes.
 */
class SqlDeleteTablePartitionRequest : public SqlBaseRequest  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<SqlDeleteTablePartitionRequest> Ptr;

    /// The function type for notifications on the completion of the request
    typedef std::function<void(Ptr)> CallbackType;

    // Default construction and copy semantics are prohibited

    SqlDeleteTablePartitionRequest() = delete;
    SqlDeleteTablePartitionRequest(SqlDeleteTablePartitionRequest const&) = delete;
    SqlDeleteTablePartitionRequest& operator=(SqlDeleteTablePartitionRequest const&) = delete;

    ~SqlDeleteTablePartitionRequest() final = default;

    std::string const& database() const { return requestBody.database(); }

    /**
     * Create a new request with specified parameters.
     *
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider
     *   is needed to access the Configuration and the Controller for communicating
     *   with the worker
     *
     * @param io_service
     *   a communication end-point
     *
     * @param worker
     *   identifier of a worker node
     *
     * @param database
     *   the name of an existing database where the table is residing
     *
     * @param table
     *   the name of a table affected by the operation
     *
     * @param transactionId
     *   a unique identifier of a transaction which corresponds to a MySQL
     *   partition to be removed.
     *
     * @param onFinish
     *   (optional) callback function to call upon completion of the request
     *
     * @param priority
     *   priority level of the request
     *
     * @param keepTracking
     *   keep tracking the request before it finishes or fails
     *
     * @param messenger
     *   interface for communicating with workers
     *
     * @return
     *   pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider,
                      boost::asio::io_service& io_service,
                      std::string const& worker,
                      std::string const& database,
                      std::string const& table,
                      uint32_t transactionId,
                      CallbackType const& onFinish,
                      int priority,
                      bool keepTracking,
                      std::shared_ptr<Messenger> const& messenger);

protected:

    /// @see Request::notify()
    void notify(util::Lock const& lock) final;

private:

    /// @see SqlDeleteTablePartitionRequest::create()
    SqlDeleteTablePartitionRequest(ServiceProvider::Ptr const& serviceProvider,
                                    boost::asio::io_service& io_service,
                                    std::string const& worker,
                                    std::string const& database,
                                    std::string const& table,
                                    uint32_t transactionId,
                                    CallbackType const& onFinish,
                                    int priority,
                                    bool keepTracking,
                                    std::shared_ptr<Messenger> const& messenger);

    CallbackType _onFinish; /// @note is reset when the request finishes
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SQLREQUEST_H
