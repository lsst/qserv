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
#include "replica/SqlRequest.h"

// System headers
#include <stdexcept>
#include <iostream>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Controller.h"
#include "replica/DatabaseServices.h"
#include "replica/Messenger.h"
#include "replica/ServiceProvider.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

void SqlBaseRequest::extendedPrinter(Ptr const& ptr) {

    Request::defaultPrinter(ptr);

    auto&& resultSet = ptr->responseData();
    if (resultSet.hasResult) {

        string const caption = "RESULT SET";
        string const indent  = "";

        auto const table = resultSet.toColumnTable(caption, indent);

        bool const topSeparator    = false;
        bool const bottomSeparator = false;
        bool const repeatedHeader  = false;

        size_t const pageSize = 0;

        table.print(cout, topSeparator, bottomSeparator, pageSize, repeatedHeader);
    }
}


SqlBaseRequest::SqlBaseRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        uint64_t maxRows,
        int  priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   RequestMessenger(
            serviceProvider,
            io_service,
            "SQL",
            worker,
            priority,
            keepTracking,
            false /* allowDuplicate */,
            messenger
        ) {

    // Partial initialization of the request body's content. Other members
    // will be set in the request type-specific subclasses.
    requestBody.set_priority(priority);
    requestBody.set_max_rows(maxRows);
}


SqlResultSet const& SqlBaseRequest::responseData() const {
    return _responseData;
}


void SqlBaseRequest::startImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Serialize the Request message header and the request body into
    // the network buffer.

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::QUEUED);
    hdr.set_queued_type(ProtocolQueuedRequestType::SQL);

    buffer()->serialize(hdr);
    buffer()->serialize(requestBody);

    _send(lock);
}


void SqlBaseRequest::_waitAsync(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Always need to set the interval before launching the timer.

    timer().expires_from_now(boost::posix_time::milliseconds(nextTimeIvalMsec()));
    timer().async_wait(
        boost::bind(
            &SqlBaseRequest::_awaken,
            shared_from_base<SqlBaseRequest>(),
            boost::asio::placeholders::error
        )
    );
}


void SqlBaseRequest::_awaken(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (isAborted(ec)) return;

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // Serialize the Status message header and the status request's body into
    // the network buffer.

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::REQUEST);
    hdr.set_management_type(ProtocolManagementRequestType::REQUEST_STATUS);

    buffer()->serialize(hdr);

    ProtocolRequestStatus statusRequestBody;
    statusRequestBody.set_id(id());
    statusRequestBody.set_queued_type(ProtocolQueuedRequestType::SQL);

    buffer()->serialize(statusRequestBody);

    _send(lock);
}


void SqlBaseRequest::_send(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    auto self = shared_from_base<SqlBaseRequest>();

    messenger()->send<ProtocolResponseSql>(
        worker(),
        id(),
        buffer(),
        [self] (string const& id, bool success, ProtocolResponseSql const& response) {
            self->_analyze(success, response);
        }
    );
}


void SqlBaseRequest::_analyze(bool success,
                              ProtocolResponseSql const& response) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  success=" << (success ? "true" : "false"));

    // This method is called on behalf of an asynchronous callback fired
    // upon a completion of the request within method _send() - the only
    // client of _analyze(). So, we should take care of proper locking and watch
    // for possible state transition which might occur while the async I/O was
    // still in a progress.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    if (not success) {
        finish(lock, CLIENT_ERROR);
        return;
    }

    // Always use  the latest status reported by the remote server

    setExtendedServerStatus(lock, replica::translate(response.status_ext()));

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.

    if (response.has_target_performance()) {
        mutablePerformance().update(response.target_performance());
    } else {
        mutablePerformance().update(response.performance());
    }

    // Always extract extended data regardless of the completion status
    // reported by the worker service.

    _responseData.set(response);
    _responseData.performanceSec =
        (PerformanceUtils::now() - performance(lock).c_create_time) / 1000.;

    // Extract target request type-specific parameters from the response
    if (response.has_request()) {
        _targetRequestParams = SqlRequestParams(response.request());
    }
    switch (response.status()) {

        case ProtocolStatus::SUCCESS:
            finish(lock, SUCCESS);
            break;

        case ProtocolStatus::QUEUED:
            if (keepTracking()) _waitAsync(lock);
            else                finish(lock, SERVER_QUEUED);
            break;

        case ProtocolStatus::IN_PROGRESS:
            if (keepTracking()) _waitAsync(lock);
            else                finish(lock, SERVER_IN_PROGRESS);
            break;

        case ProtocolStatus::IS_CANCELLING:
            if (keepTracking()) _waitAsync(lock);
            else                finish(lock, SERVER_IS_CANCELLING);
            break;

        case ProtocolStatus::BAD:
            finish(lock, SERVER_BAD);
            break;

        case ProtocolStatus::FAILED:
            finish(lock, SERVER_ERROR);
            break;

        case ProtocolStatus::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;

        default:
            throw logic_error(
                    "SqlBaseRequest::" + string(__func__) + "  unknown status '" +
                    ProtocolStatus_Name(response.status()) + "' received from server");
    }
}


void SqlBaseRequest::savePersistentState(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}


list<pair<string,string>> SqlBaseRequest::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("type", ProtocolRequestSql_Type_Name(requestBody.type()));
    result.emplace_back("max_rows", to_string(requestBody.max_rows()));
    result.emplace_back("query", requestBody.query());
    result.emplace_back("user", requestBody.user());
    result.emplace_back("database", requestBody.database());
    result.emplace_back("table", requestBody.table());
    result.emplace_back("engine", requestBody.engine());
    result.emplace_back("partition_by_column", requestBody.partition_by_column());
    result.emplace_back("transaction_id", to_string(requestBody.transaction_id()));
    result.emplace_back("num_columns", to_string(requestBody.columns_size()));
    return result;
}


SqlQueryRequest::Ptr SqlQueryRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& query,
        std::string const& user,
        std::string const& password,
        uint64_t maxRows,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlQueryRequest(
        serviceProvider,
        io_service,
        worker,
        query,
        user,
        password,
        maxRows,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlQueryRequest::SqlQueryRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& query,
        std::string const& user,
        std::string const& password,
        uint64_t maxRows,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(
            serviceProvider,
            io_service,
            worker,
            maxRows,
            priority,
            keepTracking,
            messenger
        ),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::QUERY);
    requestBody.set_query(query);
    requestBody.set_user(user);
    requestBody.set_password(password);
}


void SqlQueryRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlQueryRequest>(lock, _onFinish);
}


SqlCreateDbRequest::Ptr SqlCreateDbRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlCreateDbRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlCreateDbRequest::SqlCreateDbRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(serviceProvider,
                       io_service,
                       worker,
                       0 /* maxRows */,
                       priority,
                       keepTracking,
                       messenger),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::CREATE_DATABASE);
    requestBody.set_database(database);
}


void SqlCreateDbRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlCreateDbRequest>(lock, _onFinish);
}


SqlDeleteDbRequest::Ptr SqlDeleteDbRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlDeleteDbRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlDeleteDbRequest::SqlDeleteDbRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(serviceProvider,
                       io_service,
                       worker,
                       0 /* maxRows */,
                       priority,
                       keepTracking,
                       messenger),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::DROP_DATABASE);
    requestBody.set_database(database);
}


void SqlDeleteDbRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlDeleteDbRequest>(lock, _onFinish);
}


SqlEnableDbRequest::Ptr SqlEnableDbRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlEnableDbRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlEnableDbRequest::SqlEnableDbRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(serviceProvider,
                       io_service,
                       worker,
                       0 /* maxRows */,
                       priority,
                       keepTracking,
                       messenger),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::ENABLE_DATABASE);
    requestBody.set_database(database);
}


void SqlEnableDbRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlEnableDbRequest>(lock, _onFinish);
}


SqlDisableDbRequest::Ptr SqlDisableDbRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlDisableDbRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlDisableDbRequest::SqlDisableDbRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(serviceProvider,
                       io_service,
                       worker,
                       0 /* maxRows */,
                       priority,
                       keepTracking,
                       messenger),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::DISABLE_DATABASE);
    requestBody.set_database(database);
}


void SqlDisableDbRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlDisableDbRequest>(lock, _onFinish);
}


SqlGrantAccessRequest::Ptr SqlGrantAccessRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& user,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlGrantAccessRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        user,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlGrantAccessRequest::SqlGrantAccessRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& user,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(serviceProvider,
                       io_service,
                       worker,
                       0 /* maxRows */,
                       priority,
                       keepTracking,
                       messenger),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::GRANT_ACCESS);
    requestBody.set_database(database);
    requestBody.set_user(user);
}


void SqlGrantAccessRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlGrantAccessRequest>(lock, _onFinish);
}


SqlCreateTableRequest::Ptr SqlCreateTableRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        std::string const& engine,
        string const& partitionByColumn,
        std::list<std::pair<std::string, std::string>> const& columns,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlCreateTableRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        table,
        engine,
        partitionByColumn,
        columns,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlCreateTableRequest::SqlCreateTableRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        std::string const& engine,
        string const& partitionByColumn,
        std::list<std::pair<std::string, std::string>> const& columns,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(
            serviceProvider,
            io_service,
            worker,
            0,          /* maxRows */
            priority,
            keepTracking,
            messenger
        ),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::CREATE_TABLE);
    requestBody.set_database(database);
    requestBody.set_table(table);
    requestBody.set_engine(engine);
    requestBody.set_partition_by_column(partitionByColumn);
    for (auto&& column: columns) {
        auto out = requestBody.add_columns();
        out->set_name(column.first);
        out->set_type(column.second);
    }
}


void SqlCreateTableRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlCreateTableRequest>(lock, _onFinish);
}


SqlDeleteTableRequest::Ptr SqlDeleteTableRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlDeleteTableRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        table,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlDeleteTableRequest::SqlDeleteTableRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(
            serviceProvider,
            io_service,
            worker,
            0,          /* maxRows */
            priority,
            keepTracking,
            messenger
        ),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::DROP_TABLE);
    requestBody.set_database(database);
    requestBody.set_table(table);
}


void SqlDeleteTableRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlDeleteTableRequest>(lock, _onFinish);
}


SqlRemoveTablePartitionsRequest::Ptr SqlRemoveTablePartitionsRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlRemoveTablePartitionsRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        table,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlRemoveTablePartitionsRequest::SqlRemoveTablePartitionsRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(
            serviceProvider,
            io_service,
            worker,
            0,          /* maxRows */
            priority,
            keepTracking,
            messenger
        ),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::REMOVE_TABLE_PARTITIONING);
    requestBody.set_database(database);
    requestBody.set_table(table);
}


void SqlRemoveTablePartitionsRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlRemoveTablePartitionsRequest>(lock, _onFinish);
}


SqlDeleteTablePartitionRequest::Ptr SqlDeleteTablePartitionRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        uint32_t transactionId,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {

    return Ptr(new SqlDeleteTablePartitionRequest(
        serviceProvider,
        io_service,
        worker,
        database,
        table,
        transactionId,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


SqlDeleteTablePartitionRequest::SqlDeleteTablePartitionRequest(
        ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::string const& database,
        std::string const& table,
        uint32_t transactionId,
        CallbackType const& onFinish,
        int priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   SqlBaseRequest(
            serviceProvider,
            io_service,
            worker,
            0,          /* maxRows */
            priority,
            keepTracking,
            messenger
        ),
        _onFinish(onFinish) {

    // Finish initializing the request body's content
    requestBody.set_type(ProtocolRequestSql::DROP_TABLE_PARTITION);
    requestBody.set_database(database);
    requestBody.set_table(table);
    requestBody.set_transaction_id(transactionId);
}


void SqlDeleteTablePartitionRequest::notify(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ <<
        "[" << ProtocolRequestSql_Type_Name(requestBody.type()) << "]");

    notifyDefaultImpl<SqlDeleteTablePartitionRequest>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
