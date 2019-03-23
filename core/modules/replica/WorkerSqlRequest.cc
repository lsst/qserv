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
#include "replica/WorkerSqlRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/Performance.h"
#include "util/BlockPost.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerSqlRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

WorkerSqlRequest::Ptr WorkerSqlRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                               string const& worker,
                                               string const& id,
                                               int priority,
                                               std::string const& query,
                                               std::string const& user,
                                               std::string const& password,
                                               size_t maxRows) {
    return WorkerSqlRequest::Ptr(
        new WorkerSqlRequest(serviceProvider,
                             worker,
                             id,
                             priority,
                             query,
                             user,
                             password,
                             maxRows));
}


WorkerSqlRequest::WorkerSqlRequest(ServiceProvider::Ptr const& serviceProvider,
                                   string const& worker,
                                   string const& id,
                                   int priority,
                                   std::string const& query,
                                   std::string const& user,
                                   std::string const& password,
                                   size_t maxRows)
    :   WorkerRequest(serviceProvider,
                      worker,
                      "SQL",
                      id,
                      priority),
        _query(query),
        _user(user),
        _password(password),
        _maxRows(maxRows) {
}


void WorkerSqlRequest::setInfo(ProtocolResponseSql& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    // Update the performance of the target request before returning it
    response.set_allocated_target_performance(performance().info());

    // Carry over the result of the query only after the request
    // has finished (or failed).
    switch (status()) {
        case STATUS_SUCCEEDED:
        case STATUS_FAILED:
            response.set_error(            _response.error());
            response.set_has_result(       _response.has_result());
            *(response.mutable_fields()) = _response.fields();
            *(response.mutable_rows())   = _response.rows();
            break;
        default:
            break;
    }
}


bool WorkerSqlRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    switch (status()) {

        case STATUS_IN_PROGRESS:
            break;

        case STATUS_IS_CANCELLING:

            // Abort the operation right away

            setStatus(lock, STATUS_CANCELLED);
            throw WorkerRequestCancelled();

        default:
            throw logic_error(
                    "WorkerSqlRequest::" + context(__func__) + "  not allowed while in state: " +
                    WorkerRequest::status2string(status()));
    }

    database::mysql::Connection::Ptr conn;
    try {
        auto const workerInfo = serviceProvider()->config()->workerInfo(worker());
        conn = database::mysql::Connection::open(
            database::mysql::ConnectionParams(
                workerInfo.dbHost,
                workerInfo.dbPort,
                user(),
                password(),
                ""));

        auto self = shared_from_base<WorkerSqlRequest>();
        conn->execute([self](decltype(conn) const& conn_) {
            conn_->begin();
            conn_->execute(self->query());
            self->_setResponse(conn_);
            conn_->commit();
        });
        setStatus(lock, STATUS_SUCCEEDED);

    } catch(database::mysql::Error const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _response.set_error(ex.what());
        setStatus(lock, STATUS_FAILED, EXT_STATUS_MYSQL_ERROR);

    } catch (invalid_argument const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  no such worker: " << worker());
        _response.set_error("No such worker in the Configuration, worker: " + worker());
        setStatus(lock, STATUS_FAILED, EXT_STATUS_INVALID_PARAM);

    } catch (out_of_range const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << ex.what());
        _response.set_error(ex.what());
        setStatus(lock, STATUS_FAILED, EXT_STATUS_LARGE_RESULT);

    } catch (exception const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << ex.what());
        _response.set_error("Exception: " + string(ex.what()));
        setStatus(lock, STATUS_FAILED);

    }
    if ((nullptr != conn) and conn->inTransaction()) conn->rollback();
    return true;
}


void WorkerSqlRequest::_setResponse(database::mysql::Connection::Ptr const& conn) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    _response.set_has_result(conn->hasResult());
    if (conn->hasResult()) {
        for (size_t i=0; i < conn->numFields(); ++i) {
            conn->exportField(_response.add_fields(), i);
        }
        size_t numRowsProcessed = 0;
        database::mysql::Row row;
        while (conn->next(row)) {
            if (_maxRows != 0) {
                if (numRowsProcessed >= _maxRows) {
                    throw out_of_range(
                            "WorkerSqlRequest::" + context(__func__) + "  maxRows=" +
                            to_string(_maxRows) + " limit exceeded");
                }
                ++numRowsProcessed;
            }
            row.exportRow(_response.add_rows());
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << " has_result: " << (_response.has_result() ? 1 : 0)
         << " #fields: " << _response.fields_size()
         << " #rows: " << _response.rows_size());
}

}}} // namespace lsst::qserv::replica
