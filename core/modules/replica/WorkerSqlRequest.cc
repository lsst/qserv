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
                                               ProtocolRequestSql const& request) {
    return WorkerSqlRequest::Ptr(
        new WorkerSqlRequest(serviceProvider,
                             worker,
                             id,
                             request));
}


WorkerSqlRequest::WorkerSqlRequest(ServiceProvider::Ptr const& serviceProvider,
                                   string const& worker,
                                   string const& id,
                                   ProtocolRequestSql const& request)
    :   WorkerRequest(serviceProvider,
                      worker,
                      "SQL",
                      id,
                      request.priority()),
        _request(request) {
}


void WorkerSqlRequest::setInfo(ProtocolResponseSql& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    response.set_allocated_target_performance(performance().info().release());

    // Carry over the result of the query only after the request
    // has finished (or failed).
    switch (status()) {
        case STATUS_SUCCEEDED:
        case STATUS_FAILED:
            response.set_error(            _response.error());
            response.set_char_set_name(    _response.char_set_name());
            response.set_has_result(       _response.has_result());
            *(response.mutable_fields()) = _response.fields();
            *(response.mutable_rows())   = _response.rows();
            *(response.mutable_request())= _request;
            break;
        default:
            break;
    }
}


bool WorkerSqlRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    switch (status()) {

        case STATUS_IN_PROGRESS: break;

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
        auto self = shared_from_base<WorkerSqlRequest>();
        conn = _connector();
        conn->execute([self](decltype(conn) const& conn_) {
            conn_->begin();
            conn_->execute(self->_query(conn_));
            self->_setResponse(conn_);
            conn_->commit();
        });
        setStatus(lock, STATUS_SUCCEEDED);

    } catch(database::mysql::NoSuchTable const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _response.set_error(ex.what());
        setStatus(lock, STATUS_FAILED, EXT_STATUS_NO_SUCH_TABLE);

    } catch(database::mysql::NotPartitionedTable const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _response.set_error(ex.what());
        setStatus(lock, STATUS_FAILED, EXT_STATUS_NOT_PARTITIONED_TABLE);

    } catch(database::mysql::Error const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  MySQL error: " << ex.what());
        _response.set_error(ex.what());
        setStatus(lock, STATUS_FAILED, EXT_STATUS_MYSQL_ERROR);

    } catch (invalid_argument const& ex) {

        LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << ex.what());
        _response.set_error(ex.what());
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


database::mysql::Connection::Ptr WorkerSqlRequest::_connector() const {

    // A choice of credential for connecting to the database service depends
    // on a type of the request. For the sake of greater security, arbitrary
    // queries require a client to explicitly provide the credentials.
    // Otherwise, using credentials from the worker's configuration.

    auto const config = serviceProvider()->config();
    auto const workerInfo = config->workerInfo(worker());    
    bool const clientCredentials = _request.type() == ProtocolRequestSql::QUERY;
    return database::mysql::Connection::open(
        database::mysql::ConnectionParams(
            workerInfo.dbHost,
            workerInfo.dbPort,
            clientCredentials ? _request.user() : workerInfo.dbUser,
            clientCredentials ? _request.password() : config->qservWorkerDatabasePassword(),
            ""
        )
    );
}


string WorkerSqlRequest::_query(database::mysql::Connection::Ptr const& conn) const {

    auto const config = serviceProvider()->config();
    auto const workerInfo = config->workerInfo(worker());    

    string const qservDbsTable = conn->sqlId("qservw_worker") + "." + conn->sqlId("Dbs");
    string const databaseTable = conn->sqlId(_request.database()) + "." + conn->sqlId(_request.table());

    auto const requestType = _request.type();
    switch (requestType) {

        case ProtocolRequestSql::QUERY:
            return _request.query();

        case ProtocolRequestSql::CREATE_DATABASE:
            return "CREATE DATABASE IF NOT EXISTS " + conn->sqlId(_request.database());

        case ProtocolRequestSql::DROP_DATABASE:
            return "DROP DATABASE IF EXISTS " + conn->sqlId(_request.database());

        case ProtocolRequestSql::ENABLE_DATABASE:

            // Using REPLACE instead of INSERT to avoid hitting the DUPLICATE KEY error
            // if such entry already exists in the table.
            return "REPLACE INTO " + qservDbsTable +
                   " VALUES ("    + conn->sqlValue(_request.database()) + ")";

        case ProtocolRequestSql::DISABLE_DATABASE:
            return "DELETE FROM " + qservDbsTable +
                   " WHERE "      + conn->sqlEqual("db", _request.database());

        case ProtocolRequestSql::GRANT_ACCESS:

            // ATTENTION: MySQL/MariaDB exhibits a somewhat unexpected behavior when putting
            // the usual MySQL identifier quotes around symbol '*' for table names, like in
            // this example:
            //   GRANT ALL ON 'db'.*
            // The server will result in adding an entry to table:
            //   mysql.tables_priv
            // Instead of (as expected):
            //   mysql.db;
            // Hence removing quotes from '*' an commenting the following statement:
            //   return "GRANT ALL ON " + conn->sqlId(_request.database()) + "." + conn->sqlId("*") +
            //          " TO " + conn->sqlValue(_request.user()) + "@" + conn->sqlValue(workerInfo.dbHost);

            return "GRANT ALL ON " + conn->sqlId(_request.database()) + ".* TO " +
                   conn->sqlValue(_request.user()) + "@" + conn->sqlValue(workerInfo.dbHost);

        case ProtocolRequestSql::CREATE_TABLE: {
            string query = "CREATE TABLE IF NOT EXISTS " + databaseTable + " (";
            for (int index = 0, num_columns = _request.columns_size(); index < num_columns; ++index) {
                auto const column = _request.columns(index);
                query += conn->sqlId(column.name()) + " " + column.type();
                if (index != num_columns - 1) query += ",";
            }
            query += ") ENGINE=" + _request.engine();

            // If MySQL partitioning was requested for the table then automatically
            // create the initial partition 'p0' corresponding to value '0'
            // of the key which is used for partitioning.
            string const partitionByColumn = _request.partition_by_column();
            if (not partitionByColumn.empty()) {
                query += " PARTITION BY LIST (" + conn->sqlId(partitionByColumn) +
                         ") (PARTITION `p0` VALUES IN (0) ENGINE = " + _request.engine() + ")";
            }
            return query;
        }
        case ProtocolRequestSql::DROP_TABLE:
            return "DROP TABLE IF EXISTS " + databaseTable;

        case ProtocolRequestSql::REMOVE_TABLE_PARTITIONING:
            return "ALTER TABLE " + databaseTable + " REMOVE PARTITIONING";

        case ProtocolRequestSql::DROP_TABLE_PARTITION:
            return "ALTER TABLE " + databaseTable + " DROP PARTITION IF EXISTS " +
                   conn->sqlId("p" + to_string(_request.transaction_id()));

        default:
            throw invalid_argument(
                    "WorkerSqlRequest::" + string(__func__) +
                    "  unsupported request type: " + ProtocolRequestSql_Type_Name(requestType));
    }
}


void WorkerSqlRequest::_setResponse(database::mysql::Connection::Ptr const& conn) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    _response.set_char_set_name(conn->charSetName());
    _response.set_has_result(conn->hasResult());

    if (conn->hasResult()) {
        for (size_t i=0; i < conn->numFields(); ++i) {
            conn->exportField(_response.add_fields(), i);
        }
        size_t numRowsProcessed = 0;
        database::mysql::Row row;
        while (conn->next(row)) {
            if (_request.max_rows() != 0) {
                if (numRowsProcessed >= _request.max_rows()) {
                    throw out_of_range(
                            "WorkerSqlRequest::" + context(__func__) + "  max_rows=" +
                            to_string(_request.max_rows()) + " limit exceeded");
                }
                ++numRowsProcessed;
            }
            row.exportRow(_response.add_rows());
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << " char_set_name: " << _response.char_set_name()
         << " has_result: " << (_response.has_result() ? 1 : 0)
         << " #fields: " << _response.fields_size()
         << " #rows: " << _response.rows_size());
}

}}} // namespace lsst::qserv::replica
