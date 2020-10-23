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

WorkerSqlRequest::Ptr WorkerSqlRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestSql const& request) {
    return WorkerSqlRequest::Ptr(new WorkerSqlRequest(serviceProvider,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
    ));
}


WorkerSqlRequest::WorkerSqlRequest(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestSql const& request)
    :   WorkerRequest(
            serviceProvider,
            worker,
            "SQL",
            id,
            priority,
            onExpired,
            requestExpirationIvalSec),
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
            *(response.mutable_result_sets()) = _response.result_sets();
            break;
        default:
            break;
    }
    *(response.mutable_request()) = _request;
}


bool WorkerSqlRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    util::Lock lock(_mtx, context(__func__));

    switch (status()) {
        case STATUS_IN_PROGRESS: break;
        case STATUS_IS_CANCELLING:
            setStatus(lock, STATUS_CANCELLED);
            throw WorkerRequestCancelled();
        default:
            throw logic_error(
                    "WorkerSqlRequest::" + context(__func__) + "  not allowed while in state: " +
                    WorkerRequest::status2string(status()));
    }
    try {

        // Pre-create the default result-set message before any operations with
        // the database service. This is needed to report errors.
        auto currentResultSet = _response.add_result_sets();

        // Open the connection once and then manage transactions via
        // the connection handlers down below to ensure no lingering transactions
        // are left after the completion of the request's execution (whether it's
        // successful or not).
        auto const connection = _connector();

        // Check if this is the "batch" request which involves executing
        // a series of queries. This kind of requests needs to be processed
        // slightly differently since we need to intercept and properly handle
        // a few known (and somewhat expected) MySQL errors w/o aborting
        // the whole request.
        if (_batchMode()) {

            // Count the number of failures for proper error reporting on
            // the current request.
            size_t numFailures = 0;

            for (int i = 0; i < _request.tables_size(); ++i) {
                string const table = _request.tables(i);

                // If this is the very first iteration of the loop then use
                // the default result set created earlier. Otherwise create
                // a new one.
                if (i > 0) currentResultSet = _response.add_result_sets();
                currentResultSet->set_scope(table);      

                try {
                    database::mysql::ConnectionHandler const h(connection);
                    h.conn->execute([&](decltype(h.conn) const& conn_) {
                        conn_->begin();
                        conn_->execute(_batchQuery(conn_, table));
                        _extractResultSet(lock, conn_);
                        conn_->commit();
                    });
                } catch(database::mysql::NoSuchTable const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::NO_SUCH_TABLE);
                    currentResultSet->set_error(ex.what());

                } catch(database::mysql::NotPartitionedTable const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::NOT_PARTITIONED_TABLE);
                    currentResultSet->set_error(ex.what());

                } catch(database::mysql::DuplicateKeyName const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::DUPLICATE_KEY);
                    currentResultSet->set_error(ex.what());

                } catch(database::mysql::CantDropFieldOrKey const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::CANT_DROP_KEY);
                    currentResultSet->set_error(ex.what());

                }
            }
            if (numFailures > 0) {
                setStatus(lock, STATUS_FAILED, EXT_STATUS_MULTIPLE);
            } else {
                setStatus(lock, STATUS_SUCCEEDED);
            }

        } else {
            database::mysql::ConnectionHandler const h(connection);
            h.conn->execute([&](decltype(h.conn) const& conn_) {
                conn_->begin();
                conn_->execute(_query(conn_));
                _extractResultSet(lock, conn_);
                conn_->commit();
            });
            setStatus(lock, STATUS_SUCCEEDED);
        }

    } catch(database::mysql::NoSuchTable const& ex) {
        _reportFailure(lock, EXT_STATUS_NO_SUCH_TABLE, ex.what());

    } catch(database::mysql::NotPartitionedTable const& ex) {
        _reportFailure(lock, EXT_STATUS_NOT_PARTITIONED_TABLE, ex.what());

    } catch(database::mysql::DuplicateKeyName const& ex) {
        _reportFailure(lock, EXT_STATUS_DUPLICATE_KEY, ex.what());

    } catch(database::mysql::CantDropFieldOrKey const& ex) {
        _reportFailure(lock, EXT_STATUS_CANT_DROP_KEY, ex.what());

    } catch(database::mysql::Error const& ex) {
        _reportFailure(lock, EXT_STATUS_MYSQL_ERROR, ex.what());

    } catch (invalid_argument const& ex) {
        _reportFailure(lock, EXT_STATUS_INVALID_PARAM, ex.what());

    } catch (out_of_range const& ex) {
        _reportFailure(lock, EXT_STATUS_LARGE_RESULT, ex.what());

    } catch (exception const& ex) {
        _reportFailure(lock, EXT_STATUS_OTHER_EXCEPTION, ex.what());
    }
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

    switch (_request.type()) {

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
                   conn->sqlPartitionId(_request.transaction_id());

        case ProtocolRequestSql::ALTER_TABLE:
            return "ALTER TABLE " + databaseTable + " " + _request.alter_spec();

        default:
            throw invalid_argument(
                    "WorkerSqlRequest::" + string(__func__) +
                    "  unsupported request type: " + ProtocolRequestSql_Type_Name(_request.type()));
    }
}


string WorkerSqlRequest::_batchQuery(database::mysql::Connection::Ptr const& conn,
                                     string const& table) const {

    string const databaseTable = conn->sqlId(_request.database()) + "." + conn->sqlId(table);

    switch (_request.type()) {
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

        case ProtocolRequestSql::DROP_TABLE_PARTITION:
            return "ALTER TABLE " + databaseTable + " DROP PARTITION IF EXISTS "
                   + conn->sqlPartitionId(_request.transaction_id());

        case ProtocolRequestSql::REMOVE_TABLE_PARTITIONING:
            return "ALTER TABLE " + databaseTable + " REMOVE PARTITIONING";

        case ProtocolRequestSql::CREATE_TABLE_INDEX: {
            string const spec = _request.index_spec() == ProtocolRequestSql::DEFAULT ?
                    "" : ProtocolRequestSql_IndexSpec_Name(_request.index_spec());
            string keys;
            for (int i = 0; i < _request.index_columns_size(); ++i) {
                auto const& key = _request.index_columns(i);
                if (i != 0) keys += ",";
                keys += conn->sqlId(key.name());
                if (key.length() != 0) keys += "(" + to_string(key.length()) + ")";
                keys += key.ascending() ? " ASC" : " DESC";
            }
            return "CREATE " + spec + " INDEX " + conn->sqlId(_request.index_name()) + " ON " + databaseTable +
                   " (" + keys + ")" + " COMMENT " + conn->sqlValue(_request.index_comment());
        }

        case ProtocolRequestSql::DROP_TABLE_INDEX:
            return "DROP INDEX " + conn->sqlId(_request.index_name()) + " ON " + databaseTable;

        case ProtocolRequestSql::GET_TABLE_INDEX:
            return "SHOW INDEXES FROM " + databaseTable;

        case ProtocolRequestSql::ALTER_TABLE:
            return "ALTER TABLE " + databaseTable + " " + _request.alter_spec();

        default:
            throw invalid_argument(
                    "WorkerSqlRequest::" + string(__func__) +
                    "  not the batch request type: " + ProtocolRequestSql_Type_Name(_request.type()));
    }
}

void WorkerSqlRequest::_extractResultSet(util::Lock const& lock,
                                         database::mysql::Connection::Ptr const& conn) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    auto resultSet = _currentResultSet(lock);

    // This will explicitly reset the default failure mode as it was
    // initialized by the constructor of the result set class.
    resultSet->set_status_ext(ProtocolStatusExt::NONE);

    // Now carry over the actual rest set (if any)
    resultSet->set_char_set_name(conn->charSetName());
    resultSet->set_has_result(conn->hasResult());

    if (conn->hasResult()) {
        for (size_t i=0; i < conn->numFields(); ++i) {
            conn->exportField(resultSet->add_fields(), i);
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
            row.exportRow(resultSet->add_rows());
        }
    }
}


void WorkerSqlRequest::_reportFailure(util::Lock const& lock,
                                      ExtendedCompletionStatus statusExt,
                                      string const& error) {

    LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << error);

    // Note that the actual reason for a query to fail is recorded in its
    // result set, while the final state of the whole request may vary
    // depending on a kind of the request - if it's a simple or the "batch"
    // request.

    auto resultSet = _currentResultSet(lock);

    resultSet->set_status_ext(translate(statusExt));
    resultSet->set_error(error);

    setStatus(lock, STATUS_FAILED,
              _batchMode() ? statusExt : EXT_STATUS_MULTIPLE);
}


ProtocolResponseSqlResultSet* WorkerSqlRequest::_currentResultSet(util::Lock const& lock) {
    auto const numResultSets = _response.result_sets_size();
    if (numResultSets < 1) {
        throw logic_error(
                "WorkerSqlRequest::" + context(__func__)
                + " the operation is not allowed in this state");
    }
    return _response.mutable_result_sets(numResultSets - 1);;
}


bool WorkerSqlRequest::_batchMode() const {
    return _request.has_batch_mode() and _request.batch_mode();
}

}}} // namespace lsst::qserv::replica
