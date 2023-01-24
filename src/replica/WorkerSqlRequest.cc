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
#include "replica/DatabaseMySQLUtils.h"
#include "replica/Performance.h"
#include "replica/Mutex.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerSqlRequest");

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

WorkerSqlRequest::Ptr WorkerSqlRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                               string const& worker, string const& id, int priority,
                                               ExpirationCallbackType const& onExpired,
                                               unsigned int requestExpirationIvalSec,
                                               ProtocolRequestSql const& request) {
    return WorkerSqlRequest::Ptr(new WorkerSqlRequest(serviceProvider, worker, id, priority, onExpired,
                                                      requestExpirationIvalSec, request));
}

WorkerSqlRequest::WorkerSqlRequest(ServiceProvider::Ptr const& serviceProvider, string const& worker,
                                   string const& id, int priority, ExpirationCallbackType const& onExpired,
                                   unsigned int requestExpirationIvalSec, ProtocolRequestSql const& request)
        : WorkerRequest(serviceProvider, worker, "SQL", id, priority, onExpired, requestExpirationIvalSec),
          _request(request) {}

void WorkerSqlRequest::setInfo(ProtocolResponseSql& response) const {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    replica::Lock lock(_mtx, context(__func__));

    response.set_allocated_target_performance(performance().info().release());

    // Carry over the result of the query only after the request
    // has finished (or failed).
    switch (status()) {
        case ProtocolStatus::SUCCESS:
        case ProtocolStatus::FAILED:
            *(response.mutable_result_sets()) = _response.result_sets();
            break;
        default:
            break;
    }
    *(response.mutable_request()) = _request;
}

bool WorkerSqlRequest::execute() {
    string const context_ = "WorkerSqlRequest::" + context(__func__);
    LOGS(_log, LOG_LVL_DEBUG, context_);
    replica::Lock lock(_mtx, context_);

    switch (status()) {
        case ProtocolStatus::IN_PROGRESS:
            break;
        case ProtocolStatus::IS_CANCELLING:
            setStatus(lock, ProtocolStatus::CANCELLED);
            throw WorkerRequestCancelled();
        default:
            throw logic_error(context_ +
                              "  not allowed while in state: " + WorkerRequest::status2string(status()));
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
                    ConnectionHandler const h(connection);
                    h.conn->execute([&](decltype(h.conn) const& conn_) {
                        conn_->begin();
                        auto const query = _query(conn_, table);
                        if (query.mutexName.empty()) {
                            conn_->execute(query.query);
                        } else {
                            replica::Lock const lock(serviceProvider()->getNamedMutex(query.mutexName),
                                                     context_);
                            conn_->execute(query.query);
                        }
                        _extractResultSet(lock, conn_);
                        conn_->commit();
                    });
                } catch (database::mysql::ER_NO_SUCH_TABLE_ const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::NO_SUCH_TABLE);
                    currentResultSet->set_error(ex.what());

                } catch (database::mysql::ER_PARTITION_MGMT_ON_NONPARTITIONED_ const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::NOT_PARTITIONED_TABLE);
                    currentResultSet->set_error(ex.what());

                } catch (database::mysql::ER_DUP_KEYNAME_ const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::DUPLICATE_KEY);
                    currentResultSet->set_error(ex.what());

                } catch (database::mysql::ER_CANT_DROP_FIELD_OR_KEY_ const& ex) {
                    ++numFailures;
                    currentResultSet->set_status_ext(ProtocolStatusExt::CANT_DROP_KEY);
                    currentResultSet->set_error(ex.what());
                }
            }
            if (numFailures > 0) {
                setStatus(lock, ProtocolStatus::FAILED, ProtocolStatusExt::MULTIPLE);
            } else {
                setStatus(lock, ProtocolStatus::SUCCESS);
            }

        } else {
            // TODO: the algorithm will only report a result set of the last query
            // from the multi-query collections. The implementations of the corresponding
            // requests should take this into account.
            ConnectionHandler const h(connection);
            h.conn->execute([&](decltype(h.conn) const& conn_) {
                conn_->begin();
                for (auto const& query : _queries(conn_)) {
                    if (query.mutexName.empty()) {
                        conn_->execute(query.query);
                    } else {
                        replica::Lock const lock(serviceProvider()->getNamedMutex(query.mutexName), context_);
                        conn_->execute(query.query);
                    }
                    _extractResultSet(lock, conn_);
                }
                conn_->commit();
            });
            setStatus(lock, ProtocolStatus::SUCCESS);
        }

    } catch (database::mysql::ER_NO_SUCH_TABLE_ const& ex) {
        _reportFailure(lock, ProtocolStatusExt::NO_SUCH_TABLE, ex.what());

    } catch (database::mysql::ER_PARTITION_MGMT_ON_NONPARTITIONED_ const& ex) {
        _reportFailure(lock, ProtocolStatusExt::NOT_PARTITIONED_TABLE, ex.what());

    } catch (database::mysql::ER_DUP_KEYNAME_ const& ex) {
        _reportFailure(lock, ProtocolStatusExt::DUPLICATE_KEY, ex.what());

    } catch (database::mysql::ER_CANT_DROP_FIELD_OR_KEY_ const& ex) {
        _reportFailure(lock, ProtocolStatusExt::CANT_DROP_KEY, ex.what());

    } catch (database::mysql::Error const& ex) {
        _reportFailure(lock, ProtocolStatusExt::MYSQL_ERROR, ex.what());

    } catch (invalid_argument const& ex) {
        _reportFailure(lock, ProtocolStatusExt::INVALID_PARAM, ex.what());

    } catch (out_of_range const& ex) {
        _reportFailure(lock, ProtocolStatusExt::LARGE_RESULT, ex.what());

    } catch (exception const& ex) {
        _reportFailure(lock, ProtocolStatusExt::OTHER_EXCEPTION, ex.what());
    }
    return true;
}

Connection::Ptr WorkerSqlRequest::_connector() const {
    // A choice of credential for connecting to the database service depends
    // on a type of the request. For the sake of greater security, arbitrary
    // queries require a client to explicitly provide the credentials.
    // Otherwise, using credentials from the worker's configuration.

    bool const clientCredentials = _request.type() == ProtocolRequestSql::QUERY;
    auto connectionParams = Configuration::qservWorkerDbParams();
    if (clientCredentials) {
        connectionParams.user = _request.user();
        connectionParams.password = _request.password();
    }
    return Connection::open(connectionParams);
}

vector<Query> WorkerSqlRequest::_queries(Connection::Ptr const& conn) const {
    QueryGenerator const g(conn);
    vector<Query> queries;
    switch (_request.type()) {
        case ProtocolRequestSql::QUERY:
            queries.emplace_back(Query(_request.query()));
            break;

        case ProtocolRequestSql::CREATE_DATABASE: {
            bool const ifNotExists = true;
            string const query = g.createDb(_request.database(), ifNotExists);
            queries.emplace_back(Query(query));
            break;
        }
        case ProtocolRequestSql::DROP_DATABASE: {
            bool const ifExists = true;
            string const query = g.dropDb(_request.database(), ifExists);
            queries.emplace_back(Query(query));
            break;
        }
        case ProtocolRequestSql::ENABLE_DATABASE: {
            // Using REPLACE instead of INSERT to avoid hitting the DUPLICATE KEY error
            // if such entry already exists in the table.
            string const query = g.replace("qservw_worker", "Dbs", _request.database());
            queries.emplace_back(Query(query));
            break;
        }
        case ProtocolRequestSql::DISABLE_DATABASE: {
            string const where = g.where(g.eq("db", _request.database()));
            queries.emplace_back(Query(g.delete_(g.id("qservw_worker", "Chunks")) + where));
            queries.emplace_back(Query(g.delete_(g.id("qservw_worker", "Dbs")) + where));
            break;
        }
        case ProtocolRequestSql::GRANT_ACCESS: {
            string const query = g.grant("ALL", _request.database(), _request.user(), "localhost");
            queries.emplace_back(Query(query));
            break;
        }
        default:
            // The remaining remaining types of requests require the name of a table
            // affected by the operation.
            queries.emplace_back(_query(conn, _request.table()));
            break;
    }
    return queries;
}

Query WorkerSqlRequest::_query(Connection::Ptr const& conn, string const& table) const {
    QueryGenerator const g(conn);
    SqlId const databaseTable = g.id(_request.database(), table);

    switch (_request.type()) {
        case ProtocolRequestSql::CREATE_TABLE: {
            list<SqlColDef> columns;
            for (int index = 0, num_columns = _request.columns_size(); index < num_columns; ++index) {
                auto const column = _request.columns(index);
                columns.emplace_back(SqlColDef{column.name(), column.type()});
            }
            list<string> const keys;
            bool const ifNotExists = true;
            string query = g.createTable(databaseTable, ifNotExists, columns, keys, _request.engine());

            // If MySQL partitioning was requested for the table then configure partitioning
            // parameters and add the initial partition corresponding to the default
            // transaction identifier. The table will be partitioned based on values of
            // the transaction identifiers in the specified column.
            string const partitionByColumn = _request.partition_by_column();
            if (!partitionByColumn.empty()) {
                TransactionId const defaultTransactionId = 0;
                query += g.partitionByList(partitionByColumn) + g.partition(defaultTransactionId);
            }
            return Query(query, databaseTable.str);
        }

        case ProtocolRequestSql::DROP_TABLE: {
            bool const ifExists = true;
            string const query = g.dropTable(databaseTable, ifExists);
            return Query(query, databaseTable.str);
        }
        case ProtocolRequestSql::DROP_TABLE_PARTITION: {
            bool const ifExists = true;
            string const query =
                    g.alterTable(databaseTable) + g.dropPartition(_request.transaction_id(), ifExists);
            return Query(query, databaseTable.str);
        }
        case ProtocolRequestSql::REMOVE_TABLE_PARTITIONING: {
            string const query = g.alterTable(databaseTable) + g.removePartitioning();
            return Query(query, databaseTable.str);
        }
        case ProtocolRequestSql::CREATE_TABLE_INDEX: {
            string spec;
            if (_request.index_spec() != ProtocolRequestSql::DEFAULT) {
                spec = ProtocolRequestSql_IndexSpec_Name(_request.index_spec());
            }
            list<tuple<string, unsigned int, bool>> keys;
            for (int i = 0; i < _request.index_columns_size(); ++i) {
                auto const& key = _request.index_columns(i);
                keys.emplace_back(make_tuple(key.name(), key.length(), key.ascending()));
            }
            string const query =
                    g.createIndex(databaseTable, _request.index_name(), spec, keys, _request.index_comment());
            return Query(query, databaseTable.str);
        }
        case ProtocolRequestSql::DROP_TABLE_INDEX: {
            string const query = g.dropIndex(databaseTable, _request.index_name());
            return Query(query, databaseTable.str);
        }
        case ProtocolRequestSql::GET_TABLE_INDEX: {
            return Query(g.showIndexes(databaseTable));
        }
        case ProtocolRequestSql::ALTER_TABLE: {
            string const query = g.alterTable(databaseTable, _request.alter_spec());
            return Query(query, databaseTable.str);
        }
        case ProtocolRequestSql::TABLE_ROW_STATS: {
            // The transaction identifier column is not required to be present in
            // the legacy catalogs (ingested w/o super-transactions), or in (the narrow) tables
            // in which the column was removed to save disk space. The query generator
            // implemented below accounts for this scenario by consulting MySQL's
            // information schema. If the column isn't present then the default transaction
            // identifier 0 will be injected into the result set.
            string query = g.select(Sql::COUNT_STAR) +
                           g.from(DoNotProcess(g.id("information_schema", "COLUMNS"))) +
                           g.where(g.eq("TABLE_SCHEMA", _request.database()), g.eq("TABLE_NAME", table),
                                   g.eq("COLUMN_NAME", "qserv_trans_id"));
            int count = 0;
            selectSingleValue(conn, query, count);
            if (count == 0) {
                string const query =
                        g.select(g.as(g.val(0), "qserv_trans_id"), g.as(Sql::COUNT_STAR, "num_rows")) +
                        g.from(DoNotProcess(databaseTable));
                return Query(query);
            }
            query = g.select("qserv_trans_id", g.as(Sql::COUNT_STAR, "num_rows")) +
                    g.from(DoNotProcess(databaseTable)) + g.groupBy("qserv_trans_id");
            return Query(query);
        }
        default:
            throw invalid_argument(
                    "WorkerSqlRequest::" + string(__func__) +
                    "  not the table-scope request type: " + ProtocolRequestSql_Type_Name(_request.type()));
    }
}

void WorkerSqlRequest::_extractResultSet(replica::Lock const& lock, Connection::Ptr const& conn) {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    auto resultSet = _currentResultSet(lock);

    // This will explicitly reset the default failure mode as it was
    // initialized by the constructor of the result set class.
    resultSet->set_status_ext(ProtocolStatusExt::NONE);

    // Now carry over the actual rest set (if any)
    resultSet->set_char_set_name(conn->charSetName());
    resultSet->set_has_result(conn->hasResult());

    if (conn->hasResult()) {
        for (size_t i = 0; i < conn->numFields(); ++i) {
            conn->exportField(resultSet->add_fields(), i);
        }
        size_t numRowsProcessed = 0;
        Row row;
        while (conn->next(row)) {
            if (_request.max_rows() != 0) {
                if (numRowsProcessed >= _request.max_rows()) {
                    throw out_of_range("WorkerSqlRequest::" + context(__func__) +
                                       "  max_rows=" + to_string(_request.max_rows()) + " limit exceeded");
                }
                ++numRowsProcessed;
            }
            row.exportRow(resultSet->add_rows());
        }
    }
}

void WorkerSqlRequest::_reportFailure(replica::Lock const& lock, ProtocolStatusExt statusExt,
                                      string const& error) {
    LOGS(_log, LOG_LVL_ERROR, context(__func__) << "  exception: " << error);

    // Note that the actual reason for a query to fail is recorded in its
    // result set, while the final state of the whole request may vary
    // depending on a kind of the request - if it's a simple or the "batch"
    // request.

    auto resultSet = _currentResultSet(lock);

    resultSet->set_status_ext(statusExt);
    resultSet->set_error(error);

    setStatus(lock, ProtocolStatus::FAILED, _batchMode() ? statusExt : ProtocolStatusExt::MULTIPLE);
}

ProtocolResponseSqlResultSet* WorkerSqlRequest::_currentResultSet(replica::Lock const& lock) {
    auto const numResultSets = _response.result_sets_size();
    if (numResultSets < 1) {
        throw logic_error("WorkerSqlRequest::" + context(__func__) +
                          " the operation is not allowed in this state");
    }
    return _response.mutable_result_sets(numResultSets - 1);
    ;
}

bool WorkerSqlRequest::_batchMode() const { return _request.has_batch_mode() and _request.batch_mode(); }

}  // namespace lsst::qserv::replica
