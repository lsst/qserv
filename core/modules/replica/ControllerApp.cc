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
#include "replica/ControllerApp.h"

// System headers
#include <fstream>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/Controller.h"
#include "replica/DeleteRequest.h"
#include "replica/EchoRequest.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/IndexRequest.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlRequest.h"
#include "replica/SqlSchemaUtils.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"
#include "util/BlockPost.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

string const description =
    "This application allows launching Controller requests, and it's meant"
    " for both testing all known types of requests and for various manual fix up"
    " operations in a replication setup.";

template <class T>
void printRequest(typename T::Ptr const& request) {
    cout << request->context() << "\n"
         << "responseData: " << request->responseData() << "\n"
         << "performance:  " << request->performance() << endl;
}


template <>
void printRequest<ServiceManagementRequestBase>(ServiceManagementRequestBase::Ptr const& request) {
    cout << request->context() << "\n"
         << "serviceState: " << request->getServiceState() << "\n"
         << "performance: " << request->performance() << endl;
}


void printRequest(Request::Ptr const& request,
                  SqlResultSet const& resultSet,
                  Performance const& performance,
                  size_t const pageSize) {
    cout << request->context() << "\n"
         << "performance: " << performance << "\n"
         << "error:     " << resultSet.error << "\n"
         << "hasResult: " << (resultSet.hasResult ? "yes" : "no") << "\n"
         << "fields:    " << resultSet.fields.size() << "\n"
         << "rows:      " << resultSet.rows.size() << "\n"
         << "\n";

    if (resultSet.hasResult) {

        string const caption = "RESULT SET";
        string const indent  = "";

        auto table = resultSet.toColumnTable(caption, indent);

        bool const topSeparator    = false;
        bool const bottomSeparator = false;
        bool const repeatedHeader  = false;

        table.print(cout, topSeparator, bottomSeparator, pageSize, repeatedHeader);
        cout << "\n";
    }
}


void printRequest(IndexRequest::Ptr const& request, string const& fileName) {
    auto&& responseData = request->responseData();
    cout << request->context() << "\n"
         << "responseData.error: " << responseData.error << "\n"
         << "performance:  " << request->performance() << endl;

    if (fileName.empty()) cout << responseData.data;
    else {
        ofstream f(fileName);
        if (not f.good()) {
            cerr << "failed to open file: " << fileName << endl;
            return;
        }
        f << responseData.data;
    }
}


template <>
void printRequest<StatusIndexRequest>(StatusIndexRequest::Ptr const& request) {
    auto&& responseData = request->responseData();
    cout << request->context() << "\n"
         << "responseData.error: " << responseData.error << "\n"
         << "performance:  " << request->performance() << endl;
}


template <>
void printRequest<StopIndexRequest>(StopIndexRequest::Ptr const& request) {
    auto&& responseData = request->responseData();
    cout << request->context() << "\n"
         << "responseData.error: " << responseData.error << "\n"
         << "performance:  " << request->performance() << endl;
}


template <class T>
void printRequestExtra(typename T::Ptr const& request) {
    cout << "targetPerformance: " << request->targetPerformance() << endl;
}

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

ControllerApp::Ptr ControllerApp::create(int argc, char* argv[]) {
    return Ptr(
        new ControllerApp(argc, argv)
    );
}


ControllerApp::ControllerApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().commands(
        "request",
        {   "REPLICATE",
            "DELETE",
            "FIND",
            "FIND_ALL",
            "ECHO",
            "SQL_QUERY",
            "SQL_CREATE_DATABASE",
            "SQL_DELETE_DATABASE",
            "SQL_ENABLE_DATABASE",
            "SQL_DISABLE_DATABASE",
            "SQL_GRANT_ACCESS",
            "SQL_CREATE_TABLE",
            "SQL_DELETE_TABLE",
            "SQL_REMOVE_TABLE_PARTITIONS",
            "SQL_DELETE_TABLE_PARTITION",
            "INDEX",
            "STATUS",
            "STOP",
            "SERVICE_SUSPEND",
            "SERVICE_RESUME",
            "SERVICE_STATUS",
            "SERVICE_REQUESTS",
            "SERVICE_DRAIN",
            "SERVICE_RECONFIG"
        },
        _request);

    parser().required(
        "worker",
        "The name of a worker.",
        _workerName);

    parser().option(
        "cancel-delay-milliseconds",
        "The number of milliseconds to wait before cancelling (if the number of not 0)"
        " the earlier made request.",
        _cancelDelayMilliseconds);

    parser().option(
        "priority",
        "The priority level of a request",
        _priority);

    parser().flag(
        "do-not-track",
        "Do not track requests by waiting before they finish.",
        _doNotTrackRequest);

    parser().flag(
        "allow-duplicates",
        "Allow requests which duplicate the previously made one. This applies"
        " to requests which change the replica disposition at a worker, and only"
        " for those requests which are still in the worker's queues.",
        _allowDuplicates);

    parser().flag(
        "do-not-save-replica",
        "The flag which (if used) prevents the application from saving replica info in a database."
        " This may significantly speed up the application in setups where the number of chunks is on"
        " a scale of one million, or exceeds it.",
        _doNotSaveReplicaInfo);

    parser().flag(
        "compute-check-sum",
        " automatically compute and store in the database check/control sums for"
        " all files of the found replica.",
        _computeCheckSum);

    /// Request-specific parameters, options, flags

    auto& replicateCmd = parser().command("REPLICATE");

    replicateCmd.description(
        "Create a new replica of a chunk in a scope of database.");

    replicateCmd.required(
        "source-worker",
        "The name of a source worker which has a replica to be cloned.",
        _sourceWorkerName);

    replicateCmd.required(
        "database",
        "The name of a database which has a chunk.",
        _databaseName);

    replicateCmd.required(
        "chunk",
        "The number of a chunk.",
        _chunkNumber);

    /// Request-specific parameters, options, flags

    auto& deleteCmd = parser().command("DELETE");

    deleteCmd.description(
        "Delete an existing replica of a chunk in a scope of database.");

    deleteCmd.required(
        "database",
        "The name of a database which has a chunk.",
        _databaseName);

    deleteCmd.required(
        "chunk",
        "The number of a chunk.",
        _chunkNumber);

    /// Request-specific parameters, options, flags

    auto& findCmd = parser().command("FIND");

    findCmd.description(
        "Find info on an existing replica of a chunk in a scope of database.");

    findCmd.required(
        "database",
        "The name of a database which has a chunk.",
        _databaseName);

    findCmd.required(
        "chunk",
        "The number of a chunk.",
        _chunkNumber);

    /// Request-specific parameters, options, flags

    auto& findAllCmd = parser().command("FIND_ALL");

    findAllCmd.description(
        "Find info on all replicas in a scope of database.");

    findAllCmd.required(
        "database",
        "The name of a database which has chunks.",
        _databaseName);

    /// Request-specific parameters, options, flags

    auto& echoCmd = parser().command("ECHO");

    echoCmd.description(
        "Probe a worker service by sending a data string to be echoed back after"
        " an optional delay introduced by the worker.");

    echoCmd.required(
        "data",
        "The data string to be sent to a worker with the request.",
        _echoData);

    echoCmd.optional(
        "delay",
        "The optional delay (milliseconds) to be made by a worker before replying"
        " to requests. If a value of the parameter is set to 0 then the request will be"
        " answered immediately upon its reception by the worker.",
        _echoDelayMilliseconds);

    /// Request-specific parameters, options, flags

    auto& sqlQueryCmd = parser().command("SQL_QUERY");

    sqlQueryCmd.description(
        "Ask a worker service to execute a query against its database, get a result"
        " set (if any) back and print it as a table");

    sqlQueryCmd.required(
        "query",
        "The query to be executed by a worker against its database.",
        _sqlQuery);

    sqlQueryCmd.required(
        "user",
        "The name of a user for establishing a connection with the worker's database.",
        _sqlUser);

    sqlQueryCmd.required(
        "password",
        "A password which is used along with the user name for establishing a connection"
        " with the worker's database.",
        _sqlPassword);

    sqlQueryCmd.option(
        "max-rows",
        "The optional cap on a number of rows to be extracted by a worker from a result"
        " set. If a value of the parameter is set to 0 then no explicit limit will be"
        " be enforced.",
        _sqlMaxRows);

    sqlQueryCmd.option(
        "tables-page-size",
        "The number of rows in the table of a query result set (0 means no pages).",
        _sqlPageSize);

    auto& sqlCreateDbCmd = parser().command("SQL_CREATE_DATABASE");
    sqlCreateDbCmd.required(
        "database",
        "The name of a database to be created.",
        _sqlDatabase
    );

    auto& sqlDeleteDbCmd = parser().command("SQL_DELETE_DATABASE");
    sqlDeleteDbCmd.required(
        "database",
        "The name of a database to be deleted.",
        _sqlDatabase
    );

    auto& sqlEnableDbCmd = parser().command("SQL_ENABLE_DATABASE");
    sqlEnableDbCmd.required(
        "database",
        "The name of a database to be enabled at Qserv workers.",
        _sqlDatabase
    );

    auto& sqlDisableDbCmd = parser().command("SQL_DISABLE_DATABASE");
    sqlDisableDbCmd.required(
        "database",
        "The name of a database to be disable at Qserv workers.",
        _sqlDatabase
    );

    auto& grantAccessCmd = parser().command("SQL_GRANT_ACCESS");
    grantAccessCmd.required(
        "database",
        "The name of a database to be accessed.",
        _sqlDatabase
    );
    grantAccessCmd.required(
        "user",
        "The name of a user to be affected by the operation.",
        _sqlUser
    );

    auto& sqlCreateTableCmd = parser().command("SQL_CREATE_TABLE");
    sqlCreateTableCmd.required(
        "database",
        "The name of an existing database where the table will be created.",
        _sqlDatabase
    );
    sqlCreateTableCmd.required(
        "table",
        "The name of a table to be created.",
        _sqlTable
    );
    sqlCreateTableCmd.required(
        "engine",
        "The name of a MySQL engine for the new table",
        _sqlEngine
    );
    sqlCreateTableCmd.required(
        "schema-file",
        "The name of a file where column definitions of the table schema will be"
        " read from. If symbol '-' is passed instead of the file name then column"
        " definitions will be read from the Standard Input File. The file is required"
        " to have the following format: <column-name> <type>",
        _sqlSchemaFile
    );
    sqlCreateTableCmd.option(
        "partition-by-column",
        "The name of a column which is used for creating the table based on"
        " the MySQL partitioning mechanism,",
        _sqlPartitionByColumn
    );

    auto& sqlDeleteTableCmd = parser().command("SQL_DELETE_TABLE");
    sqlDeleteTableCmd.required(
        "database",
        "The name of an existing database where the table is residing.",
        _sqlDatabase
    );
    sqlDeleteTableCmd.required(
        "table",
        "The name of an existing table to be deleted.",
        _sqlTable
    );

    auto& sqlRemoveTablePartitionsCmd = parser().command("SQL_REMOVE_TABLE_PARTITIONS");
    sqlRemoveTablePartitionsCmd.required(
        "database",
        "The name of an existing database where the table is residing.",
        _sqlDatabase
    );
    sqlRemoveTablePartitionsCmd.required(
        "table",
        "The name of an existing table to be affected by the operation.",
        _sqlTable
    );

    auto& sqlDeleteTablePartitionCmd = parser().command("SQL_DELETE_TABLE_PARTITION");
    sqlDeleteTablePartitionCmd.required(
        "database",
        "The name of an existing database where the table is residing.",
        _sqlDatabase
    );
    sqlDeleteTablePartitionCmd.required(
        "table",
        "The name of an existing table to be affected by the operation.",
        _sqlTable
    );
    sqlDeleteTablePartitionCmd.required(
        "transaction",
        "An identifier of a super-transaction corresponding to a partition"
        " to be dropped from the table. The transaction must exist, and it"
        " should be in the ABORTED state.",
        _transactionId
    );

    auto& indexCmd = parser().command("INDEX");
    indexCmd.required(
        "database",
        "The name of an existing database where the table is residing.",
        _sqlDatabase
    );
    indexCmd.required(
        "chunk",
        "The chunk number.",
        _chunkNumber
    );
    indexCmd.option(
        "transaction",
        "An identifier of a super-transaction corresponding to a MySQL partition of the"
        " 'director' table. If the option isn't used then the complete content of"
        " the table will be scanned, and the scan won't include the super-transaction"
        " column 'qserv_trans_id'.",
        _transactionId
    );
    indexCmd.option(
        "index-file",
        "The name of a file where the 'secondary index' data will be written into"
        " upon a successful completion of a request. If the option is not used then"
        " the data will be printed onto the Standard Output Stream",
        _indexFileName
    );


    auto& statusCmd = parser().command("STATUS");
    statusCmd.description(
        "Ask a worker to return a status of a request.");

    statusCmd.required(
        "affected-request",
        "The type of a request affected by the operation. Supported types:"
        " REPLICATE, DELETE, FIND, FIND_ALL, ECHO, SQL_QUERY, SQL_CREATE_DATABASE"
        " SQL_DELETE_DATABASE, SQL_ENABLE_DATABASE, SQL_DISABLE_DATABASE"
        " SQL_GRANT_ACCESS"
        " SQL_CREATE_TABLE, SQL_DELETE_TABLE, SQL_REMOVE_TABLE_PARTITIONS,"
        " SQL_DELETE_TABLE_PARTITION,"
        " INDEX.",
        _affectedRequest,
       {"REPLICATE", "DELETE", "FIND", "FIND_ALL", "ECHO", "SQL_QUERY",
        "SQL_CREATE_DATABASE", "SQL_DELETE_DATABASE", "SQL_ENABLE_DATABASE",
        "SQL_DISABLE_DATABASE", "SQL_GRANT_ACCESS", "SQL_CREATE_TABLE", "SQL_DELETE_TABLE",
        "SQL_REMOVE_TABLE_PARTITIONS", "SQL_DELETE_TABLE_PARTITION",
        "INDEX"});

    statusCmd.required(
        "id",
        "A valid identifier of a request to be probed.",
        _affectedRequestId);

    /// Request-specific parameters, options, flags

    auto& stopCmd = parser().command("STOP");

    stopCmd.description(
        "Ask a worker to stop an on-going request of the given type.");

    stopCmd.required(
        "affected-request",
        "The type of a request affected by the operation. Supported types:"
        " REPLICATE, DELETE, FIND, FIND_ALL, ECHO, SQL_QUERY, SQL_CREATE_DATABASE"
        " SQL_DELETE_DATABASE, SQL_ENABLE_DATABASE, SQL_DISABLE_DATABASE"
        " SQL_GRANT_ACCESS"
        " SQL_CREATE_TABLE, SQL_DELETE_TABLE, SQL_REMOVE_TABLE_PARTITIONS,"
       " SQL_DELETE_TABLE_PARTITION,"
        " INDEX.",
        _affectedRequest,
       {"REPLICATE", "DELETE", "FIND", "FIND_ALL", "ECHO", "SQL_QUERY",
        "SQL_CREATE_DATABASE", "SQL_DELETE_DATABASE", "SQL_ENABLE_DATABASE",
        "SQL_DISABLE_DATABASE", "SQL_GRANT_ACCESS", "SQL_CREATE_TABLE", "SQL_DELETE_TABLE",
        "SQL_REMOVE_TABLE_PARTITIONS", "SQL_DELETE_TABLE_PARTITION",
        "INDEX"});

    stopCmd.required(
        "id",
        "A valid identifier of a request to be stopped.",
        _affectedRequestId);

    /// Request-specific parameters, options, flags for the remaining
    /// request types

    parser().command("SERVICE_SUSPEND").description(
        "Suspend the worker service. All ongoing requests will be cancelled and put"
        " back into the input queue as if they had never been attempted."
        " The service will be still accepting new requests which will be landing"
        " in the input queue.");

    parser().command("SERVICE_RESUME").description(
        "Resume the worker service");

    parser().command("SERVICE_STATUS").description(
        "Return a general status of the worker service. This will also include"
        " request counters for the service's queues.");

    parser().command("SERVICE_REQUESTS").description(
        "Return the detailed status of the worker service. This will include"
        " both request counters for the service's queues as well as an info on each"
        " request known to the worker.");

    parser().command("SERVICE_DRAIN").description(
        "Drain all requests by stopping cancelling all ongoing requests"
        " and emptying all queues.");

    parser().command("SERVICE_RECONFIG").description(
        "Reload worker's Configuration. Requests known to a worker won't be affected"
        " by the operation.");
}


int ControllerApp::runImpl() {

    string const context = "ControllerApp::" + string(__func__) + " ";

    auto const controller = Controller::create(serviceProvider());
    Request::Ptr ptr;

    if ("REPLICATE" == _request) {
        ptr = controller->replicate(
            _workerName,
            _sourceWorkerName,
            _databaseName,
            _chunkNumber,
            [] (ReplicationRequest::Ptr const& ptr_) {
                ::printRequest<ReplicationRequest>(ptr_);
            },
            _priority,
            not _doNotTrackRequest,
            _allowDuplicates);
    } else if ("DELETE" == _request) {
        ptr = controller->deleteReplica(
            _workerName,
            _databaseName,
            _chunkNumber,
            [] (DeleteRequest::Ptr const& ptr_) {
                ::printRequest<DeleteRequest>(ptr_);
            },
            _priority,
            not _doNotTrackRequest,
            _allowDuplicates);
    } else if ("FIND" == _request) {
        ptr = controller->findReplica(
            _workerName,
            _databaseName,
            _chunkNumber,
            [] (FindRequest::Ptr const& ptr_) {
                ::printRequest<FindRequest>(ptr_);
            },
            _priority,
            _computeCheckSum,
            not _doNotTrackRequest);
    } else if ("FIND_ALL" == _request) {
        ptr = controller->findAllReplicas(
            _workerName,
            _databaseName,
            not _doNotSaveReplicaInfo,
            [] (FindAllRequest::Ptr const& ptr_) {
                ::printRequest<FindAllRequest>(ptr_);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("ECHO" == _request) {
        ptr = controller->echo(
            _workerName,
            _echoData,
            _echoDelayMilliseconds,
            [] (EchoRequest::Ptr const& ptr_) {
                ::printRequest<EchoRequest>(ptr_);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("INDEX" == _request) {
        bool const hasTransactions = _transactionId != numeric_limits<uint32_t>::max();
        ptr = controller->index(
            _workerName,
            _sqlDatabase,
            _chunkNumber,
            hasTransactions,
            _transactionId,
            [&] (IndexRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               _indexFileName);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_QUERY" == _request) {
        ptr = controller->sqlQuery(
            _workerName,
            _sqlQuery,
            _sqlUser,
            _sqlPassword,
            _sqlMaxRows,
            [&] (SqlQueryRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_CREATE_DATABASE" == _request) {
        ptr = controller->sqlCreateDb(
            _workerName,
            _sqlDatabase,
            [&] (SqlCreateDbRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_DELETE_DATABASE" == _request) {
        ptr = controller->sqlDeleteDb(
            _workerName,
            _sqlDatabase,
            [&] (SqlDeleteDbRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_ENABLE_DATABASE" == _request) {
        ptr = controller->sqlEnableDb(
            _workerName,
            _sqlDatabase,
            [&] (SqlEnableDbRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_DISABLE_DATABASE" == _request) {
        ptr = controller->sqlDisableDb(
            _workerName,
            _sqlDatabase,
            [&] (SqlDisableDbRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_GRANT_ACCESS" == _request) {
        ptr = controller->sqlGrantAccess(
            _workerName,
            _sqlDatabase,
            _sqlUser,
            [&] (SqlGrantAccessRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_CREATE_TABLE" == _request) {
        ptr = controller->sqlCreateTable(
            _workerName,
            _sqlDatabase,
            _sqlTable,
            _sqlEngine,
            _sqlPartitionByColumn,
            SqlSchemaUtils::readFromTextFile(_sqlSchemaFile),
            [&] (SqlCreateTableRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_DELETE_TABLE" == _request) {
        ptr = controller->sqlDeleteTable(
            _workerName,
            _sqlDatabase,
            _sqlTable,
            [&] (SqlDeleteTableRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_REMOVE_TABLE_PARTITIONS" == _request) {
        ptr = controller->sqlRemoveTablePartitions(
            _workerName,
            _sqlDatabase,
            _sqlTable,
            [&] (SqlRemoveTablePartitionsRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("SQL_DELETE_TABLE_PARTITION" == _request) {
        ptr = controller->sqlDeleteTablePartition(
            _workerName,
            _sqlDatabase,
            _sqlTable,
            _transactionId,
            [&] (SqlDeleteTablePartitionRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("STATUS" == _request) {
        if ("REPLICATE"  == _affectedRequest) {
            ptr = controller->statusById<StatusReplicationRequest>(
                _workerName,
                _affectedRequestId,
                [] (StatusReplicationRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusReplicationRequest>(ptr_);
                    ::printRequestExtra<StatusReplicationRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("DELETE"  == _affectedRequest) {
            ptr = controller->statusById<StatusDeleteRequest>(
                _workerName,
                _affectedRequestId,
                [] (StatusDeleteRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusDeleteRequest>(ptr_);
                    ::printRequestExtra<StatusDeleteRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND"  == _affectedRequest) {
            ptr = controller->statusById<StatusFindRequest>(
                _workerName,
                _affectedRequestId,
                [] (StatusFindRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusFindRequest>(ptr_);
                    ::printRequestExtra<StatusFindRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND_ALL"  == _affectedRequest) {
            ptr = controller->statusById<StatusFindAllRequest>(
                _workerName,
                _affectedRequestId,
                [] (StatusFindAllRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusFindAllRequest>(ptr_);
                    ::printRequestExtra<StatusFindAllRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("ECHO" == _affectedRequest) {
            ptr = controller->statusById<StatusEchoRequest>(
                _workerName,
                _affectedRequestId,
                [] (StatusEchoRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusEchoRequest>(ptr_);
                    ::printRequestExtra<StatusEchoRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("INDEX" == _affectedRequest) {
            ptr = controller->statusById<StatusIndexRequest>(
                _workerName,
                _affectedRequestId,
                [] (StatusIndexRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusIndexRequest>(ptr_);
                    ::printRequestExtra<StatusIndexRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_QUERY" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlQueryRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlQueryRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlQueryRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_CREATE_DATABASE" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlCreateDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlCreateDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlCreateDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DELETE_DATABASE" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlDeleteDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlDeleteDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlDeleteDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_ENABLE_DATABASE" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlEnableDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlEnableDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlEnableDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DISABLE_DATABASE" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlDisableDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlDisableDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlDisableDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_GRANT_ACCESS" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlGrantAccessRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlGrantAccessRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlGrantAccessRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_CREATE_TABLE" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlCreateTableRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlCreateTableRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlCreateTableRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DELETE_TABLE" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlDeleteTableRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlDeleteTableRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlDeleteTableRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_REMOVE_TABLE_PARTITIONS" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlRemoveTablePartitionsRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlRemoveTablePartitionsRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlRemoveTablePartitionsRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DELETE_TABLE_PARTITION" == _affectedRequest) {
            ptr = controller->statusById<StatusSqlDeleteTablePartitionRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlDeleteTablePartitionRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlDeleteTablePartitionRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("INDEX" == _affectedRequest) {
            ptr = controller->statusById<StatusIndexRequest>(
                _workerName,
                _affectedRequestId,
                [] (StatusIndexRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusIndexRequest>(ptr_);
                    ::printRequestExtra<StatusIndexRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else {
            throw logic_error(context + "unsupported request: " + _affectedRequest);
        }
    } else if ("STOP" == _request) {
        if ("REPLICATE" == _affectedRequest) {
            ptr = controller->stopById<StopReplicationRequest>(
                _workerName,
                _affectedRequestId,
                [] (StopReplicationRequest::Ptr const& ptr_) {
                    ::printRequest     <StopReplicationRequest>(ptr_);
                    ::printRequestExtra<StopReplicationRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("DELETE" == _affectedRequest) {
            ptr = controller->stopById<StopDeleteRequest>(
                _workerName,
                _affectedRequestId,
                [] (StopDeleteRequest::Ptr const& ptr_) {
                    ::printRequest     <StopDeleteRequest>(ptr_);
                    ::printRequestExtra<StopDeleteRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND" == _affectedRequest) {
            ptr = controller->stopById<StopFindRequest>(
                _workerName,
                _affectedRequestId,
                [] (StopFindRequest::Ptr const& ptr_) {
                    ::printRequest     <StopFindRequest>(ptr_);
                    ::printRequestExtra<StopFindRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND_ALL" == _affectedRequest) {
            ptr = controller->stopById<StopFindAllRequest>(
                _workerName,
                _affectedRequestId,
                [] (StopFindAllRequest::Ptr const& ptr_) {
                    ::printRequest     <StopFindAllRequest>(ptr_);
                    ::printRequestExtra<StopFindAllRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("ECHO" == _affectedRequest) {
            ptr = controller->stopById<StopEchoRequest>(
                _workerName,
                _affectedRequestId,
                [] (StopEchoRequest::Ptr const& ptr_) {
                    ::printRequest     <StopEchoRequest>(ptr_);
                    ::printRequestExtra<StopEchoRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("INDEX" == _affectedRequest) {
            ptr = controller->stopById<StopIndexRequest>(
                _workerName,
                _affectedRequestId,
                [] (StopIndexRequest::Ptr const& ptr_) {
                    ::printRequest     <StopIndexRequest>(ptr_);
                    ::printRequestExtra<StopIndexRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_QUERY" == _affectedRequest) {
            ptr = controller->stopById<StopSqlQueryRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlQueryRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlQueryRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_CREATE_DATABASE" == _affectedRequest) {
            ptr = controller->stopById<StopSqlCreateDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlCreateDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlCreateDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DELETE_DATABASE" == _affectedRequest) {
            ptr = controller->stopById<StopSqlDeleteDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlDeleteDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlDeleteDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_ENABLE_DATABASE" == _affectedRequest) {
            ptr = controller->stopById<StopSqlEnableDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlEnableDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlEnableDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DISABLE_DATABASE" == _affectedRequest) {
            ptr = controller->stopById<StopSqlDisableDbRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlDisableDbRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlDisableDbRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_GRANT_ACCESS" == _affectedRequest) {
            ptr = controller->stopById<StopSqlGrantAccessRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlGrantAccessRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlGrantAccessRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_CREATE_TABLE" == _affectedRequest) {
            ptr = controller->stopById<StopSqlCreateTableRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlCreateTableRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlCreateTableRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DELETE_TABLE" == _affectedRequest) {
            ptr = controller->stopById<StopSqlDeleteTableRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlDeleteTableRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlDeleteTableRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_REMOVE_TABLE_PARTITIONS" == _affectedRequest) {
            ptr = controller->stopById<StopSqlRemoveTablePartitionsRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlRemoveTablePartitionsRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlRemoveTablePartitionsRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL_DELETE_TABLE_PARTITION" == _affectedRequest) {
            ptr = controller->stopById<StopSqlDeleteTablePartitionRequest>(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlDeleteTablePartitionRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlDeleteTablePartitionRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else {
            throw logic_error(context + "unsupported request: " + _affectedRequest);
        }
    } else if ("SERVICE_SUSPEND" == _request) {
        ptr = controller->suspendWorkerService(
            _workerName,
            [] (ServiceSuspendRequest::Ptr const& ptr_) {
                ::printRequest<ServiceManagementRequestBase>(ptr_);
            });
    } else if ("SERVICE_RESUME" == _request) {
        ptr = controller->resumeWorkerService(
            _workerName,
            [] (ServiceResumeRequest::Ptr const& ptr_) {
                ::printRequest<ServiceManagementRequestBase>(ptr_);
            });
    } else if ("SERVICE_STATUS" == _request) {
        ptr = controller->statusOfWorkerService(
            _workerName,
            [] (ServiceStatusRequest::Ptr const& ptr_) {
                ::printRequest<ServiceManagementRequestBase>(ptr_);
            });
    } else if ("SERVICE_REQUESTS" == _request) {
        ptr = controller->requestsOfWorkerService(
            _workerName,
            [] (ServiceRequestsRequest::Ptr const& ptr_) {
                ::printRequest<ServiceManagementRequestBase>(ptr_);
            });
    } else if ("SERVICE_DRAIN" == _request) {
        ptr = controller->drainWorkerService(
            _workerName,
            [] (ServiceDrainRequest::Ptr const& ptr_) {
                ::printRequest<ServiceManagementRequestBase>(ptr_);
            });
    } else if ("SERVICE_RECONFIG" == _request) {
        ptr = controller->reconfigWorkerService(
            _workerName,
            [] (ServiceReconfigRequest::Ptr const& ptr_) {
                ::printRequest<ServiceManagementRequestBase>(ptr_);
            });
    } else {
        throw logic_error(context + "unsupported request: " + _affectedRequest);
    }
    
    // Cancel the last request if required, or just block the thread waiting
    // before it will finish.
    
    if (_cancelDelayMilliseconds != 0) {
        util::BlockPost blockPost(_cancelDelayMilliseconds, _cancelDelayMilliseconds + 1);
        blockPost.wait();
        ptr->cancel();
    } else {
        ptr->wait();
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
