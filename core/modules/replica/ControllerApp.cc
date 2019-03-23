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
#include <atomic>
#include <iostream>
#include <stdexcept>

// Qserv headers
#include "replica/Controller.h"
#include "replica/DeleteRequest.h"
#include "replica/EchoRequest.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/SqlRequest.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"
#include "util/BlockPost.h"
#include "util/TablePrinter.h"

// Third party headers
#include <mysql/mysql.h>

using namespace std;

namespace {

string const description =
    "This application allows launching Controller requests, and it's meant"
    " for both testing all known types of requests and for various manual fix up"
    " operations in a replication setup.";

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

void printAsTable(SqlResultSet const& result,
                  bool sqlShowType) {

    cout << "  error:     " << result.error << "\n"
         << "  hasResult: " << (result.hasResult ? "yes" : "no") << "\n"
         << "  #fields:   " << to_string(result.fields.size()) << "\n"
         << "  #rows:     " << to_string(result.rows.size()) << "\n";

    if (result.hasResult) {

        size_t const numRows = result.rows.size();
        size_t const numColumns = result.fields.size();

        vector<shared_ptr<vector<string>>> tableDataByColumns;
        tableDataByColumns.reserve(numColumns);

        for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {
            auto tableColumnPtr = make_shared<vector<string>>();
            tableColumnPtr->reserve(numRows);
            tableDataByColumns.push_back(tableColumnPtr);
        }
        for (auto const& row :  result.rows) {                
            for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {

                auto const& field  = result.fields[columnIdx];
                auto const& cell   = row   .cells [columnIdx];

                auto const& tableColumnPtr = tableDataByColumns[columnIdx];
                if (row.nulls[columnIdx]) {
                    tableColumnPtr->push_back("NULL");
                } else {
                    if (sqlShowType) {
                        string typeName ="UNKNOWN MYSQL TYPE:" + to_string(field.type);
                        switch (field.type) {
                            case MYSQL_TYPE_DECIMAL:     typeName = "MYSQL_TYPE_DECIMAL";     break;
                            case MYSQL_TYPE_TINY:        typeName = "MYSQL_TYPE_TINY";        break;
                            case MYSQL_TYPE_SHORT:       typeName = "MYSQL_TYPE_SHORT";       break;
                            case MYSQL_TYPE_LONG:        typeName = "MYSQL_TYPE_LONG";        break;
                            case MYSQL_TYPE_FLOAT:       typeName = "MYSQL_TYPE_FLOAT";       break;
                            case MYSQL_TYPE_DOUBLE:      typeName = "MYSQL_TYPE_DOUBLE";      break;
                            case MYSQL_TYPE_NULL:        typeName = "MYSQL_TYPE_NULL";        break;
                            case MYSQL_TYPE_TIMESTAMP:   typeName = "MYSQL_TYPE_TIMESTAMP";   break;
                            case MYSQL_TYPE_LONGLONG:    typeName = "MYSQL_TYPE_LONGLONG";    break;
                            case MYSQL_TYPE_INT24:       typeName = "MYSQL_TYPE_INT24";       break;
                            case MYSQL_TYPE_DATE:        typeName = "MYSQL_TYPE_DATE";        break;
                            case MYSQL_TYPE_TIME:        typeName = "MYSQL_TYPE_TIME";        break;
                            case MYSQL_TYPE_DATETIME:    typeName = "MYSQL_TYPE_DATETIME";    break;
                            case MYSQL_TYPE_YEAR:        typeName = "MYSQL_TYPE_YEAR";        break;
                            case MYSQL_TYPE_NEWDATE:     typeName = "MYSQL_TYPE_NEWDATE";     break;
                            case MYSQL_TYPE_VARCHAR:     typeName = "MYSQL_TYPE_VARCHAR";     break;
                            case MYSQL_TYPE_BIT:         typeName = "MYSQL_TYPE_BIT";         break;
                            case MYSQL_TYPE_TIMESTAMP2:  typeName = "MYSQL_TYPE_TIMESTAMP2";  break;
                            case MYSQL_TYPE_DATETIME2:   typeName = "MYSQL_TYPE_DATETIME2";   break;
                            case MYSQL_TYPE_TIME2:       typeName = "MYSQL_TYPE_TIME2";       break;
                            case MYSQL_TYPE_JSON:        typeName = "MYSQL_TYPE_JSON";        break;
                            case MYSQL_TYPE_NEWDECIMAL:  typeName = "MYSQL_TYPE_NEWDECIMAL";  break;
                            case MYSQL_TYPE_ENUM:        typeName = "MYSQL_TYPE_ENUM";        break;
                            case MYSQL_TYPE_SET:         typeName = "MYSQL_TYPE_SET";         break;
                            case MYSQL_TYPE_TINY_BLOB:   typeName = "MYSQL_TYPE_TINY_BLOB";   break;
                            case MYSQL_TYPE_MEDIUM_BLOB: typeName = "MYSQL_TYPE_MEDIUM_BLOB"; break;
                            case MYSQL_TYPE_LONG_BLOB:   typeName = "MYSQL_TYPE_LONG_BLOB";   break;
                            case MYSQL_TYPE_BLOB:        typeName = "MYSQL_TYPE_BLOB";        break;
                            case MYSQL_TYPE_VAR_STRING:  typeName = "MYSQL_TYPE_VAR_STRING";  break;
                            case MYSQL_TYPE_STRING:      typeName = "MYSQL_TYPE_STRING";      break;
                            case MYSQL_TYPE_GEOMETRY:    typeName = "MYSQL_TYPE_GEOMETRY";    break;
                        }
                        tableColumnPtr->push_back(typeName + ": " + cell);
                    } else {
                        tableColumnPtr->push_back(cell);
                    }
                }
            }
        }
        bool   const topSeparator    = true;
        bool   const bottomSeparator = true;
        size_t const pageSize        = 20;
        bool   const repeatedHeader  = false;

        util::ColumnTablePrinter table("QUERY RESULT SET:", "  ");

        for (size_t columnIdx = 0; columnIdx < numColumns; ++columnIdx) {
            table.addColumn(result.fields[columnIdx].name,
                            *(tableDataByColumns[columnIdx]),
                            util::ColumnTablePrinter::Alignment::LEFT);
        }
        table.print(cout, topSeparator, bottomSeparator, pageSize, repeatedHeader);
    }
}



/// Report result of the operation
template <class T>
void printRequest(typename T::Ptr const& request) {
    cout << request->context() << "** DONE **" << "\n"
         << "  responseData: " << request->responseData() << "\n"
         << "  performance: "  << request->performance() << endl;
}


template <>
void printRequest<ServiceManagementRequestBase>(ServiceManagementRequestBase::Ptr const& request) {
    cout << request->context() << "** DONE **" << "\n"
         << "  servicState: " << request->getServiceState() << "\n"
         << "  performance: " << request->performance() << endl;
}

void printRequest(SqlRequest::Ptr const& request,
                  bool sqlShowType) {
    cout << request->context() << "** DONE **" << "\n"
         << "  performance: " << request->performance() << endl;
    printAsTable(request->responseData(),
                 sqlShowType);
}

void printRequest(StatusSqlRequest::Ptr const& request,
                  bool sqlShowType) {
    cout << request->context() << "** DONE **" << "\n"
         << "  performance: " << request->performance() << endl;
    printAsTable(request->responseData(),
                 sqlShowType);
}

void printRequest(StopSqlRequest::Ptr const& request,
                  bool sqlShowType) {
    cout << request->context() << "** DONE **" << "\n"
         << "  performance: " << request->performance() << endl;
    printAsTable(request->responseData(),
                 sqlShowType);
}


template <class T>
void printRequestExtra(typename T::Ptr const& request) {
    cout << "  targetPerformance: " << request->targetPerformance() << endl;
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
            "SQL",
            "STATUS",
            "STOP",
            "SERVICE_SUSPEND",
            "SERVICE_RESUME",
            "SERVICE_STATUS",
            "SERVICE_REQUESTS",
            "SERVICE_DRAIN"
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

    auto& sqlCmd = parser().command("SQL");

    echoCmd.description(
        "Ask a worker service to execute a query against its database, get a result"
        " set (if any) back and print it as a table");

    sqlCmd.required(
        "query",
        "The query to be executed by a worker against its database.",
        _sqlQuery);

    sqlCmd.required(
        "user",
        "The name of a user for establishing a connection with the worker's database.",
        _sqlUser);

    sqlCmd.required(
        "password",
        "A password which is used along with the user name for establishing a connection"
        " with the worker's database.",
        _sqlPassword);

    sqlCmd.option(
        "max-rows",
        "The optional cap on a number of rows to be extracted by a worker from a result"
        " set. If a value of the parameter is set to 0 then no explicit limit will be"
        " be enforced.",
        _sqlMaxRows);

    sqlCmd.flag(
        "show-data-type",
        "If this flag is provided then the application will also print types for"
        " each data cell of a result set",
        _sqlShowType);

    /// Request-specific parameters, options, flags

    auto& statusCmd = parser().command("STATUS");

    statusCmd.description(
        "Ask a worker to return a status of a request.");

    statusCmd.required(
        "affected-request",
        "The type of a request affected by the operation. Supported types:"
        " REPLICATE, DELETE, FIND, FIND_ALL, ECHO, SQL.",
        _affectedRequest,
       {"REPLICATE", "DELETE", "FIND", "FIND_ALL", "ECHO", "SQL"});

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
        " REPLICATE, DELETE, FIND, FIND_ALL, ECHO, SQL.",
        _affectedRequest,
       {"REPLICATE", "DELETE", "FIND", "FIND_ALL", "ECHO", "SQL"});

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
}


int ControllerApp::runImpl() {

    auto const controller = Controller::create(serviceProvider());

    atomic<bool> finished(false);
    Request::Ptr request;

    if ("REPLICATE" == _request) {
        request = controller->replicate(
            _workerName,
            _sourceWorkerName,
            _databaseName,
            _chunkNumber,
            [&finished] (ReplicationRequest::Ptr const& request) {
                ::printRequest<ReplicationRequest>(request);
                finished = true;
            },
            _priority,
            not _doNotTrackRequest,
            _allowDuplicates);


    } else if ("DELETE" == _request) {
        request = controller->deleteReplica(
            _workerName,
            _databaseName,
            _chunkNumber,
            [&finished] (DeleteRequest::Ptr const& request) {
                ::printRequest<DeleteRequest>(request);
                finished = true;
            },
            _priority,
            not _doNotTrackRequest,
            _allowDuplicates);

    } else if ("FIND" == _request) {
        request = controller->findReplica(
            _workerName,
            _databaseName,
            _chunkNumber,
            [&finished] (FindRequest::Ptr const& request) {
                ::printRequest<FindRequest>(request);
                finished = true;
            },
            _priority,
            _computeCheckSum,
            not _doNotTrackRequest);

    } else if ("FIND_ALL" == _request) {
        request = controller->findAllReplicas(
            _workerName,
            _databaseName,
            not _doNotSaveReplicaInfo,
            [&finished] (FindAllRequest::Ptr const& request) {
                ::printRequest<FindAllRequest>(request);
                finished = true;
            },
            _priority,
            not _doNotTrackRequest);

    } else if ("ECHO" == _request) {
        request = controller->echo(
            _workerName,
            _echoData,
            _echoDelayMilliseconds,
            [&finished] (EchoRequest::Ptr const& request) {
                ::printRequest<EchoRequest>(request);
                finished = true;
            },
            _priority,
            not _doNotTrackRequest);

    } else if ("SQL" == _request) {
        request = controller->sql(
            _workerName,
            _sqlQuery,
            _sqlUser,
            _sqlPassword,
            _sqlMaxRows,
            [&] (SqlRequest::Ptr const& request) {
                ::printRequest(request, _sqlShowType);
                finished = true;
            },
            _priority,
            not _doNotTrackRequest);

    } else if ("STATUS" == _request) {

        if ("REPLICATE"  == _affectedRequest) {
            request = controller->statusOfReplication(
                _workerName,
                _affectedRequestId,
                [&finished] (StatusReplicationRequest::Ptr const& request) {
                    ::printRequest     <StatusReplicationRequest>(request);
                    ::printRequestExtra<StatusReplicationRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("DELETE"  == _affectedRequest) {
            request = controller->statusOfDelete(
                _workerName,
                _affectedRequestId,
                [&finished] (StatusDeleteRequest::Ptr const& request) {
                    ::printRequest     <StatusDeleteRequest>(request);
                    ::printRequestExtra<StatusDeleteRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("FIND"  == _affectedRequest) {
            request = controller->statusOfFind(
                _workerName,
                _affectedRequestId,
                [&finished] (StatusFindRequest::Ptr const& request) {
                    ::printRequest     <StatusFindRequest>(request);
                    ::printRequestExtra<StatusFindRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("FIND_ALL"  == _affectedRequest) {
            request = controller->statusOfFindAll(
                _workerName,
                _affectedRequestId,
                [&finished] (StatusFindAllRequest::Ptr const& request) {
                    ::printRequest     <StatusFindAllRequest>(request);
                    ::printRequestExtra<StatusFindAllRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("ECHO" == _affectedRequest) {
            request = controller->statusOfEcho(
                _workerName,
                _affectedRequestId,
                [&finished] (StatusEchoRequest::Ptr const& request) {
                    ::printRequest     <StatusEchoRequest>(request);
                    ::printRequestExtra<StatusEchoRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("SQL" == _affectedRequest) {
            request = controller->statusOfSql(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlRequest::Ptr const& request) {
                    ::printRequest(request, _sqlShowType);
                    ::printRequestExtra<StatusSqlRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else {
            throw logic_error(
                    "ControllerApp::" + string(__func__) + "  unsupported request: " +
                    _affectedRequest);
            return 1;
        }

    } else if ("STOP" == _request) {

        if ("REPLICATE" == _affectedRequest) {
            request = controller->stopReplication(
                _workerName,
                _affectedRequestId,
                [&finished] (StopReplicationRequest::Ptr const& request) {
                    ::printRequest     <StopReplicationRequest>(request);
                    ::printRequestExtra<StopReplicationRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("DELETE" == _affectedRequest) {
            request = controller->stopReplicaDelete(
                _workerName,
                _affectedRequestId,
                [&finished] (StopDeleteRequest::Ptr const& request) {
                    ::printRequest     <StopDeleteRequest>(request);
                    ::printRequestExtra<StopDeleteRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("FIND" == _affectedRequest) {
            request = controller->stopReplicaFind(
                _workerName,
                _affectedRequestId,
                [&finished] (StopFindRequest::Ptr const& request) {
                    ::printRequest     <StopFindRequest>(request);
                    ::printRequestExtra<StopFindRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("FIND_ALL" == _affectedRequest) {
            request = controller->stopReplicaFindAll(
                _workerName,
                _affectedRequestId,
                [&finished] (StopFindAllRequest::Ptr const& request) {
                    ::printRequest     <StopFindAllRequest>(request);
                    ::printRequestExtra<StopFindAllRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("ECHO" == _affectedRequest) {
            request = controller->stopEcho(
                _workerName,
                _affectedRequestId,
                [&finished] (StopEchoRequest::Ptr const& request) {
                    ::printRequest     <StopEchoRequest>(request);
                    ::printRequestExtra<StopEchoRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else if ("SQL" == _affectedRequest) {
            request = controller->stopSql(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlRequest::Ptr const& request) {
                    ::printRequest(request, _sqlShowType);
                    ::printRequestExtra<StopSqlRequest>(request);
                    finished = true;
                },
                not _doNotTrackRequest);

        } else {
            throw logic_error(
                    "ControllerApp::" + string(__func__) + "  unsupported request: " +
                    _affectedRequest);
            return 1;
        }

    } else if ("SERVICE_SUSPEND" == _request) {
        request = controller->suspendWorkerService(
            _workerName,
            [&finished] (ServiceSuspendRequest::Ptr const& request) {
                ::printRequest<ServiceManagementRequestBase>(request);
                finished = true;
            });

    } else if ("SERVICE_RESUME" == _request) {
        request = controller->resumeWorkerService(
            _workerName,
            [&finished] (ServiceResumeRequest::Ptr const& request) {
                ::printRequest<ServiceManagementRequestBase>(request);
                finished = true;
            });

    } else if ("SERVICE_STATUS" == _request) {
        request = controller->statusOfWorkerService(
            _workerName,
            [&finished] (ServiceStatusRequest::Ptr const& request) {
                ::printRequest<ServiceManagementRequestBase>(request);
                finished = true;
            });

    } else if ("SERVICE_REQUESTS" == _request) {
        request = controller->requestsOfWorkerService(
            _workerName,
            [&finished] (ServiceRequestsRequest::Ptr const& request) {
                ::printRequest<ServiceManagementRequestBase>(request);
                finished = true;
            });
    } else if ("SERVICE_DRAIN" == _request) {
        request = controller->drainWorkerService(
            _workerName,
            [&finished] (ServiceDrainRequest::Ptr const& request) {
                ::printRequest<ServiceManagementRequestBase>(request);
                finished = true;
            });

    } else {
            throw logic_error(
                    "ControllerApp::" + string(__func__) + "  unsupported request: " +
                    _affectedRequest);
        return 1;
    }
    
    // Cancel the last request if required
    
    if (_cancelDelayMilliseconds != 0) {
        util::BlockPost blockPost(_cancelDelayMilliseconds, _cancelDelayMilliseconds + 1);
        blockPost.wait();
        request->cancel();
    }

    // Print periodic heartbeats while waiting before
    // the request will finish.

    util::BlockPost blockPost(100, 200);   // for random delays (in milliseconds) between iterations

    size_t const printIvalMs   = 5000;
    size_t       currentIvalMs = 0;
    while (not finished) {
        currentIvalMs += blockPost.wait();
        if (currentIvalMs >= printIvalMs) {
            cout << "HEARTBEAT: " << currentIvalMs << " ms" << endl;
            currentIvalMs = 0;
        }
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
