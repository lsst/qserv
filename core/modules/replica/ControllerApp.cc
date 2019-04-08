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

    sqlCmd.description(
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

    sqlCmd.option(
        "tables-page-size",
        "The number of rows in the table of a query result set (0 means no pages).",
        _sqlPageSize);

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
    } else if ("SQL" == _request) {
        ptr = controller->sql(
            _workerName,
            _sqlQuery,
            _sqlUser,
            _sqlPassword,
            _sqlMaxRows,
            [&] (SqlRequest::Ptr const& ptr_) {
                ::printRequest(ptr_,
                               ptr_->responseData(),
                               ptr_->performance(),
                               _sqlPageSize);
            },
            _priority,
            not _doNotTrackRequest);
    } else if ("STATUS" == _request) {
        if ("REPLICATE"  == _affectedRequest) {
            ptr = controller->statusOfReplication(
                _workerName,
                _affectedRequestId,
                [] (StatusReplicationRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusReplicationRequest>(ptr_);
                    ::printRequestExtra<StatusReplicationRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("DELETE"  == _affectedRequest) {
            ptr = controller->statusOfDelete(
                _workerName,
                _affectedRequestId,
                [] (StatusDeleteRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusDeleteRequest>(ptr_);
                    ::printRequestExtra<StatusDeleteRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND"  == _affectedRequest) {
            ptr = controller->statusOfFind(
                _workerName,
                _affectedRequestId,
                [] (StatusFindRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusFindRequest>(ptr_);
                    ::printRequestExtra<StatusFindRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND_ALL"  == _affectedRequest) {
            ptr = controller->statusOfFindAll(
                _workerName,
                _affectedRequestId,
                [] (StatusFindAllRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusFindAllRequest>(ptr_);
                    ::printRequestExtra<StatusFindAllRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("ECHO" == _affectedRequest) {
            ptr = controller->statusOfEcho(
                _workerName,
                _affectedRequestId,
                [] (StatusEchoRequest::Ptr const& ptr_) {
                    ::printRequest     <StatusEchoRequest>(ptr_);
                    ::printRequestExtra<StatusEchoRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL" == _affectedRequest) {
            ptr = controller->statusOfSql(
                _workerName,
                _affectedRequestId,
                [&] (StatusSqlRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StatusSqlRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else {
            throw logic_error(context + "unsupported request: " + _affectedRequest);
        }
    } else if ("STOP" == _request) {
        if ("REPLICATE" == _affectedRequest) {
            ptr = controller->stopReplication(
                _workerName,
                _affectedRequestId,
                [] (StopReplicationRequest::Ptr const& ptr_) {
                    ::printRequest     <StopReplicationRequest>(ptr_);
                    ::printRequestExtra<StopReplicationRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("DELETE" == _affectedRequest) {
            ptr = controller->stopReplicaDelete(
                _workerName,
                _affectedRequestId,
                [] (StopDeleteRequest::Ptr const& ptr_) {
                    ::printRequest     <StopDeleteRequest>(ptr_);
                    ::printRequestExtra<StopDeleteRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND" == _affectedRequest) {
            ptr = controller->stopReplicaFind(
                _workerName,
                _affectedRequestId,
                [] (StopFindRequest::Ptr const& ptr_) {
                    ::printRequest     <StopFindRequest>(ptr_);
                    ::printRequestExtra<StopFindRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("FIND_ALL" == _affectedRequest) {
            ptr = controller->stopReplicaFindAll(
                _workerName,
                _affectedRequestId,
                [] (StopFindAllRequest::Ptr const& ptr_) {
                    ::printRequest     <StopFindAllRequest>(ptr_);
                    ::printRequestExtra<StopFindAllRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("ECHO" == _affectedRequest) {
            ptr = controller->stopEcho(
                _workerName,
                _affectedRequestId,
                [] (StopEchoRequest::Ptr const& ptr_) {
                    ::printRequest     <StopEchoRequest>(ptr_);
                    ::printRequestExtra<StopEchoRequest>(ptr_);
                },
                not _doNotTrackRequest);
        } else if ("SQL" == _affectedRequest) {
            ptr = controller->stopSql(
                _workerName,
                _affectedRequestId,
                [&] (StopSqlRequest::Ptr const& ptr_) {
                    ::printRequest(ptr_,
                                   ptr_->responseData(),
                                   ptr_->performance(),
                                   _sqlPageSize);
                    ::printRequestExtra<StopSqlRequest>(ptr_);
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
