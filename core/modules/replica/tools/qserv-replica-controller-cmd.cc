#include <cstdlib>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <string>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/CmdParser.h"
#include "replica/BlockPost.h"
#include "replica/DeleteRequest.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/Controller.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"

namespace rc = lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_controller_cmd");

// Command line parameters
//
std::string configUrl;
std::string operation;
std::string worker;
std::string sourceWorker;
std::string db;
std::string id;
uint32_t    chunk;

// Options and flags
//
int  priority        = 0;
bool keepTracking    = true;
bool allowDuplicate  = false;
bool computeCheckSum = false;

/// Report result of the operation
template <class T>
void printRequest (typename T::pointer request) {
    LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **");
    LOGS(_log, LOG_LVL_INFO, request->context() << "responseData:\n" << request->responseData());
    LOGS(_log, LOG_LVL_INFO, request->context() << "performance:\n"  << request->performance());
}

template <>
void printRequest<rc::ServiceManagementRequestBase> (rc::ServiceManagementRequestBase::pointer request) {
    LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **");
    LOGS(_log, LOG_LVL_INFO, request->context() << "servicState:\n\n" << request->getServiceState ());
    LOGS(_log, LOG_LVL_INFO, request->context() << "performance:\n"   << request->performance ());
}

template <class T>
void printRequestExtra (typename T::pointer request) {
    LOGS(_log, LOG_LVL_INFO, request->context() << "targetPerformance:\n" << request->targetPerformance());
}

/// Run the test
bool test () {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests

        rc::ServiceProvider provider (configUrl);

        rc::Controller::pointer controller = rc::Controller::create (provider);

        controller->run();

        /////////////////////////////////////////
        // Launch a request of the requested type

        rc::Request::pointer request;

        if ("REPLICA_CREATE" == operation) {
            request = controller->replicate (
                worker, sourceWorker, db, chunk,
                [] (rc::ReplicationRequest::pointer request) {
                    printRequest<rc::ReplicationRequest>(request);
                },
                priority,
                keepTracking,
                allowDuplicate);

        } else if ("REPLICA_CREATE,CANCEL" == operation) {
            request = controller->replicate (
                worker, sourceWorker, db, chunk,
                [] (rc::ReplicationRequest::pointer request) {
                    printRequest<rc::ReplicationRequest>(request);
                },
                priority,
                keepTracking);

            rc::BlockPost blockPost (0, 500);
            blockPost.wait();

            request->cancel();

        } else if ("REPLICA_DELETE" == operation) {
            request = controller->deleteReplica (
                worker, db, chunk,
                [] (rc::DeleteRequest::pointer request) {
                    printRequest<rc::DeleteRequest>(request);
                },
                priority,
                keepTracking,
                allowDuplicate);

        } else if ("REPLICA_FIND" == operation) {
            request = controller->findReplica (
                worker, db, chunk,
                [] (rc::FindRequest::pointer request) {
                    printRequest<rc::FindRequest>(request);
                },
                priority,
                computeCheckSum,
                keepTracking);

        } else if ("REPLICA_FIND_ALL" == operation) {
            request = controller->findAllReplicas (
                worker, db,
                [] (rc::FindAllRequest::pointer request) {
                    printRequest<rc::FindAllRequest>(request);
                },
                priority,
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_CREATE"  == operation) {
            request = controller->statusOfReplication (
                worker, id,
                [] (rc::StatusReplicationRequest::pointer request) {
                    printRequest     <rc::StatusReplicationRequest>(request);
                    printRequestExtra<rc::StatusReplicationRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_DELETE"  == operation) {
            request = controller->statusOfDelete (
                worker, id,
                [] (rc::StatusDeleteRequest::pointer request) {
                    printRequest     <rc::StatusDeleteRequest>(request);
                    printRequestExtra<rc::StatusDeleteRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_FIND"  == operation) {
            request = controller->statusOfFind (
                worker, id,
                [] (rc::StatusFindRequest::pointer request) {
                    printRequest     <rc::StatusFindRequest>(request);
                    printRequestExtra<rc::StatusFindRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_FIND_ALL"  == operation) {
            request = controller->statusOfFindAll (
                worker, id,
                [] (rc::StatusFindAllRequest::pointer request) {
                    printRequest     <rc::StatusFindAllRequest>(request);
                    printRequestExtra<rc::StatusFindAllRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_CREATE"  == operation) {
            request = controller->stopReplication (
                worker, id,
                [] (rc::StopReplicationRequest::pointer request) {
                    printRequest     <rc::StopReplicationRequest>(request);
                    printRequestExtra<rc::StopReplicationRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_DELETE"  == operation) {
            request = controller->stopReplicaDelete (
                worker, id,
                [] (rc::StopDeleteRequest::pointer request) {
                    printRequest     <rc::StopDeleteRequest>(request);
                    printRequestExtra<rc::StopDeleteRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_FIND"  == operation) {
            request = controller->stopReplicaFind (
                worker, id,
                [] (rc::StopFindRequest::pointer request) {
                    printRequest     <rc::StopFindRequest>(request);
                    printRequestExtra<rc::StopFindRequest>(request);
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_FIND_ALL"  == operation) {
            request = controller->stopReplicaFindAll (
                worker, id,
                [] (rc::StopFindAllRequest::pointer request) {
                    printRequest     <rc::StopFindAllRequest>(request);
                    printRequestExtra<rc::StopFindAllRequest>(request);
                },
                keepTracking);

        } else if ("SERVICE_SUSPEND" == operation) {
            request = controller->suspendWorkerService (
                worker,
                [] (rc::ServiceSuspendRequest::pointer request) {
                    printRequest<rc::ServiceManagementRequestBase>(request);
                });

        } else if ("SERVICE_RESUME"  == operation) {
            request = controller->resumeWorkerService (
                worker,
                [] (rc::ServiceResumeRequest::pointer request) {
                    printRequest<rc::ServiceManagementRequestBase>(request);
                });

        } else if ("SERVICE_STATUS"  == operation) {
            request = controller->statusOfWorkerService (
                worker,
                [] (rc::ServiceStatusRequest::pointer request) {
                    printRequest<rc::ServiceManagementRequestBase>(request);
                });

        } else if ("SERVICE_REQUESTS"  == operation) {
            request = controller->requestsOfWorkerService (
                worker,
                [] (rc::ServiceRequestsRequest::pointer request) {
                    printRequest<rc::ServiceManagementRequestBase>(request);
                });
        } else if ("SERVICE_DRAIN"  == operation) {
            request = controller->drainWorkerService (
                worker,
                [] (rc::ServiceDrainRequest::pointer request) {
                    printRequest<rc::ServiceManagementRequestBase>(request);
                });

        } else {
            return false;
        }

        // Wait before the request is finished. Then stop the master controller

        rc::BlockPost blockPost (0, 5000);  // for random delays (milliseconds) between iterations

        while (request->state() != rc::Request::State::FINISHED) {
            std::cout << "HEARTBEAT: " << blockPost.wait() << " msec" << std::endl;;
        }
        controller->stop();

        // Block the current thread indefinitively or untill the controller is cancelled.

        LOGS(_log, LOG_LVL_DEBUG, "waiting for: controller->join()");
        controller->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return true;
}
} /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        rc::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <operation> [<parameter> [<parameter> [...]]]\n"
            "              [--check-sum] [--do-not-track]\n"
            "              [--allow-duplicate]\n"
            "              [--priority=<level>] [--config=<url>]\n"
            "\n"
            "Supported operations and mandatory parameters:\n"
            "    REPLICA_CREATE                  <worker> <source_worker> <db> <chunk>\n"
            "    REPLICA_CREATE,CANCEL           <worker> <source_worker> <db> <chunk>\n"
            "    REPLICA_DELETE                  <worker> <db> <chunk>\n"
            "    REPLICA_FIND                    <worker> <db> <chunk>\n"
            "    REPLICA_FIND_ALL                <worker> <db>\n"
            "\n"
            "    REQUEST_STATUS:REPLICA_CREATE   <worker> <id>\n"
            "    REQUEST_STATUS:REPLICA_DELETE   <worker> <id>\n"
            "    REQUEST_STATUS:REPLICA_FIND     <worker> <id>\n"
            "    REQUEST_STATUS:REPLICA_FIND_ALL <worker> <id>\n"
            "\n"
            "    REQUEST_STOP:REPLICA_CREATE     <worker> <id>\n"
            "    REQUEST_STOP:REPLICA_DELETE     <worker> <id>\n"
            "    REQUEST_STOP:REPLICA_FIND       <worker> <id>\n"
            "    REQUEST_STOP:REPLICA_FIND_ALL   <worker> <id>\n"
            "\n"
            "    SERVICE_SUSPEND                 <worker>\n"
            "    SERVICE_RESUME                  <worker>\n"
            "    SERVICE_STATUS                  <worker>\n"
            "    SERVICE_REQUESTS                <worker>\n"
            "    SERVICE_DRAIN                   <worker>\n"
            "\n"
            "Flags and options:\n"
            "  --priority=<level>  - assign the specific priority level (default: 0)\n"
            "  --check-sum         - compute check/control sum of files\n"
            "  --do-not-track      - do not keep tracking\n"
            "  --config            - a configuration URL (a configuration file or a set of the database\n"
            "                        connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::operation = parser.parameterRestrictedBy (1, {

            "REPLICA_CREATE",
            "REPLICA_CREATE,CANCEL",
            "REPLICA_DELETE",
            "REPLICA_FIND",
            "REPLICA_FIND_ALL",
            "REQUEST_STATUS:REPLICA_CREATE",
            "REQUEST_STATUS:REPLICA_DELETE",
            "REQUEST_STATUS:REPLICA_FIND",
            "REQUEST_STATUS:REPLICA_FIND_ALL",
            "REQUEST_STOP:REPLICA_CREATE",
            "REQUEST_STOP:REPLICA_DELETE",
            "REQUEST_STOP:REPLICA_FIND",
            "REQUEST_STOP:REPLICA_FIND_ALL",
            "SERVICE_SUSPEND",
            "SERVICE_RESUME",
            "SERVICE_STATUS",
            "SERVICE_REQUESTS",
            "SERVICE_DRAIN"});

        ::worker = parser.parameter<std::string>(2);

        if (parser.found_in(::operation, {

            "REPLICA_CREATE",
            "REPLICA_CREATE,CANCEL"})) {
            
            ::sourceWorker = parser.parameter<std::string>(3);
            ::db           = parser.parameter<std::string>(4);
            ::chunk        = parser.parameter<int>        (5);

        } else if (parser.found_in(::operation, {

            "REPLICA_DELETE",
            "REPLICA_FIND",
            "REPLICA_FIND_ALL"})) {

            ::db    = parser.parameter<std::string>(3);
            ::chunk = parser.parameter<int>        (4);

        } else if (parser.found_in(::operation, {

            "REQUEST_STATUS:REPLICA_CREATE",
            "REQUEST_STATUS:REPLICA_DELETE",
            "REQUEST_STATUS:REPLICA_FIND",
            "REQUEST_STATUS:REPLICA_FIND_ALL",
            "REQUEST_STOP:REPLICA_CREATE",
            "REQUEST_STOP:REPLICA_DELETE",
            "REQUEST_STOP:REPLICA_FIND",
            "REQUEST_STOP:REPLICA_FIND_ALL"})) {

            ::id = parser.parameter<std::string>(3);
        }
        ::computeCheckSum =   parser.flag                ("check-sum");
        ::keepTracking    = ! parser.flag                ("do-not-track");
        ::allowDuplicate  =   parser.flag                ("allow-duplicate");
        ::priority        =   parser.option<int>         ("priority", 1);
        ::configUrl       =   parser.option<std::string> ("config",   "file:replication.cfg");

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }
    ::test();
    return 0;
}