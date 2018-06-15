/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

/// qserv-replica-controller-cmd.cc is a Controller application for
/// testing all known types of requests.

// System headers
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Controller.h"
#include "replica/DeleteRequest.h"
#include "replica/EchoRequest.h"
#include "replica/FindRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.tools.qserv-replica-controller-cmd");

// Command line parameters
//
std::string operation;
std::string worker;
std::string sourceWorker;
std::string db;
std::string id;
std::string data;
uint32_t    chunk;
uint64_t    delay;

int  priority         = 0;
bool keepTracking     = true;
bool allowDuplicate   = false;
bool saveReplicaInfo  = true;
bool computeCheckSum  = false;
std::string configUrl = "";


/// Report result of the operation
template <class T>
void printRequest(typename T::Ptr request) {
    LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **");
    LOGS(_log, LOG_LVL_INFO, request->context() << "responseData:\n" << request->responseData());
    LOGS(_log, LOG_LVL_INFO, request->context() << "performance:\n"  << request->performance());
}

template <>
void printRequest<ServiceManagementRequestBase>(ServiceManagementRequestBase::Ptr request) {
    LOGS(_log, LOG_LVL_INFO, request->context() << "** DONE **");
    LOGS(_log, LOG_LVL_INFO, request->context() << "servicState:\n\n" << request->getServiceState());
    LOGS(_log, LOG_LVL_INFO, request->context() << "performance:\n"   << request->performance());
}

template <class T>
void printRequestExtra(typename T::Ptr request) {
    LOGS(_log, LOG_LVL_INFO, request->context() << "targetPerformance:\n" << request->targetPerformance());
}

/// Run the test
bool test() {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests

        ServiceProvider::Ptr const provider   = ServiceProvider::create(configUrl);
        Controller::Ptr      const controller = Controller::create(provider);

        controller->run();

        /////////////////////////////////////////
        // Launch a request of the requested type

        std::atomic<bool> finished(false);
        Request::Ptr request;

        if ("REPLICA_CREATE" == operation) {
            request = controller->replicate(
                worker, sourceWorker, db, chunk,
                [&finished] (ReplicationRequest::Ptr request) {
                    printRequest<ReplicationRequest>(request);
                    finished = true;
                },
                priority,
                keepTracking,
                allowDuplicate);

        } else if ("REPLICA_CREATE,CANCEL" == operation) {
            request = controller->replicate(
                worker, sourceWorker, db, chunk,
                [&finished] (ReplicationRequest::Ptr request) {
                    printRequest<ReplicationRequest>(request);
                    finished = true;
                },
                priority,
                keepTracking,
                allowDuplicate);

            util::BlockPost blockPost(0, 500);
            blockPost.wait();

            request->cancel();

        } else if ("REPLICA_DELETE" == operation) {
            request = controller->deleteReplica(
                worker, db, chunk,
                [&finished] (DeleteRequest::Ptr request) {
                    printRequest<DeleteRequest>(request);
                    finished = true;
                },
                priority,
                keepTracking,
                allowDuplicate);

        } else if ("REPLICA_FIND" == operation) {
            request = controller->findReplica(
                worker, db, chunk,
                [&finished] (FindRequest::Ptr request) {
                    printRequest<FindRequest>(request);
                    finished = true;
                },
                priority,
                computeCheckSum,
                keepTracking);

        } else if ("REPLICA_FIND_ALL" == operation) {
            request = controller->findAllReplicas(
                worker, db, saveReplicaInfo,
                [&finished] (FindAllRequest::Ptr request) {
                    printRequest<FindAllRequest>(request);
                    finished = true;
                },
                priority,
                keepTracking);

        } else if ("REPLICA_ECHO" == operation) {
            request = controller->echo(
                worker, data, delay,
                [&finished] (EchoRequest::Ptr request) {
                    printRequest<EchoRequest>(request);
                    finished = true;
                },
                priority,
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_CREATE"  == operation) {
            request = controller->statusOfReplication(
                worker, id,
                [&finished] (StatusReplicationRequest::Ptr request) {
                    printRequest     <StatusReplicationRequest>(request);
                    printRequestExtra<StatusReplicationRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_DELETE"  == operation) {
            request = controller->statusOfDelete(
                worker, id,
                [&finished] (StatusDeleteRequest::Ptr request) {
                    printRequest     <StatusDeleteRequest>(request);
                    printRequestExtra<StatusDeleteRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_FIND"  == operation) {
            request = controller->statusOfFind(
                worker, id,
                [&finished] (StatusFindRequest::Ptr request) {
                    printRequest     <StatusFindRequest>(request);
                    printRequestExtra<StatusFindRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_FIND_ALL"  == operation) {
            request = controller->statusOfFindAll(
                worker, id,
                [&finished] (StatusFindAllRequest::Ptr request) {
                    printRequest     <StatusFindAllRequest>(request);
                    printRequestExtra<StatusFindAllRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STATUS:REPLICA_ECHO" == operation) {
            request = controller->statusOfEcho(
                worker, id,
                [&finished] (StatusEchoRequest::Ptr request) {
                    printRequest     <StatusEchoRequest>(request);
                    printRequestExtra<StatusEchoRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_CREATE" == operation) {
            request = controller->stopReplication(
                worker, id,
                [&finished] (StopReplicationRequest::Ptr request) {
                    printRequest     <StopReplicationRequest>(request);
                    printRequestExtra<StopReplicationRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_DELETE" == operation) {
            request = controller->stopReplicaDelete(
                worker, id,
                [&finished] (StopDeleteRequest::Ptr request) {
                    printRequest     <StopDeleteRequest>(request);
                    printRequestExtra<StopDeleteRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_FIND" == operation) {
            request = controller->stopReplicaFind(
                worker, id,
                [&finished] (StopFindRequest::Ptr request) {
                    printRequest     <StopFindRequest>(request);
                    printRequestExtra<StopFindRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_FIND_ALL" == operation) {
            request = controller->stopReplicaFindAll(
                worker, id,
                [&finished] (StopFindAllRequest::Ptr request) {
                    printRequest     <StopFindAllRequest>(request);
                    printRequestExtra<StopFindAllRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("REQUEST_STOP:REPLICA_ECHO" == operation) {
            request = controller->stopEcho(
                worker, id,
                [&finished] (StopEchoRequest::Ptr request) {
                    printRequest     <StopEchoRequest>(request);
                    printRequestExtra<StopEchoRequest>(request);
                    finished = true;
                },
                keepTracking);

        } else if ("SERVICE_SUSPEND" == operation) {
            request = controller->suspendWorkerService(
                worker,
                [&finished] (ServiceSuspendRequest::Ptr request) {
                    printRequest<ServiceManagementRequestBase>(request);
                    finished = true;
                });

        } else if ("SERVICE_RESUME" == operation) {
            request = controller->resumeWorkerService(
                worker,
                [&finished] (ServiceResumeRequest::Ptr request) {
                    printRequest<ServiceManagementRequestBase>(request);
                    finished = true;
                });

        } else if ("SERVICE_STATUS" == operation) {
            request = controller->statusOfWorkerService(
                worker,
                [&finished] (ServiceStatusRequest::Ptr request) {
                    printRequest<ServiceManagementRequestBase>(request);
                    finished = true;
                });

        } else if ("SERVICE_REQUESTS" == operation) {
            request = controller->requestsOfWorkerService(
                worker,
                [&finished] (ServiceRequestsRequest::Ptr request) {
                    printRequest<ServiceManagementRequestBase>(request);
                    finished = true;
                });
        } else if ("SERVICE_DRAIN" == operation) {
            request = controller->drainWorkerService(
                worker,
                [&finished] (ServiceDrainRequest::Ptr request) {
                    printRequest<ServiceManagementRequestBase>(request);
                    finished = true;
                });

        } else {
            return false;
        }

        // Wait before the request is finished. Then stop the master controller

        util::BlockPost blockPost(0, 5000);     // for random delays (milliseconds) between iterations

        while (not finished) {
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

int main(int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
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
            "    REPLICA_FIND_ALL                <worker> <db> [--do-not-save-replica]\n"
            "    REPLICA_ECHO                    <worker> <data> <delay>\n"
            "\n"
            "    REQUEST_STATUS:REPLICA_CREATE   <worker> <id>\n"
            "    REQUEST_STATUS:REPLICA_DELETE   <worker> <id>\n"
            "    REQUEST_STATUS:REPLICA_FIND     <worker> <id>\n"
            "    REQUEST_STATUS:REPLICA_FIND_ALL <worker> <id>\n"
            "    REQUEST_STATUS:REPLICA_ECHO     <worker> <id>\n"
            "\n"
            "    REQUEST_STOP:REPLICA_CREATE     <worker> <id>\n"
            "    REQUEST_STOP:REPLICA_DELETE     <worker> <id>\n"
            "    REQUEST_STOP:REPLICA_FIND       <worker> <id>\n"
            "    REQUEST_STOP:REPLICA_FIND_ALL   <worker> <id>\n"
            "    REQUEST_STOP:REPLICA_ECHO       <worker> <id>\n"
            "\n"
            "    SERVICE_SUSPEND                 <worker>\n"
            "    SERVICE_RESUME                  <worker>\n"
            "    SERVICE_STATUS                  <worker>\n"
            "    SERVICE_REQUESTS                <worker>\n"
            "    SERVICE_DRAIN                   <worker>\n"
            "\n"
            "Flags and options:\n"
            "  --do-not-save-replica  - do not save replica info in a database"
            "  --priority=<level>     - assign the specific priority level (default: 0)\n"
            "  --check-sum            - compute check/control sum of files\n"
            "  --do-not-track         - do not keep tracking\n"
            "  --config               - a configuration URL (a configuration file or a set of the database\n"
            "                           connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::operation = parser.parameterRestrictedBy(1, {

            "REPLICA_CREATE",
            "REPLICA_CREATE,CANCEL",
            "REPLICA_DELETE",
            "REPLICA_FIND",
            "REPLICA_FIND_ALL",
            "REPLICA_ECHO",
            "REQUEST_STATUS:REPLICA_CREATE",
            "REQUEST_STATUS:REPLICA_DELETE",
            "REQUEST_STATUS:REPLICA_FIND",
            "REQUEST_STATUS:REPLICA_FIND_ALL",
            "REQUEST_STATUS:REPLICA_ECHO",
            "REQUEST_STOP:REPLICA_CREATE",
            "REQUEST_STOP:REPLICA_DELETE",
            "REQUEST_STOP:REPLICA_FIND",
            "REQUEST_STOP:REPLICA_FIND_ALL",
            "REQUEST_STOP:REPLICA_ECHO",
            "SERVICE_SUSPEND",
            "SERVICE_RESUME",
            "SERVICE_STATUS",
            "SERVICE_REQUESTS",
            "SERVICE_DRAIN"});

        ::worker = parser.parameter<std::string>(2);

        if (parser.in(::operation, {

            "REPLICA_CREATE",
            "REPLICA_CREATE,CANCEL"})) {

            ::sourceWorker = parser.parameter<std::string>(3);
            ::db           = parser.parameter<std::string>(4);
            ::chunk        = parser.parameter<int>(5);

        } else if (parser.in(::operation, {

            "REPLICA_DELETE",
            "REPLICA_FIND"})) {

            ::db    = parser.parameter<std::string>(3);
            ::chunk = parser.parameter<int>(4);

        } else if (parser.in(::operation, {

            "REPLICA_FIND_ALL"})) {

            ::db = parser.parameter<std::string>(3);

        } else if (parser.in(::operation, {

            "REPLICA_ECHO"})) {

            ::data  = parser.parameter<std::string>(3);
            ::delay = parser.parameter<uint64_t>(4);

        } else if (parser.in(::operation, {

            "REQUEST_STATUS:REPLICA_CREATE",
            "REQUEST_STATUS:REPLICA_DELETE",
            "REQUEST_STATUS:REPLICA_FIND",
            "REQUEST_STATUS:REPLICA_FIND_ALL",
            "REQUEST_STATUS:REPLICA_ECHO",
            "REQUEST_STOP:REPLICA_CREATE",
            "REQUEST_STOP:REPLICA_DELETE",
            "REQUEST_STOP:REPLICA_FIND",
            "REQUEST_STOP:REPLICA_FIND_ALL",
            "REQUEST_STOP:REPLICA_ECHO"})) {

            ::id = parser.parameter<std::string>(3);
        }
        ::saveReplicaInfo = not parser.flag("do-not-save-replica");
        ::computeCheckSum =     parser.flag("check-sum");
        ::keepTracking    = not parser.flag("do-not-track");
        ::allowDuplicate  =     parser.flag("allow-duplicate");
        ::priority        =     parser.option<int>("priority", 1);
        ::configUrl       =     parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
        return 1;
    }
    ::test();
    return 0;
}
