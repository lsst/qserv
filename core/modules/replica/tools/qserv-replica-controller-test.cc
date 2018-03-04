#include <iostream>
#include <list>
#include <stdexcept>
#include <string>
#include <thread>

#include "lsst/log/Log.h"
#include "proto/replication.pb.h"
#include "replica/Controller.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace rc   = lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.replica_controller_test");

// Command line parameters

std::string workerName;
std::string sourceWorkerName;
std::string database;
std::string configUrl;
    
/**
 * The helper class for generating various requests. The main purpose
 * of the class is to reduce code duplication in the tests.
 *
 * THREAD SAFETY: an implementation of this class is as thread safe
 * as any objec used by it.
 */
class RequestGenerator {

public:

    /// A collection of REPLICATION requsts
    typedef std::vector<rc::ReplicationRequest::pointer> replication_requests;

    /// A collection of STATUS requsts
    typedef std::vector<rc::StatusReplicationRequest::pointer> status_requests;

    /// A collection of STOP requsts
    typedef std::vector<rc::StopReplicationRequest::pointer> stop_requests;

    // Default construction and copy semantics are proxibited

    RequestGenerator () = delete;
    RequestGenerator (RequestGenerator const&) = delete;
    RequestGenerator & operator= (RequestGenerator const&) = delete;


    /**
     * The normal constructor.
     *
     * @param controller        - the controller API for initiating requests
     * @param worker            - the name of a worker node for new replicas
     * @param sourceWorker      - the name of a worker node for chunks to be replicated
     * @param database          - the database which is a subject of the replication
     */
    RequestGenerator (rc::Controller::pointer  controller,
                      const std::string       &worker,
                      const std::string       &sourceWorker,
                      const std::string       &database)
        :   _controller   (controller),
            _worker       (worker),
            _sourceWorker (sourceWorker),
            _database     (database) {
    }

    /**
     * Initiate the specified numder of replication requests
     * and return a collection of pointers to them. The requests will
     * address a contigous range of chunk numbers staring with the one
     * specified as a parameter of the method.
     *
     * @param num        - the total number of requests to launch
     * @param firstChunk - the number of the first chunk in a series
     * @param blockPost  - an optional pointer to the BlockPost object
     *                     if a random delay before(!!!) generating each request
     *                     is required.
     *                     
     * @return - a collection of pointers to the requests.
     */
    replication_requests replicate (size_t num,
                                    unsigned int firstChunk,
                                    util::BlockPost *blockPost=nullptr) {

        replication_requests requests;
        
        unsigned int chunk = firstChunk;

        for (size_t i = 0; i < num; ++i) {

            // Delay the request generation if needed.

            if (blockPost) blockPost->wait();

            rc::ReplicationRequest::pointer request =
                _controller->replicate (
                    _worker,
                    _sourceWorker,
                    _database,
                    chunk++,
                    [] (rc::ReplicationRequest::pointer request) {
                        LOGS(_log, LOG_LVL_INFO, request->context()
                            << "** DONE **"
                            << "  chunk: " << request->chunk()
                            << "  " << request->performance());
                    }
                );
            requests.push_back(request);
        }
        return requests;
    }

    /**
     * Initiate status inqueries for the specified replication requests.
     *
     * @param replicationRequests - a collection of pointers to the REPLICATION requests
     * 
     * @return - a collection of pointers to the STATUS requests.
     */
    status_requests status (const replication_requests& replicationRequests) {

        status_requests requests;

        for (auto request : replicationRequests)
            requests.push_back (
                _controller->statusOfReplication (
                    request->worker(),
                    request->id(),
                    [] (rc::StatusReplicationRequest::pointer request) {
                        LOGS(_log, LOG_LVL_INFO, request->context()
                            << "** DONE **"
                            << "  targetRequestId: " << request->targetRequestId()
                            << "  " << request->performance());
                    },
                    true
                )
            );

        return requests;
    }

    /**
     * Initiate stop commands for the specified replication requests.
     *
     * @param replicationRequests - a collection of pointers to the REPLICATION requests
     * 
     * @return - a collection of pointers to the STOP requests.
     */
    stop_requests stop (const replication_requests& replicationRequests) {

        stop_requests requests;

        for (auto request : replicationRequests)
            requests.push_back (
                _controller->stopReplication (
                    request->worker(),
                    request->id(),
                    [] (rc::StopReplicationRequest::pointer request) {
                        LOGS(_log, LOG_LVL_INFO, request->context()
                            << "** DONE **"
                            << "  targetRequestId: " << request->targetRequestId()
                            << "  " << request->performance());
                    },
                    true
                )
            );

        return requests;
    }

private:    

    // Parameters of the object

    rc::Controller::pointer _controller;

    std::string _worker;
    std::string _sourceWorker;
    std::string _database;        
};

/**
 * Print a status of the controller
 */
void reportControllerStatus (rc::Controller::pointer controller) {
    LOGS(_log, LOG_LVL_INFO, "controller is " << (controller->isRunning() ? "" : "NOT ") << "running");
}

/**
 * Run the test
 */
void test () {

    try {

        util::BlockPost blockPost (0, 100);     // for random delays (milliseconds) between operations

        rc::ServiceProvider provider (configUrl);

        rc::Controller::pointer controller = rc::Controller::create (provider);

        // Configure the generator of requests 

        RequestGenerator requestGenerator (
            controller,
            workerName,
            sourceWorkerName,
            database);
        
        // Start the controller in its own thread before injecting any requests

        reportControllerStatus(controller);
        controller->run();
        reportControllerStatus(controller);

        // Create the first bunch of requests which are to be launched
        // right away.

        requestGenerator.replicate(10, 0);
      
        // Inject the second bunch of requests delayed one from another
        // by a random interval of time.

        requestGenerator.replicate(10, 10, &blockPost);
        
        // Do a proper clean up of the service by stopping it. This way of stopping
        // the service will guarantee that all outstanding opeations will finish
        // and not aborted.
        //
        // NOTE: Joining to the controller's thread is not needed because
        //       this will always be done internally inside the stop method.
        //       The only reason for joining would be for having an option of
        //       integrating the controller into a larger application.

        reportControllerStatus(controller);
        //controller->stop();
        reportControllerStatus(controller);

        //controller->run();
        reportControllerStatus(controller);

        //requestGenerator.replicate(1000, 100, &blockPost);

        // Launch another thread which will test injecting requests from there.
        // 
        // NOTE: The thread may (and will) finish when the specified number of
        // requests will be launched because the requests are execured in
        // a context of the controller thread.
        std::thread another (
            [&requestGenerator, &blockPost] () {

                const size_t num = 1000;
                const unsigned int chunk = 100;

                requestGenerator.replicate(num, chunk, &blockPost);

            }
        );

        // Continue injecting requests on the periodic bases, one at a time for
        // each known worker.

        RequestGenerator::replication_requests requests =
            requestGenerator.replicate(10, 30, &blockPost);

        // Launch STATUS and STOP requests for each of the previously
        // generated REPLICATION request.

        LOGS(_log, LOG_LVL_INFO, "checking status of " <<  requests.size() << " requests");
        requestGenerator.status(requests);

        LOGS(_log, LOG_LVL_INFO, "stopping " <<  requests.size() << " requests");
        requestGenerator.stop  (requests);

        // Wait before the request launching thread finishes

        reportControllerStatus(controller);
        LOGS(_log, LOG_LVL_INFO, "waiting for: another.join()");
        another.join();
        
        // This should block the current thread indefinitively or
        // untill the controller is cancelled.

        while (true) {
            blockPost.wait();
            LOGS(_log, LOG_LVL_INFO, "HEARTBEAT  active requests: " << controller->numActiveRequests());
        }
        LOGS(_log, LOG_LVL_INFO, "waiting for: controller->join()");
        controller->join();
        LOGS(_log, LOG_LVL_INFO, "past: controller->join()");

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
}
}

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <worker> <source_worker> <database> [--config=<url>]\n"
            "\n"
            "Parameters:\n"
            "  <worker>           - the name of a destination worker\n"
            "  <source_worker>    - the name of a source worker\n"
            "  <database>         - the name of a database\n"
            "\n"
            "Flags and options:\n"
            "  --config           - a configuration URL (a configuration file or a set of the database\n"
            "                       connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::workerName       = parser.parameter<std::string> (1);
        ::sourceWorkerName = parser.parameter<std::string> (2);
        ::database         = parser.parameter<std::string> (3);
        ::configUrl        = parser.option   <std::string> ("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    } 
    ::test ();

    return 0;
}
