#include <atomic>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/Messenger.h"
#include "replica/MessengerConnector.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

namespace proto   = lsst::qserv::proto;
namespace replica = lsst::qserv::replica;
namespace util    = lsst::qserv::util;

namespace {

// Command line parameters

std::string workerName;
int         numIterations;
int         cancelIter;
std::string configUrl;

/// Run the test
bool test() {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        replica::ServiceProvider::pointer const provider   = replica::ServiceProvider::create(configUrl);
        replica::Controller::pointer      const controller = replica::Controller::create(provider);

        controller->run();

        /////////////////////////////////////////////////////////////////////
        // Instantiate the messenger configured in the same way as Controller 

        replica::Messenger::pointer const messenger
            = replica::Messenger::create(provider,
                                         controller->io_service());

        //////////////////////////////////////////////////
        // Prepare, serialize and launch multiple requests

        std::atomic<int> numFinished(0);

        for (int i=0; i < numIterations; ++i) {

            std::string const& id{"unique-request-id-"+std::to_string(i)};
        
            std::shared_ptr<replica::ProtocolBuffer> const requestBufferPtr =
                std::make_shared<replica::ProtocolBuffer>(provider->config()->requestBufferSizeBytes());
 
            requestBufferPtr->resize();
    
            proto::ReplicationRequestHeader hdr;
            hdr.set_id(id);
            hdr.set_type(proto::ReplicationRequestHeader::SERVICE);
            hdr.set_service_type(proto::ReplicationServiceRequestType::SERVICE_STATUS);
        
            requestBufferPtr->serialize(hdr);
    
            messenger->send<proto::ReplicationServiceResponse>(
                workerName,
                id,
                requestBufferPtr,
                [&numFinished] (std::string const& id,
                                bool success,
                                proto::ReplicationServiceResponse const& response) {
                    numFinished++;
                    std::cout
                        << std::setw(32) << id
                        << "  ** finished **  "
                        << (success ? "SUCCEEDED" : "FAILED") << std::endl;
                }
            );
        }
        if (cancelIter >= 0) {
            std::string const& id("unique-request-id-"+std::to_string(cancelIter));
            messenger->cancel(workerName, id);
        }

        //////////////////////////////////
        // Wait before all requests finish
 
        util::BlockPost blockPost(1000, 2000);
        while (numFinished < numIterations) {
            std::cout << "HEARTBEAT  " << blockPost.wait() << " millisec" << std::endl;
        }

        ///////////////////////////////////////////////////
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
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
            "  <worker> [--iterations=<number>] [--cancel=<idx>] [--config=<url>]\n"
            "\n"
            "Parameters:\n"
            "  <worker>  - the name of a worker node"
            "Flags and options:\n"
            "  --iterations  - the number of iterations\n"
            "                  [ DEFAULT: 1]\n"
            "  --cancel      - if provided and if positive then issue a request to cancel\n"
            "                  an earlier made request iteration (starting from 0 and before the number\n"
            "                  of iterations)\n"
            "                  [ DEFAULT: -1]\n"
            "  --config      - a configuration URL (a configuration file or a set of the database\n"
            "                  connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::workerName    = parser.parameter<std::string>(1);
        ::numIterations = parser.option<int>("iterations", 1);
        ::cancelIter    = parser.option<int>("cancel", -1);
        ::configUrl     = parser.option<std::string>("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    } 
 
    ::test();
    return 0;
}