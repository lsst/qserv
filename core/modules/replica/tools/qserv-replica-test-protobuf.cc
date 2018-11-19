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

/// qserv-replica-test-protobuf.cc test the performance and possible memory
/// leaks within Google Protobuf

// System headers
#include <iostream> 
#include <stdexcept>
#include <string> 

// Qserv headers
#include "proto/FrameBuffer.h"
#include "proto/worker.pb.h"
#include "util/CmdLineParser.h"

namespace proto = lsst::qserv::proto;
namespace util  = lsst::qserv::util;

namespace {

// Command line parameters and options

unsigned int steps  = 1;
unsigned int chunks = 1;
bool         clear  = false;

std::string const database = "database";   

/// The test
void test() {
    try {

        proto::FrameBuffer buf;
        for (unsigned int step = 0; step < steps; ++step) {
            proto::WorkerCommandSetChunkListM message;
            for (unsigned int chunk = 0; chunk < chunks; ++chunk) {
                proto::WorkerCommandChunk* ptr = message.add_chunks();
                ptr->set_db(database);
                ptr->set_chunk(chunk);
            }
            std::cout << "SpaceUsed: " << message.SpaceUsed() << "  chunks_size: " << message.chunks_size();
            buf.serialize(message);
            std::cout << "  buf.size: " << buf.size() << std::endl;

            // Un-comment either of these to test an effect of explicit resets
            // of the message content
            //
            // if (clear) message.clear_chunks();
            // if (clear) message.Clear();
        }

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
        std::exit(1);
    }
}
} // namespace

int main(int argc, const char *argv[]) {

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
            "  [--steps=<num>] [--chunks=<num>]\n"
            "  [--clear]\n"
            "\n"
            "Flags and options\n"
            "  --steps   - the number of steps\n"
            "              [ DEFAULT: " + std::to_string(::steps) + " ]\n"
            "\n"
            "  --chunks  - the number of chunks per each step\n"
            "              [ DEFAULT: " + std::to_string(::chunks) + " ]\n"
            "\n"
            "  --clear   - clear embeded chunks after each step\n");

        ::steps  = parser.option<unsigned int>("steps",  ::steps);
        ::chunks = parser.option<unsigned int>("chunks", ::chunks);

        ::clear  = parser.flag("clear");

    } catch (std::exception const &ex) {
        return 1;
    } 
    ::test();
    return 0;
}
