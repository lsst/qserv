
#include "replica/CmdParser.h"
#include "replica_core/FileUtils.h"

#include <iostream> 
#include <stdexcept>
#include <string> 
#include <vector> 

namespace r  = lsst::qserv::replica;
namespace rc = lsst::qserv::replica_core;

namespace {

/// The name of an input file to be processed
std::vector<std::string> fileNames;

/// USe the incremental engine if set
bool incremental;

/// The test
void test () {
    try {
        if (incremental) {
            rc::MultiFileCsComputeEngine eng(fileNames);
            while (!eng.execute()) ;
            for (auto const& name: fileNames)
                std::cout << name << ": " << eng.cs(name) << std::endl;
        } else {
            for (auto const& name: fileNames) {
                std::cout << name << ": " << rc::FileUtils::compute_cs (name) << std::endl;
            }
        }
    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
        std::exit(1);
    }
}
} // namespace

int main(int argc, const char *argv[]) {

    // Parse command line parameters
    try {
        r::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <file> [<file> [<file> ... ] [--incremental]\n"
            "\n"
            "Parameters:\n"
            "  <file>  - the name of a file to read. Multiple files can be specified\n"
            "\n"
            "Flags and options\n"
            "  --incremental  -- use the incremental ile reader instead\n");

        parser.parameters<std::string>(::fileNames);
        ::incremental = parser.flag("incremental");

    } catch (std::exception &ex) {
        return 1;
    } 

    ::test();
    return 0;
}